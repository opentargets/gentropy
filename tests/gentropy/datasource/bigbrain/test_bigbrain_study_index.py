"""Test BigBrain study index."""

from __future__ import annotations

from pyspark.sql import Row

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.bigbrain.study_index import BigBrainStudyIndex


class TestBigBrainStudyIndex:
    """Test methods of BigBrainStudyIndex."""

    def test_gene_map_from_feature(self, session: Session) -> None:
        """EQTL feature is already a versioned Ensembl gene ID; geneId should be version-stripped."""
        features = session.spark.createDataFrame([Row(feature="ENSG00000177757.2")])
        gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
        row = gene_map.collect()[0]
        assert row.geneId == "ENSG00000177757"
        assert row.traitFromSource == "ENSG00000177757.2"

    def test_gene_map_from_top_assoc(self, session: Session) -> None:
        """SQTL feature-to-gene mapping is derived from top_assoc, deduplicated by feature."""
        top_assoc = session.spark.createDataFrame(
            [
                Row(
                    feature="chr1_136708_136903",
                    gene_id="ENSG00000107554.10",
                    gene_name="DNMBP",
                ),
                Row(
                    # Duplicate feature (e.g. EUR/ALL rows) should collapse to one.
                    feature="chr1_136708_136903",
                    gene_id="ENSG00000107554.10",
                    gene_name="DNMBP",
                ),
            ]
        )
        gene_map = BigBrainStudyIndex.gene_map_from_top_assoc(top_assoc)
        rows = gene_map.collect()
        assert len(rows) == 1
        assert rows[0].geneId == "ENSG00000107554"
        assert rows[0].traitFromSource == "DNMBP"

    def test_from_source_eqtl(self, session: Session) -> None:
        """EQTL study index: one row per feature, geneId resolved directly."""
        features = session.spark.createDataFrame(
            [Row(feature="ENSG00000177757.2"), Row(feature="ENSG00000187608.9")]
        )
        gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
        study_index = BigBrainStudyIndex.from_source(features, gene_map, "eqtl")

        assert isinstance(study_index, StudyIndex)
        rows = {row.studyId: row for row in study_index.df.collect()}
        assert len(rows) == 2
        row = rows["BigBrain_eqtl_EUR_ENSG00000177757.2"]
        assert row.geneId == "ENSG00000177757"
        assert row.studyType == "eqtl"
        assert row.nSamples == 10_725
        assert row.cohorts == ["BigBrain"]
        assert row.biosampleFromSourceId == "UBERON_0000955"

    def test_from_source_maps_ancestry_to_ld_population(self, session: Session) -> None:
        """discoverySamples.ancestry must be a key of gwas_population_2_LD_panel_map.json.

        The short code "EUR" is not in that map and silently produced a null
        ldPopulation, which made SusieFineMapperStep reject every BigBrain study
        with "Major ancestry is not nfe, csa or afr". The studyId keeps the short
        code; only the ancestry label is long-form.
        """
        features = session.spark.createDataFrame([Row(feature="ENSG00000177757.2")])
        gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
        study_index = BigBrainStudyIndex.from_source(features, gene_map, "eqtl")

        row = study_index.df.collect()[0]
        assert row.studyId == "BigBrain_eqtl_EUR_ENSG00000177757.2"
        assert row.discoverySamples[0].ancestry == "European"
        assert row.ldPopulationStructure[0].ldPopulation == "nfe"
        assert row.ldPopulationStructure[0].relativeSampleSize == 1.0

    def test_from_source_sets_has_sumstats(self, session: Session) -> None:
        """HasSumstats must be explicitly True, not left to default to null.

        BigBrain publishes summary statistics for every study, so it is always true.
        """
        features = session.spark.createDataFrame([Row(feature="ENSG00000177757.2")])
        gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
        study_index = BigBrainStudyIndex.from_source(features, gene_map, "eqtl")

        assert study_index.df.collect()[0].hasSumstats is True

    def test_from_source_sqtl_missing_gene_mapping(self, session: Session) -> None:
        """SQTL features absent from top_assoc resolve to a null geneId, not an error."""
        features = session.spark.createDataFrame(
            [Row(feature="chr1_136708_136903"), Row(feature="chr2_999_1000")]
        )
        top_assoc = session.spark.createDataFrame(
            [
                Row(
                    feature="chr1_136708_136903",
                    gene_id="ENSG00000107554.10",
                    gene_name="DNMBP",
                )
            ]
        )
        gene_map = BigBrainStudyIndex.gene_map_from_top_assoc(top_assoc)
        study_index = BigBrainStudyIndex.from_source(features, gene_map, "sqtl")

        rows = {row.studyId: row for row in study_index.df.collect()}
        assert len(rows) == 2
        assert rows["BigBrain_sqtl_EUR_chr1_136708_136903"].geneId == "ENSG00000107554"
        assert rows["BigBrain_sqtl_EUR_chr2_999_1000"].geneId is None
        assert rows["BigBrain_sqtl_EUR_chr2_999_1000"].studyType == "sqtl"

    def test_gene_map_from_top_assoc_resolves_multi_gene_junctions(
        self, session: Session
    ) -> None:
        """A junction in several genes keeps the one gene, or protein-coding gene, containing it."""
        schema = TargetIndex.get_schema()
        genes = [
            # (id, biotype, start, end)
            ("ENSG_A", "protein_coding", 100, 300),  # contains chr1_150_250
            ("ENSG_B", "protein_coding", 200, 400),  # does not contain it
            ("ENSG_C", "protein_coding", 400, 700),  # both contain chr1_500_600 ...
            ("ENSG_D", "lncRNA", 450, 650),  # ... but only C is protein coding
            ("ENSG_E", "protein_coding", 650, 900),  # both contain chr1_700_800
            ("ENSG_F", "protein_coding", 690, 850),  # and both are protein coding
        ]
        target_index = TargetIndex(
            _df=session.spark.createDataFrame(
                [
                    {fld.name: None for fld in schema.fields}
                    | {
                        "id": gid,
                        "biotype": biotype,
                        "genomicLocation": {
                            "chromosome": "1",
                            "start": start,
                            "end": end,
                            "strand": 1,
                        },
                    }
                    for gid, biotype, start, end in genes
                ],
                schema=schema,
            ),
            _schema=schema,
        )
        top_assoc = session.spark.createDataFrame(
            [
                Row(
                    feature="chr1_150_250", gene_id="ENSG_A.1;ENSG_B.2", gene_name="A;B"
                ),
                Row(feature="chr1_500_600", gene_id="ENSG_C;ENSG_D", gene_name="C;D"),
                Row(feature="chr1_700_800", gene_id="ENSG_E;ENSG_F", gene_name="E;F"),
                Row(feature="chr1_10_20", gene_id="NA", gene_name="NA"),
            ]
        )
        gene_map = {
            r.feature: (r.geneId, r.traitFromSource)
            for r in BigBrainStudyIndex.gene_map_from_top_assoc(
                top_assoc, target_index
            ).collect()
        }
        assert gene_map == {
            "chr1_150_250": ("ENSG_A", "A;B"),
            "chr1_500_600": ("ENSG_C", "C;D"),
            "chr1_700_800": (None, "E;F"),
            "chr1_10_20": (None, None),
        }
