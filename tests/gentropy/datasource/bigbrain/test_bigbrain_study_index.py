"""Test BigBrain study index."""

from __future__ import annotations

from pyspark.sql import Row

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
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
