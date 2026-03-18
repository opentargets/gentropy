"""Unit tests for deCODE study index."""

from __future__ import annotations

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.session import Session
from gentropy.dataset.molecular_complex import MolecularComplex
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.datasource.decode.aptamer_metadata import AptamerMetadata
from gentropy.datasource.decode.manifest import deCODEManifest
from gentropy.datasource.decode.study_index import (
    deCODEStudyIdParts,
    deCODEStudyIndex,
)


class TestdeCODEStudyIdParts:
    """Tests for the deCODEStudyIdParts regex-based ID parsing."""

    @pytest.mark.parametrize(
        "study_id,expected_project,expected_datasource,expected_aptamer,expected_gene,expected_protein",
        [
            (
                "deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
                "deCODE-proteomics-smp",
                "Proteomics_SMP_PC0",
                "10000-2",
                "GENE1",
                "PROTEIN1",
            ),
            (
                "deCODE-proteomics-raw_Proteomics_PC0_10001_1_GENEXY_PROTEINY_99999999",
                "deCODE-proteomics-raw",
                "Proteomics_PC0",
                "10001-1",
                "GENEXY",
                "PROTEINY",
            ),
        ],
    )
    def test_extract_study_id_parts(
        self,
        session: Session,
        study_id: str,
        expected_project: str,
        expected_datasource: str,
        expected_aptamer: str,
        expected_gene: str,
        expected_protein: str,
    ) -> None:
        """extract_study_id_parts should correctly parse each component of a studyId."""
        df = session.spark.createDataFrame([(study_id,)], "studyId STRING")
        id_parts = deCODEStudyIdParts.extract_study_id_parts(f.col("studyId"))
        row = df.select(*id_parts).collect()[0]
        assert row.projectId == expected_project
        assert row.datasourceType == expected_datasource
        assert row.aptamerId == expected_aptamer
        assert row.geneSymbolFromStudyId == expected_gene
        assert row.proteinNameFromStudyId == expected_protein

    def test_extract_study_id_parts_invalid_pattern_returns_empty(
        self, session: Session
    ) -> None:
        """An unrecognised studyId should return empty strings for all parts."""
        df = session.spark.createDataFrame(
            [("not-a-valid-study-id",)], "studyId STRING"
        )
        id_parts = deCODEStudyIdParts.extract_study_id_parts(f.col("studyId"))
        row = df.select(*id_parts).collect()[0]
        assert row.projectId == ""

    def test_mark_missing_gene_id_na_becomes_null(self, session: Session) -> None:
        """A gene symbol of 'NA' should be converted to null."""
        df = session.spark.createDataFrame([("NA",), ("REAL_GENE",)], "gene STRING")
        result = df.select(
            deCODEStudyIdParts._mark_missing_gene_id(f.col("gene")).alias("out")
        ).collect()
        assert result[0].out is None
        assert result[1].out == "REAL_GENE"

    @pytest.mark.parametrize("protein", ["Deprecated", "No_Protein"])
    def test_mark_missing_protein_deprecated_becomes_null(
        self, session: Session, protein: str
    ) -> None:
        """'Deprecated' and 'No_Protein' protein names should be nulled out."""
        df = session.spark.createDataFrame([(protein,)], "protein STRING")
        result = df.select(
            deCODEStudyIdParts._mark_missing_protein(f.col("protein")).alias("out")
        ).collect()
        assert result[0].out is None

    def test_trait_property_concatenates_parts(self, session: Session) -> None:
        """The trait property should concat datasourceType, aptamerId, geneSymbol, proteinName."""
        study_id = (
            "deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001"
        )
        df = session.spark.createDataFrame([(study_id,)], "studyId STRING")
        id_parts = deCODEStudyIdParts.extract_study_id_parts(f.col("studyId"))
        row = df.select(id_parts.trait).collect()[0]
        assert row.traitFromSource == "Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1"


class TestdeCODEStudyIndexSampleSizes:
    """Tests for sample-size helper methods."""

    @pytest.mark.parametrize(
        "project_id,expected_n",
        [
            ("deCODE-proteomics-raw", 36_136),
            ("deCODE-proteomics-smp", 35_892),
        ],
    )
    def test_get_n_samples_by_project(
        self, session: Session, project_id: str, expected_n: int
    ) -> None:
        """get_n_samples should return the correct integer sample count per project."""
        from gentropy.datasource.decode import deCODEPublicationMetadata

        pub = deCODEPublicationMetadata()
        df = session.spark.createDataFrame([(project_id,)], "projectId STRING")
        row = df.select(
            deCODEStudyIndex.get_n_samples(f.col("projectId"), pub)
        ).collect()[0]
        assert row.nSamples == expected_n

    @pytest.mark.parametrize(
        "project_id,expected_text",
        [
            ("deCODE-proteomics-raw", "36,136 Icelandic individuals"),
            ("deCODE-proteomics-smp", "35,892 Icelandic individuals"),
        ],
    )
    def test_get_initial_sample_text(
        self, session: Session, project_id: str, expected_text: str
    ) -> None:
        """get_initial_sample should return a readable string with correct count."""
        from gentropy.datasource.decode import deCODEPublicationMetadata

        pub = deCODEPublicationMetadata()
        df = session.spark.createDataFrame([(project_id,)], "projectId STRING")
        row = df.select(
            deCODEStudyIndex.get_initial_sample(f.col("projectId"), pub)
        ).collect()[0]
        assert row.initialSampleSize == expected_text


class TestdeCODEStudyIndexUpdateStudyId:
    """Tests for deCODEStudyIndex.update_study_id."""

    def test_update_study_id_single_target(self, session: Session) -> None:
        """update_study_id should reconstruct studyId using gene/protein from targets."""
        study_id = (
            "deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001"
        )
        targets_type = t.ArrayType(
            t.StructType(
                [
                    t.StructField("geneSymbol", t.StringType()),
                    t.StructField("proteinId", t.StringType()),
                ]
            )
        )
        schema = t.StructType(
            [
                t.StructField("studyId", t.StringType()),
                t.StructField("targets", targets_type),
            ]
        )
        df = session.spark.createDataFrame(
            [(study_id, [Row(geneSymbol="GENE1", proteinId="PROTEIN1")])],
            schema=schema,
        )
        row = df.select(
            deCODEStudyIndex.update_study_id(f.col("studyId"), f.col("targets"))
        ).collect()[0]
        assert (
            row.updatedStudyId
            == "deCODE-proteomics-smp_Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1"
        )

    def test_update_study_id_multi_target(self, session: Session) -> None:
        """Multi-target studyId should comma-join geneSymbols and proteinNames."""
        study_id = (
            "deCODE-proteomics-smp_Proteomics_SMP_PC0_20000_1_GENEA_PROTEINA_00000001"
        )
        targets_type = t.ArrayType(
            t.StructType(
                [
                    t.StructField("geneSymbol", t.StringType()),
                    t.StructField("proteinId", t.StringType()),
                ]
            )
        )
        schema = t.StructType(
            [
                t.StructField("studyId", t.StringType()),
                t.StructField("targets", targets_type),
            ]
        )
        df = session.spark.createDataFrame(
            [
                (
                    study_id,
                    [
                        Row(geneSymbol="GENEA", proteinId="PROTEINA"),
                        Row(geneSymbol="GENEB", proteinId="PROTEINB"),
                    ],
                )
            ],
            schema=schema,
        )
        row = df.select(
            deCODEStudyIndex.update_study_id(f.col("studyId"), f.col("targets"))
        ).collect()[0]
        assert "GENEA,GENEB" in row.updatedStudyId
        assert "PROTEINA,PROTEINB" in row.updatedStudyId


class TestdeCODEStudyIndexFromManifest:
    """Tests for deCODEStudyIndex.from_manifest."""

    def test_from_manifest_returns_pqtl_study_index(
        self,
        sample_manifest: deCODEManifest,
        sample_aptamer_metadata: AptamerMetadata,
        empty_molecular_complex: MolecularComplex,
    ) -> None:
        """from_manifest should return a ProteinQuantitativeTraitLocusStudyIndex."""
        result = deCODEStudyIndex.from_manifest(
            manifest=sample_manifest,
            aptamer_metadata=sample_aptamer_metadata,
            molecular_complex=empty_molecular_complex,
        )
        assert isinstance(result, ProteinQuantitativeTraitLocusStudyIndex)

    def test_from_manifest_inner_joins_on_aptamer_id(
        self,
        session: Session,
        sample_manifest: deCODEManifest,
        empty_molecular_complex: MolecularComplex,
    ) -> None:
        """Rows without a matching aptamerId should be excluded (inner join)."""
        # Only provide aptamer for the first (SMP) study → only 1 output row
        single_aptamer_df = session.spark.createDataFrame(
            [
                Row(
                    aptamerId="10000-2",
                    targetName="GENE1",
                    targetFullName="Gene 1",
                    isProteinComplex=False,
                    targetMetadata=[Row(geneSymbol="GENE1", proteinId="P12345")],
                )
            ],
            schema=AptamerMetadata.get_schema(),
        )
        single_aptamer = AptamerMetadata(_df=single_aptamer_df)
        result = deCODEStudyIndex.from_manifest(
            manifest=sample_manifest,
            aptamer_metadata=single_aptamer,
            molecular_complex=empty_molecular_complex,
        )
        assert result.df.count() == 1

    def test_from_manifest_drops_missing_gene_or_protein(
        self,
        session: Session,
        empty_molecular_complex: MolecularComplex,
    ) -> None:
        """Rows where the gene symbol is 'NA' (→ null) are dropped before the join."""
        from datetime import datetime

        from gentropy.datasource.decode import deCODEDataSource

        bad_rows = [
            Row(
                projectId=deCODEDataSource.DECODE_PROTEOMICS_SMP.value,
                studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_NA_PROTEIN1_00000001",
                hasSumstats=True,
                summarystatsLocation="s3a://bucket/file.txt.gz",
                size="1 MiB",
                accessionTimestamp=datetime(2022, 1, 1),
            ),
        ]
        manifest = deCODEManifest(
            _df=session.spark.createDataFrame(
                bad_rows, schema=deCODEManifest.get_schema()
            )
        )
        aptamer_df = session.spark.createDataFrame(
            [
                Row(
                    aptamerId="10000-2",
                    targetName="GENE1",
                    targetFullName="Full",
                    isProteinComplex=False,
                    targetMetadata=[Row(geneSymbol="GENE1", proteinId="P12345")],
                )
            ],
            schema=AptamerMetadata.get_schema(),
        )
        aptamer = AptamerMetadata(_df=aptamer_df)
        result = deCODEStudyIndex.from_manifest(
            manifest=manifest,
            aptamer_metadata=aptamer,
            molecular_complex=empty_molecular_complex,
        )
        assert result.df.count() == 0

    def test_from_manifest_publication_metadata(
        self,
        session: Session,
        empty_molecular_complex: MolecularComplex,
    ) -> None:
        """Output rows should carry hardcoded publication metadata."""
        from datetime import datetime

        from gentropy.datasource.decode import deCODEDataSource

        rows = [
            Row(
                projectId=deCODEDataSource.DECODE_PROTEOMICS_SMP.value,
                studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
                hasSumstats=True,
                summarystatsLocation="s3a://bucket/f.txt.gz",
                size="1 MiB",
                accessionTimestamp=datetime(2022, 1, 1),
            )
        ]
        manifest = deCODEManifest(
            _df=session.spark.createDataFrame(rows, schema=deCODEManifest.get_schema())
        )
        aptamer_df = session.spark.createDataFrame(
            [
                Row(
                    aptamerId="10000-2",
                    targetName="GENE1",
                    targetFullName="Full",
                    isProteinComplex=False,
                    targetMetadata=[Row(geneSymbol="GENE1", proteinId="P12345")],
                )
            ],
            schema=AptamerMetadata.get_schema(),
        )
        aptamer = AptamerMetadata(_df=aptamer_df)
        result = deCODEStudyIndex.from_manifest(
            manifest=manifest,
            aptamer_metadata=aptamer,
            molecular_complex=empty_molecular_complex,
        )
        row = result.df.select(
            "pubmedId",
            "publicationJournal",
            "biosampleFromSourceId",
            "studyType",
            "nSamples",
            "initialSampleSize",
        ).collect()[0]
        assert row.pubmedId == "37794188"
        assert row.publicationJournal == "Nature"
        assert row.biosampleFromSourceId == "UBERON_0001969"
        assert row.studyType == "pqtl"
        assert row.nSamples == 35_892
        assert row.initialSampleSize == "35,892 Icelandic individuals"

    def test_from_manifest_molecular_complex_annotated(
        self,
        session: Session,
    ) -> None:
        """A matched molecular complex should populate molecularComplexId."""
        from datetime import datetime

        from gentropy.datasource.decode import deCODEDataSource

        rows = [
            Row(
                projectId=deCODEDataSource.DECODE_PROTEOMICS_SMP.value,
                studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
                hasSumstats=True,
                summarystatsLocation="s3a://bucket/f.txt.gz",
                size="1 MiB",
                accessionTimestamp=datetime(2022, 1, 1),
            )
        ]
        manifest = deCODEManifest(
            _df=session.spark.createDataFrame(rows, schema=deCODEManifest.get_schema())
        )
        aptamer_df = session.spark.createDataFrame(
            [
                Row(
                    aptamerId="10000-2",
                    targetName="GENE1",
                    targetFullName="Full",
                    isProteinComplex=False,
                    targetMetadata=[Row(geneSymbol="GENE1", proteinId="P12345")],
                )
            ],
            schema=AptamerMetadata.get_schema(),
        )
        aptamer = AptamerMetadata(_df=aptamer_df)
        mc_df = session.spark.createDataFrame(
            [
                Row(
                    id="COMPLEX_001",
                    description="desc",
                    properties=None,
                    assembly=None,
                    components=[Row(id="P12345", stoichiometry="1", source="intact")],
                    evidenceCodes=None,
                    crossReferences=None,
                    source=Row(id="intact", source="intact"),
                )
            ],
            schema=MolecularComplex.get_schema(),
        )
        mc = MolecularComplex(_df=mc_df)
        result = deCODEStudyIndex.from_manifest(
            manifest=manifest,
            aptamer_metadata=aptamer,
            molecular_complex=mc,
        )
        row = result.df.select("molecularComplexId").collect()[0]
        assert row.molecularComplexId == "COMPLEX_001"
