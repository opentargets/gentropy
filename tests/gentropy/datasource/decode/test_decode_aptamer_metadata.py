"""Unit tests for AptamerMetadata."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from pyspark.sql import types as t

from gentropy.common.session import Session
from gentropy.datasource.decode.aptamer_metadata import AptamerMetadata


class TestAptamerMetadataSchema:
    """Tests for AptamerMetadata schema."""

    def test_get_schema_returns_struct_type(self) -> None:
        """get_schema() should return a StructType."""
        schema = AptamerMetadata.get_schema()
        assert isinstance(schema, t.StructType)

    def test_get_schema_field_names(self) -> None:
        """get_schema() must contain all expected field names."""
        expected_fields = {
            "aptamerId",
            "targetName",
            "targetFullName",
            "isProteinComplex",
            "targetMetadata",
        }
        assert set(AptamerMetadata.get_schema().fieldNames()) == expected_fields

    def test_aptamer_id_is_non_nullable(self) -> None:
        """AptamerId and targetName must be non-nullable."""
        schema = AptamerMetadata.get_schema()
        fields = {f.name: f for f in schema.fields}
        assert fields["aptamerId"].nullable is False
        assert fields["targetName"].nullable is False


class TestAptamerMetadataTransform:
    """Tests for AptamerMetadata._transform_source."""

    def test_strips_seqid_prefix(self, session: Session) -> None:
        """SeqId. prefix must be stripped; hyphens in the numeric suffix are preserved."""
        raw = session.spark.createDataFrame(
            [("SeqId.10000-2", "GENE1", "Full name", "GENE1", "P12345")],
            "seqid STRING, target_name STRING, target_full_name STRING, gene_name STRING, uniprot STRING",
        )
        result = AptamerMetadata._transform_source(raw)
        rows = result.df.collect()
        assert len(rows) == 1
        assert rows[0].aptamerId == "10000-2"

    def test_single_target_not_complex(self, session: Session) -> None:
        """A single-gene aptamer should have isProteinComplex=False and one targetMetadata entry."""
        raw = session.spark.createDataFrame(
            [("SeqId.1111-1", "GENE1", "Gene 1", "GENE1", "P12345")],
            "seqid STRING, target_name STRING, target_full_name STRING, gene_name STRING, uniprot STRING",
        )
        result = AptamerMetadata._transform_source(raw)
        rows = result.df.collect()
        assert rows[0].isProteinComplex is False
        assert len(rows[0].targetMetadata) == 1
        assert rows[0].targetMetadata[0].geneSymbol == "GENE1"
        assert rows[0].targetMetadata[0].proteinId == "P12345"

    def test_multi_target_is_complex(self, session: Session) -> None:
        """A comma-separated gene list should yield isProteinComplex=True and multiple targets."""
        raw = session.spark.createDataFrame(
            [
                (
                    "SeqId.2222_3",
                    "COMPLEX",
                    "Complex protein",
                    "GENEA,GENEB",
                    "P111,P222",
                )
            ],
            "seqid STRING, target_name STRING, target_full_name STRING, gene_name STRING, uniprot STRING",
        )
        result = AptamerMetadata._transform_source(raw)
        rows = result.df.collect()
        assert rows[0].isProteinComplex is True
        assert len(rows[0].targetMetadata) == 2

    @pytest.mark.parametrize(
        "gene_name,uniprot",
        [
            ("GENE1", "P12345"),
            ("GENE1,GENE2", "P12345,P67890"),
        ],
    )
    def test_target_metadata_gene_and_protein_id(
        self, session: Session, gene_name: str, uniprot: str
    ) -> None:
        """TargetMetadata entries should carry geneSymbol and proteinId."""
        raw = session.spark.createDataFrame(
            [("SeqId.9999-1", "TNAME", "Full", gene_name, uniprot)],
            "seqid STRING, target_name STRING, target_full_name STRING, gene_name STRING, uniprot STRING",
        )
        result = AptamerMetadata._transform_source(raw)
        rows = result.df.collect()
        gene_list = gene_name.split(",")
        protein_list = uniprot.split(",")
        for i, (gene, protein) in enumerate(zip(gene_list, protein_list)):
            assert rows[0].targetMetadata[i].geneSymbol == gene
            assert rows[0].targetMetadata[i].proteinId == protein

    def test_distinct_rows_deduplicated(self, session: Session) -> None:
        """Duplicate rows in the source should be deduplicated."""
        raw = session.spark.createDataFrame(
            [
                ("SeqId.1-1", "G", "FN", "G", "P1"),
                ("SeqId.1-1", "G", "FN", "G", "P1"),
            ],
            "seqid STRING, target_name STRING, target_full_name STRING, gene_name STRING, uniprot STRING",
        )
        result = AptamerMetadata._transform_source(raw)
        assert result.df.count() == 1


class TestAptamerMetadataFromSource:
    """Tests for AptamerMetadata.from_source."""

    def test_from_source_returns_aptamer_metadata(
        self, session: Session, tmp_path: Path
    ) -> None:
        """from_source should return an AptamerMetadata instance."""
        tsv_path = tmp_path / "aptamers.tsv"
        rows = [
            {
                "seqid": "SeqId.10000_2",
                "target_name": "GENE1",
                "target_full_name": "Gene 1",
                "gene_name": "GENE1",
                "uniprot": "P12345",
            },
            {
                "seqid": "SeqId.10001_1",
                "target_name": "GENE2",
                "target_full_name": "Gene 2",
                "gene_name": "GENE2",
                "uniprot": "Q99999",
            },
        ]
        with tsv_path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys(), delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

        result = AptamerMetadata.from_source(session, str(tsv_path))
        assert isinstance(result, AptamerMetadata), "Should return AptamerMetadata"
        assert result.df.count() == 2

    def test_from_source_uses_sample_file(self, session: Session) -> None:
        """from_source should parse the repository sample file without errors."""
        sample_path = (
            "tests/gentropy/data_samples/decode/decode_2023_aptamer_mapping.tsv"
        )
        result = AptamerMetadata.from_source(session, sample_path)
        assert isinstance(result, AptamerMetadata)
        assert result.df.count() > 0
