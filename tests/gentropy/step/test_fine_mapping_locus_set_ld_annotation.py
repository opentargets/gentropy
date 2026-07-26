"""Tests for fine-mapping locus-set LD annotation helpers."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.dataset.fine_mapping_study_metadata import (
    FineMappingStudyMetadata,
    FineMappingStudyMetadataRecord,
)
from gentropy.fine_mapping_locus_set_ld_annotation import (
    FineMappingLocusSetLDAnnotationStep,
)


def test_metadata_record_reads_valid_jsonl(tmp_path: Path) -> None:
    """A metadata JSONL file is parsed into validated study records."""
    metadata_path = tmp_path / "metadata.jsonl"
    metadata_path.write_text(
        '{"studyId":"STUDY_A","ancestry":"nfe","sampleSize":100}\n'
    )

    records = FineMappingStudyMetadataRecord.from_jsonl(metadata_path)

    assert records == [
        FineMappingStudyMetadataRecord(
            studyId="STUDY_A", ancestry="nfe", sampleSize=100
        )
    ]


def test_metadata_record_rejects_duplicate_study_ids(tmp_path: Path) -> None:
    """Metadata JSONL cannot define more than one record for a study."""
    metadata_path = tmp_path / "metadata.jsonl"
    metadata_path.write_text(
        '{"studyId":"STUDY_A","ancestry":"nfe","sampleSize":100}\n'
        '{"studyId":"STUDY_A","ancestry":"afr","sampleSize":200}\n'
    )

    with pytest.raises(ValueError, match="Duplicate studyId: STUDY_A"):
        FineMappingStudyMetadataRecord.from_jsonl(metadata_path)


def test_metadata_dataset_reads_validated_jsonl(
    session: Session, tmp_path: Path
) -> None:
    """The Gentropy metadata dataset can be built directly from JSONL."""
    metadata_path = tmp_path / "metadata.jsonl"
    metadata_path.write_text(
        '{"studyId":"STUDY_A","ancestry":"nfe","sampleSize":100}\n'
    )

    metadata = FineMappingStudyMetadata.from_jsonl(session, metadata_path)

    assert metadata.df.select("studyId", "ancestry", "sampleSize").collect()[
        0
    ].asDict() == {"studyId": "STUDY_A", "ancestry": "nfe", "sampleSize": 100}


def test_ld_pair_counts_include_requested_ancestries_with_zero_rows(
    spark: SparkSession,
) -> None:
    """LD statistics report zero for requested ancestries with no pairs."""
    pairs = spark.createDataFrame(
        [("nfe", "1_1_A_C", "1_1_A_C", 1.0)],
        ["ancestry", "variantIdI", "variantIdJ", "r"],
    )

    counts = FineMappingLocusSetLDAnnotationStep._ld_pair_counts(pairs, ["nfe", "afr"])

    assert counts == [
        {"ancestry": "afr", "n_ld_pairs": 0},
        {"ancestry": "nfe", "n_ld_pairs": 1},
    ]


def test_ld_pair_stats_are_written_as_jsonl(tmp_path: Path) -> None:
    """LD pair counts are persisted one JSON object per line."""
    stats_path = tmp_path / "stats.jsonl"

    FineMappingLocusSetLDAnnotationStep._write_ld_pair_stats(
        [
            {"ancestry": "afr", "n_ld_pairs": 0},
            {"ancestry": "nfe", "n_ld_pairs": 1},
        ],
        stats_path,
    )

    assert stats_path.read_text().splitlines() == [
        '{"ancestry":"afr","n_ld_pairs":0}',
        '{"ancestry":"nfe","n_ld_pairs":1}',
    ]


def test_validate_ld_registry_preserves_configured_ancestry_labels() -> None:
    """Registry resolution keeps logical labels and concrete paths unchanged."""
    references = FineMappingLocusSetLDAnnotationStep._normalize_ld_registry(
        [{"ancestry": "nfe", "vi_path": "eur.vi", "bm_path": "eur.bm"}]
    )

    assert references == {
        "nfe": {"ancestry": "nfe", "vi_path": "eur.vi", "bm_path": "eur.bm"}
    }


@pytest.mark.parametrize(
    "references",
    [
        [],
        [{"ancestry": "EUR", "vi_path": "eur.vi"}],
        [
            {"ancestry": "EUR", "vi_path": "eur.vi", "bm_path": "eur.bm"},
            {"ancestry": "EUR", "vi_path": "nfe.vi", "bm_path": "nfe.bm"},
        ],
    ],
)
def test_normalize_ld_registry_rejects_invalid_configuration(
    references: list[dict[str, str]],
) -> None:
    """Missing and duplicate ancestry references fail early."""
    with pytest.raises(ValueError):
        FineMappingLocusSetLDAnnotationStep._normalize_ld_registry(references)


def test_locus_variant_ids_deduplicates_in_input_order() -> None:
    """Repeated variants in collected locus arrays do not duplicate matrix indices."""
    assert FineMappingLocusSetLDAnnotationStep._locus_variant_ids(
        [
            {"variantId": "1_10_A_C"},
            {"variantId": "1_20_G_T"},
            {"variantId": "1_10_A_C"},
        ]
    ) == ["1_10_A_C", "1_20_G_T"]


def test_validate_reference_coverage_rejects_unregistered_ancestry() -> None:
    """Every observed ancestry must have an index and BlockMatrix reference."""
    with pytest.raises(ValueError, match="AFR"):
        FineMappingLocusSetLDAnnotationStep._validate_reference_coverage(
            ["EUR", "AFR"],
            {"EUR": {"vi_path": "eur.vi", "bm_path": "eur.bm"}},
        )
