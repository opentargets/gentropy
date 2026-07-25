"""Tests for the PanUKBB variant-index preparation step."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from gentropy.pan_ukb_ingestion import PanUKBBVariantIndexStep


def test_pan_ukbb_variant_index_step_writes_full_and_filtered_eur_outputs() -> None:
    """PanUKBB step writes full EUR index plus requested filtered indexes and matrices."""
    session = MagicMock()
    session.write_mode = "overwrite"
    variant_annotation = MagicMock()
    variant_index = MagicMock(df=variant_annotation)
    full_index = MagicMock()
    chr1_variants = MagicMock()
    full_test_variants = MagicMock()
    chr1_filtered_index = MagicMock()
    full_test_filtered_index = MagicMock()
    session.spark.read.parquet.side_effect = [
        full_index,
        chr1_variants,
        full_test_variants,
    ]

    with (
        patch(
            "gentropy.pan_ukb_ingestion.VariantIndex.from_parquet",
            return_value=variant_index,
        ) as mock_variant_index_from_parquet,
        patch("gentropy.pan_ukb_ingestion.PanUKBBLDMatrix") as mock_matrix_class,
    ):
        matrix = mock_matrix_class.return_value
        matrix.filter_ld_index_to_variants.side_effect = [
            chr1_filtered_index,
            full_test_filtered_index,
        ]

        PanUKBBVariantIndexStep(
            session=session,
            variant_annotation_path="/variant-index",
            pan_ukbb_ht_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
            pan_ukbb_bm_path="gs://panukbb/UKBB.{POP}.ldadj",
            ukbb_annotation_path="/ld-reference/UKBB.{POP}.aligned.parquet",
            pan_ukbb_pops=["EUR"],
            variant_filter_paths={
                "chr1": "/filters/chr1.parquet",
                "full_test": "/filters/full-test.parquet",
            },
            filtered_ukbb_annotation_path="/ld-reference/{FILTER}/UKBB.{POP}.aligned.parquet",
            filtered_pan_ukbb_bm_path="/ld-reference/{FILTER}/UKBB.{POP}.ldadj",
        )

    mock_variant_index_from_parquet.assert_called_once_with(
        session=session, path="/variant-index"
    )
    mock_matrix_class.assert_called_once_with(
        pan_ukbb_ht_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
        pan_ukbb_bm_path="gs://panukbb/UKBB.{POP}.ldadj",
        ukbb_annotation_path="/ld-reference/UKBB.{POP}.aligned.parquet",
        ld_populations=["EUR"],
    )
    matrix.align_ld_index_alleles.assert_called_once_with(
        variant_annotation=variant_annotation,
        population="EUR",
        hail_table_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
        hail_table_output="/ld-reference/UKBB.{POP}.aligned.parquet",
    )
    assert session.spark.read.parquet.call_args_list[0].args == (
        "/ld-reference/UKBB.EUR.aligned.parquet",
    )
    assert session.spark.read.parquet.call_args_list[1].args == (
        "/filters/chr1.parquet",
    )
    assert session.spark.read.parquet.call_args_list[2].args == (
        "/filters/full-test.parquet",
    )
    assert matrix.filter_ld_index_to_variants.call_args_list[0].args == (
        full_index,
        chr1_variants,
    )
    assert matrix.filter_ld_index_to_variants.call_args_list[1].args == (
        full_index,
        full_test_variants,
    )
    chr1_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    chr1_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/chr1/UKBB.EUR.aligned.parquet"
    )
    full_test_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    full_test_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/full_test/UKBB.EUR.aligned.parquet"
    )
    assert matrix.write_filtered_block_matrix.call_args_list[0].kwargs == {
        "locus_index": chr1_filtered_index,
        "ancestry": "EUR",
        "output_path": "/ld-reference/chr1/UKBB.EUR.ldadj",
    }
    assert matrix.write_filtered_block_matrix.call_args_list[1].kwargs == {
        "locus_index": full_test_filtered_index,
        "ancestry": "EUR",
        "output_path": "/ld-reference/full_test/UKBB.EUR.ldadj",
    }
