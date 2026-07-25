"""Tests for the PanUKBB variant-index preparation step."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from gentropy.pan_ukb_ingestion import PanUKBBVariantIndexStep


def test_pan_ukbb_variant_index_step_normalizes_populations_for_all_outputs() -> None:
    """PanUKBB step expands requested populations into normalized full and filtered outputs."""
    session = MagicMock()
    session.write_mode = "overwrite"
    variant_annotation = MagicMock()
    variant_index = MagicMock(df=variant_annotation)
    csa_full_index = MagicMock()
    eur_full_index = MagicMock()
    afr_full_index = MagicMock()
    chr1_variants = MagicMock()
    full_test_variants = MagicMock()
    csa_chr1_filtered_index = MagicMock()
    csa_full_test_filtered_index = MagicMock()
    eur_chr1_filtered_index = MagicMock()
    eur_full_test_filtered_index = MagicMock()
    afr_chr1_filtered_index = MagicMock()
    afr_full_test_filtered_index = MagicMock()
    session.spark.read.parquet.side_effect = [
        csa_full_index,
        chr1_variants,
        full_test_variants,
        eur_full_index,
        chr1_variants,
        full_test_variants,
        afr_full_index,
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
            csa_chr1_filtered_index,
            csa_full_test_filtered_index,
            eur_chr1_filtered_index,
            eur_full_test_filtered_index,
            afr_chr1_filtered_index,
            afr_full_test_filtered_index,
        ]

        PanUKBBVariantIndexStep(
            session=session,
            variant_annotation_path="/variant-index",
            pan_ukbb_ht_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
            pan_ukbb_bm_path="gs://panukbb/UKBB.{POP}.ldadj",
            ukbb_annotation_path="/ld-reference/UKBB.{POP}.aligned.parquet",
            pan_ukbb_pops=["csa", "EUR", "afr"],
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
        ld_populations=["CSA", "EUR", "AFR"],
    )
    assert matrix.align_ld_index_alleles.call_args_list == [
        call(
            variant_annotation=variant_annotation,
            population="CSA",
            hail_table_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
            hail_table_output="/ld-reference/UKBB.{POP}.aligned.parquet",
        ),
        call(
            variant_annotation=variant_annotation,
            population="EUR",
            hail_table_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
            hail_table_output="/ld-reference/UKBB.{POP}.aligned.parquet",
        ),
        call(
            variant_annotation=variant_annotation,
            population="AFR",
            hail_table_path="gs://panukbb/UKBB.{POP}.ldadj.variant.b38",
            hail_table_output="/ld-reference/UKBB.{POP}.aligned.parquet",
        ),
    ]
    assert [args.args[0] for args in session.spark.read.parquet.call_args_list] == [
        "/ld-reference/UKBB.CSA.aligned.parquet",
        "/filters/chr1.parquet",
        "/filters/full-test.parquet",
        "/ld-reference/UKBB.EUR.aligned.parquet",
        "/filters/chr1.parquet",
        "/filters/full-test.parquet",
        "/ld-reference/UKBB.AFR.aligned.parquet",
        "/filters/chr1.parquet",
        "/filters/full-test.parquet",
    ]
    assert matrix.filter_ld_index_to_variants.call_args_list == [
        call(csa_full_index, chr1_variants),
        call(csa_full_index, full_test_variants),
        call(eur_full_index, chr1_variants),
        call(eur_full_index, full_test_variants),
        call(afr_full_index, chr1_variants),
        call(afr_full_index, full_test_variants),
    ]
    csa_chr1_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    csa_chr1_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/chr1/UKBB.CSA.aligned.parquet"
    )
    csa_full_test_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    csa_full_test_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/full_test/UKBB.CSA.aligned.parquet"
    )
    eur_chr1_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    eur_chr1_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/chr1/UKBB.EUR.aligned.parquet"
    )
    eur_full_test_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    eur_full_test_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/full_test/UKBB.EUR.aligned.parquet"
    )
    afr_chr1_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    afr_chr1_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/chr1/UKBB.AFR.aligned.parquet"
    )
    afr_full_test_filtered_index.write.mode.assert_called_once_with(session.write_mode)
    afr_full_test_filtered_index.write.mode.return_value.parquet.assert_called_once_with(
        "/ld-reference/full_test/UKBB.AFR.aligned.parquet"
    )
    assert matrix.write_filtered_block_matrix.call_args_list == [
        call(
            locus_index=csa_chr1_filtered_index,
            ancestry="CSA",
            output_path="/ld-reference/chr1/UKBB.CSA.ldadj",
        ),
        call(
            locus_index=csa_full_test_filtered_index,
            ancestry="CSA",
            output_path="/ld-reference/full_test/UKBB.CSA.ldadj",
        ),
        call(
            locus_index=eur_chr1_filtered_index,
            ancestry="EUR",
            output_path="/ld-reference/chr1/UKBB.EUR.ldadj",
        ),
        call(
            locus_index=eur_full_test_filtered_index,
            ancestry="EUR",
            output_path="/ld-reference/full_test/UKBB.EUR.ldadj",
        ),
        call(
            locus_index=afr_chr1_filtered_index,
            ancestry="AFR",
            output_path="/ld-reference/chr1/UKBB.AFR.ldadj",
        ),
        call(
            locus_index=afr_full_test_filtered_index,
            ancestry="AFR",
            output_path="/ld-reference/full_test/UKBB.AFR.ldadj",
        ),
    ]
