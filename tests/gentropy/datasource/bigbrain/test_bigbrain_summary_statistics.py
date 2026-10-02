"""Test BigBrain summary statistics."""

from __future__ import annotations

import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyspark.sql import Row

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.bigbrain.summary_statistics import (
    FULL_ASSOC_SCHEMA,
    BigBrainSummaryStatistics,
)

#: Window size used for the flipping-window join in tests (kept small on purpose;
#: must match whatever is passed to `from_source` as `flipping_window_size`).
TEST_WINDOW_SIZE = 10_000


class TestBigBrainSummaryStatistics:
    """Test methods of BigBrainSummaryStatistics."""

    raw_rows = [
        Row(
            # ref is always the effect allele; otherAllele resolves to alt.
            # Also matches gnomAD's own ref/alt order directly (direction=1): no
            # correction expected.
            feature="ENSG00000177757.2",
            variant_id="rs374545136",
            chr="chr1",
            pos=17556,
            ref="T",
            alt="C",
            fixed_beta=-0.116786,
            fixed_sd=0.0895386,
            fixed_z=-1.30431,
            Random_Z=1.30431,
            Fixed_P="0.19212790711186328",
            Random_P="0.19212790711186328",
            Fixed_bonf=1.0,
            Random_bonf=1.0,
            Fixed_FDR=0.6439818121792503,
            Random_FDR=0.6383092451154466,
        ),
        Row(
            # Matches gnomAD's flipped orientation: beta must be negated and
            # variantId normalised to gnomAD's originalVariantId ("1_20000_T_C").
            feature="ENSG00000177757.2",
            variant_id="rs11111111",
            chr="chr1",
            pos=20000,
            ref="T",
            alt="C",
            fixed_beta=0.25,
            fixed_sd=0.05,
            fixed_z=5.0,
            Random_Z=5.0,
            Fixed_P="2.80606e-174",
            Random_P="2.80606e-174",
            Fixed_bonf=1.0e-170,
            Random_bonf=1.0e-170,
            Fixed_FDR=1.0e-170,
            Random_FDR=1.0e-170,
        ),
        Row(
            # Filler/untested row (fixed_beta == 0) — must be dropped.
            feature="ENSG00000177757.2",
            variant_id="rs22222222",
            chr="chr1",
            pos=30000,
            ref="A",
            alt="G",
            fixed_beta=0.0,
            fixed_sd=0.0,
            fixed_z=0.0,
            Random_Z=0.0,
            Fixed_P="1",
            Random_P="1",
            Fixed_bonf=1.0,
            Random_bonf=1.0,
            Fixed_FDR=1.0,
            Random_FDR=1.0,
        ),
        Row(
            # Absent from gnomAD entirely — kept unchanged (no evidence of a problem).
            feature="ENSG00000177757.2",
            variant_id="rs33333333",
            chr="chr1",
            pos=40000,
            ref="G",
            alt="A",
            fixed_beta=0.5,
            fixed_sd=0.1,
            fixed_z=5.0,
            Random_Z=5.0,
            Fixed_P="0.01",
            Random_P="0.01",
            Fixed_bonf=1.0,
            Random_bonf=1.0,
            Fixed_FDR=1.0,
            Random_FDR=1.0,
        ),
        Row(
            # Position is in gnomAD (as "1_50000_C_T"), but reported alleles ("A"/"G")
            # match neither orientation of gnomAD's actual alleles ("C"/"T") — a
            # confirmed allele mismatch, must be dropped.
            feature="ENSG00000177757.2",
            variant_id="rs44444444",
            chr="chr1",
            pos=50000,
            ref="A",
            alt="G",
            fixed_beta=0.3,
            fixed_sd=0.1,
            fixed_z=3.0,
            Random_Z=3.0,
            Fixed_P="0.01",
            Random_P="0.01",
            Fixed_bonf=1.0,
            Random_bonf=1.0,
            Fixed_FDR=1.0,
            Random_FDR=1.0,
        ),
        Row(
            # Indel whose event gnomAD lists in both orientations (e.g.
            # "1_60000_T_TG" and "1_60000_TG_T" as separate source variants). Must
            # resolve to exactly one output row, on the exact/direct match.
            feature="ENSG00000177757.2",
            variant_id="rs55555555",
            chr="chr1",
            pos=60000,
            ref="TG",
            alt="T",
            fixed_beta=0.4,
            fixed_sd=0.1,
            fixed_z=4.0,
            Random_Z=4.0,
            Fixed_P="0.01",
            Random_P="0.01",
            Fixed_bonf=1.0,
            Random_bonf=1.0,
            Fixed_FDR=1.0,
            Random_FDR=1.0,
        ),
    ]

    vd_rows = [
        Row(
            chromosome="1",
            rangeId=17556 // TEST_WINDOW_SIZE,
            variantId="1_17556_C_T",
            originalVariantId="1_17556_C_T",
            direction=1,
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=20000 // TEST_WINDOW_SIZE,
            variantId="1_20000_C_T",
            originalVariantId="1_20000_T_C",
            direction=-1,
            strand=1,
        ),
        # Real gnomAD variant at 50000 has different alleles than BigBrain reports.
        Row(
            chromosome="1",
            rangeId=50000 // TEST_WINDOW_SIZE,
            variantId="1_50000_C_T",
            originalVariantId="1_50000_C_T",
            direction=1,
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=50000 // TEST_WINDOW_SIZE,
            variantId="1_50000_T_C",
            originalVariantId="1_50000_C_T",
            direction=-1,
            strand=1,
        ),
        # gnomAD lists the same 60000 indel event in both orientations as separate
        # source variants — otherAllele="T"/effectAllele="TG" gives BigBrain
        # variantId "1_60000_T_TG", which matches BOTH of the next two rows.
        Row(
            chromosome="1",
            rangeId=60000 // TEST_WINDOW_SIZE,
            variantId="1_60000_T_TG",
            originalVariantId="1_60000_T_TG",
            direction=1,
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=60000 // TEST_WINDOW_SIZE,
            variantId="1_60000_T_TG",
            originalVariantId="1_60000_TG_T",
            direction=-1,
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=60000 // TEST_WINDOW_SIZE,
            variantId="1_60000_TG_T",
            originalVariantId="1_60000_TG_T",
            direction=1,
            strand=1,
        ),
        Row(
            chromosome="1",
            rangeId=60000 // TEST_WINDOW_SIZE,
            variantId="1_60000_TG_T",
            originalVariantId="1_60000_T_TG",
            direction=-1,
            strand=1,
        ),
    ]

    def test_from_source(self, session: Session) -> None:
        """Test harmonisation: effect-allele resolution, gnomAD orientation correction, filler-row drop, constant sample size."""
        raw = session.spark.createDataFrame(self.raw_rows, schema=FULL_ASSOC_SCHEMA)
        vd_df = session.spark.createDataFrame(self.vd_rows)
        variant_direction = MagicMock(df=vd_df)

        result = BigBrainSummaryStatistics.from_source(
            raw, "eqtl", variant_direction, flipping_window_size=TEST_WINDOW_SIZE
        )

        assert isinstance(result, SummaryStatistics)
        collected = result.df.collect()
        rows = {row.variantId: row for row in collected}

        # No duplicate variantIds: the indel-both-orientations case must not
        # produce two contradictory output rows for the same input variant.
        assert len(collected) == len(rows)

        # The filler row (fixed_beta == 0) and the confirmed allele-mismatch row
        # (position 50000) must both be dropped.
        assert len(rows) == 4

        # Direct gnomAD match (direction=1): unchanged.
        row1 = rows["1_17556_C_T"]
        assert row1.studyId == "BigBrain_eqtl_EUR_ENSG00000177757.2"
        assert row1.beta == -0.116786
        assert row1.sampleSize == 10_725
        assert row1.effectAlleleFrequencyFromSource is None
        assert row1.pValueExponent == -1

        # Flipped gnomAD match (direction=-1): beta negated, variantId normalised.
        row2 = rows["1_20000_T_C"]
        assert row2.beta == -0.25
        assert row2.pValueExponent == -174

        # Absent from gnomAD entirely: kept unchanged.
        row3 = rows["1_40000_A_G"]
        assert row3.beta == 0.5

        # Indel listed in both orientations in gnomAD: resolves to exactly one
        # row, on the exact/direct match (direction=1, no flip).
        row4 = rows["1_60000_T_TG"]
        assert row4.beta == 0.4

    @patch("gentropy.datasource.bigbrain.summary_statistics.requests.get")
    def test_download_tsv_gz(
        self, mock_get: MagicMock, tmp_path: Path, session: Session
    ) -> None:
        """Test that download_tsv_gz streams response chunks to the destination path."""
        content = gzip.compress(b"feature\tvariant_id\nENSG1\trs123\n")

        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [content]
        mock_get.return_value = mock_response

        output_path = tmp_path / "full_assoc.tsv.gz"
        BigBrainSummaryStatistics.download_tsv_gz(
            "https://zenodo.org/api/records/17226890/files/BigBrain_cis_eQTL_EUR_full_assoc.tsv.gz/content",
            output_path.as_posix(),
            session,
        )

        assert output_path.exists()
        assert output_path.read_bytes() == content
