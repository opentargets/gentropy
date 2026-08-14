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


class TestBigBrainSummaryStatistics:
    """Test methods of BigBrainSummaryStatistics."""

    raw_rows = [
        Row(
            # Allele == alt: no flip needed, effectAllele stays "C".
            feature="ENSG00000177757.2",
            variant_id="rs374545136",
            chr="chr1",
            pos=17556,
            ref="T",
            alt="C",
            Allele="C",
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
            # Allele == ref: effect allele is "T", so otherAllele should resolve to "C".
            feature="ENSG00000177757.2",
            variant_id="rs11111111",
            chr="chr1",
            pos=20000,
            ref="T",
            alt="C",
            Allele="T",
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
            Allele="G",
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
    ]

    def test_from_source(self, session: Session) -> None:
        """Test harmonisation: effect-allele resolution, filler-row drop, constant sample size."""
        raw = session.spark.createDataFrame(self.raw_rows, schema=FULL_ASSOC_SCHEMA)
        result = BigBrainSummaryStatistics.from_source(raw, "eqtl")

        assert isinstance(result, SummaryStatistics)
        rows = {row.variantId: row for row in result.df.collect()}

        # The filler row (fixed_beta == 0) must be dropped.
        assert len(rows) == 2

        # Allele == alt: no flip, variantId ends in effect allele "C".
        row1 = rows["1_17556_T_C"]
        assert row1.studyId == "BigBrain_eqtl_EUR_ENSG00000177757.2"
        assert row1.beta == -0.116786
        assert row1.sampleSize == 10_725
        assert row1.effectAlleleFrequencyFromSource is None
        assert row1.pValueExponent == -1

        # Allele == ref: otherAllele/effectAllele are resolved from Allele, not assumed from alt.
        row2 = rows["1_20000_C_T"]
        assert row2.beta == 0.25
        assert row2.pValueExponent == -174

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
            "https://zenodo.org/records/17226890/files/full_assoc.tsv.gz/content",
            output_path.as_posix(),
            session,
        )

        assert output_path.exists()
        assert output_path.read_bytes() == content
