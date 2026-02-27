"""Tests for common processing functions."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as f

from gentropy.common.processing import extract_chromosome, parse_efos
from gentropy.common.session import Session


@pytest.mark.usefixtures("session")
class TestProcessing:
    """Test common processing functions."""

    def test_parse_efos_single_efo(self, session: Session) -> None:
        """Test parsing of a single EFO URI."""
        data = [("http://www.ebi.ac.uk/efo/EFO_0000001",)]
        df = session.spark.createDataFrame(data, schema="efos STRING")
        result = df.select(parse_efos(f.col("efos")).alias("parsed_efos")).collect()

        assert result[0]["parsed_efos"] == ["EFO_0000001"]

    def test_parse_efos_multiple_efos(self, session: Session) -> None:
        """Test parsing of multiple EFO URIs."""
        data = [
            (
                "http://www.ebi.ac.uk/efo/EFO_0000001,http://purl.obolibrary.org/obo/OBA_VT0001253,http://www.orpha.net/ORDO/Orphanet_101953",
            )
        ]
        df = session.spark.createDataFrame(data, schema="efos STRING")
        result = df.select(parse_efos(f.col("efos")).alias("parsed_efos")).collect()

        parsed = sorted(result[0]["parsed_efos"])
        expected = sorted(["EFO_0000001", "OBA_VT0001253", "Orphanet_101953"])
        assert parsed == expected

    def test_parse_efos_duplicate_handling(self, session: Session) -> None:
        """Test that parse_efos removes duplicates."""
        data = [
            (
                "http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002",
            )
        ]
        df = session.spark.createDataFrame(data, schema="efos STRING")
        result = df.select(parse_efos(f.col("efos")).alias("parsed_efos")).collect()

        parsed = sorted(result[0]["parsed_efos"])
        # Should only have 2 unique EFOs
        assert len(parsed) == 2
        assert sorted(["EFO_0000001", "EFO_0000002"]) == parsed

    def test_extract_chromosome_simple(self, session: Session) -> None:
        """Test extraction of chromosome from simple variant IDs."""
        data = [
            ("1_12345_A_T",),
            ("2_54321_G_C",),
            ("X_999999_T_A",),
        ]
        df = session.spark.createDataFrame(data, schema="variantId STRING")
        result = df.select(
            extract_chromosome(f.col("variantId")).alias("chromosome")
        ).collect()

        chromosomes = [row["chromosome"] for row in result]
        assert chromosomes == ["1", "2", "X"]

    def test_extract_chromosome_with_prefix(self, session: Session) -> None:
        """Test extraction of chromosome with 'chr' prefix."""
        data = [
            ("chr1_12345_A_T",),
            ("chrX_54321_G_C",),
        ]
        df = session.spark.createDataFrame(data, schema="variantId STRING")
        result = df.select(
            extract_chromosome(f.col("variantId")).alias("chromosome")
        ).collect()

        chromosomes = [row["chromosome"] for row in result]
        assert chromosomes == ["chr1", "chrX"]

    def test_extract_chromosome_complex(self, session: Session) -> None:
        """Test extraction of chromosome from complex variant IDs."""
        data = [
            ("15_KI270850v1_alt_48777_C_T",),
            ("GL000220.1_13000_A_G",),
        ]
        df = session.spark.createDataFrame(data, schema="variantId STRING")
        result = df.select(
            extract_chromosome(f.col("variantId")).alias("chromosome")
        ).collect()

        chromosomes = [row["chromosome"] for row in result]
        assert chromosomes[0] == "15_KI270850v1_alt"
        assert chromosomes[1] == "GL000220.1"
