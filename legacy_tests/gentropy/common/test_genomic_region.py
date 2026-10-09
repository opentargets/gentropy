"""Tests for genomic region utilities."""

from __future__ import annotations

import pytest

from gentropy.common.genomic_region import GenomicRegion, KnownGenomicRegions


class TestGenomicRegion:
    """Test GenomicRegion class."""

    def test_genomic_region_initialization(self) -> None:
        """Test GenomicRegion object creation."""
        region = GenomicRegion(chromosome="6", start=28510120, end=33480577)
        assert region.chromosome == "6"
        assert region.start == 28510120
        assert region.end == 33480577

    def test_genomic_region_str_representation(self) -> None:
        """Test string representation of GenomicRegion."""
        region = GenomicRegion(chromosome="6", start=28510120, end=33480577)
        assert str(region) == "6:28510120-33480577"

    def test_genomic_region_from_string_simple(self) -> None:
        """Test parsing simple genomic region string."""
        region = GenomicRegion.from_string("6:28510120-33480577")
        assert region.chromosome == "6"
        assert region.start == 28510120
        assert region.end == 33480577

    def test_genomic_region_from_string_with_chr_prefix(self) -> None:
        """Test parsing genomic region string with chr prefix."""
        region = GenomicRegion.from_string("chr6:28510120-33480577")
        assert region.chromosome == "6"
        assert region.start == 28510120
        assert region.end == 33480577

    def test_genomic_region_from_string_with_commas(self) -> None:
        """Test parsing genomic region string with commas in positions."""
        region = GenomicRegion.from_string("chr6:28,510,120-33,480,577")
        assert region.chromosome == "6"
        assert region.start == 28510120
        assert region.end == 33480577

    def test_genomic_region_from_string_invalid_format(self) -> None:
        """Test parsing invalid genomic region string format."""
        with pytest.raises(
            ValueError, match="Genomic region should follow a ##:####-#### format"
        ):
            GenomicRegion.from_string("6:28510120")

    def test_genomic_region_from_string_invalid_positions(self) -> None:
        """Test parsing genomic region string with non-integer positions."""
        with pytest.raises(
            ValueError,
            match="Start and the end position of the region has to be integer",
        ):
            GenomicRegion.from_string("6:28510120-abc")

    def test_genomic_region_from_string_malformed(self) -> None:
        """Test parsing malformed genomic region string."""
        # Note: The implementation treats "invalid:region:format" -> "invalid-region-format"
        # and then tries to parse positions, which fails with integer conversion error
        with pytest.raises(
            ValueError,
            match="Start and the end position of the region has to be integer",
        ):
            GenomicRegion.from_string("invalid:region:format")

    def test_genomic_region_from_known_genomic_region(self) -> None:
        """Test creating GenomicRegion from known genomic region."""
        region = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        assert region.chromosome == "6"
        assert region.start == 25726063
        assert region.end == 33400556

    def test_genomic_region_from_known_genomic_region_mhc_string(self) -> None:
        """Test MHC region string representation."""
        region = GenomicRegion.from_known_genomic_region(KnownGenomicRegions.MHC)
        assert str(region) == "6:25726063-33400556"

    def test_genomic_region_equality(self) -> None:
        """Test equality of GenomicRegion objects."""
        region1 = GenomicRegion(chromosome="6", start=28510120, end=33480577)
        region2 = GenomicRegion.from_string("6:28510120-33480577")
        # Note: GenomicRegion doesn't define __eq__, so we compare attributes
        assert region1.chromosome == region2.chromosome
        assert region1.start == region2.start
        assert region1.end == region2.end

    def test_genomic_region_different_chromosomes(self) -> None:
        """Test GenomicRegion with different chromosomes."""
        region_chr1 = GenomicRegion.from_string("1:1000-2000")
        region_chr22 = GenomicRegion.from_string("22:3000-4000")
        assert region_chr1.chromosome == "1"
        assert region_chr22.chromosome == "22"

    def test_genomic_region_large_positions(self) -> None:
        """Test GenomicRegion with large position numbers."""
        region = GenomicRegion.from_string("chr21:1000000-249250621")
        assert region.chromosome == "21"
        assert region.start == 1000000
        assert region.end == 249250621

    @pytest.mark.parametrize(
        "region_string,expected_chr,expected_start,expected_end",
        [
            ("1:100-200", "1", 100, 200),
            ("chr2:1000-2000", "2", 1000, 2000),
            ("3:10,000-20,000", "3", 10000, 20000),
            ("chrX:100-200", "X", 100, 200),
            ("chrY:1-100", "Y", 1, 100),
            ("chrMT:1-16569", "MT", 1, 16569),
        ],
    )
    def test_genomic_region_various_formats(
        self,
        region_string: str,
        expected_chr: str,
        expected_start: int,
        expected_end: int,
    ) -> None:
        """Test GenomicRegion parsing with various valid formats."""
        region = GenomicRegion.from_string(region_string)
        assert region.chromosome == expected_chr
        assert region.start == expected_start
        assert region.end == expected_end


class TestKnownGenomicRegions:
    """Test KnownGenomicRegions enum."""

    def test_mhc_region_value(self) -> None:
        """Test MHC region value."""
        assert KnownGenomicRegions.MHC.value == "chr6:25726063-33400556"

    def test_known_regions_enum_members(self) -> None:
        """Test that KnownGenomicRegions has expected members."""
        # Verify MHC is available
        assert hasattr(KnownGenomicRegions, "MHC")
        # Verify it's an enum with string values
        assert isinstance(KnownGenomicRegions.MHC.value, str)
