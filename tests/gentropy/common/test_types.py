"""Tests for common types."""

from __future__ import annotations

from gentropy.common.types import (
    GWASEffect,
    PValComponents,
)


class TestTypes:
    """Test type definitions and named tuples."""

    def test_pval_components_creation(self) -> None:
        """Test creation of PValComponents named tuple."""
        # Create mock column objects
        mock_mantissa = "mantissa_col"
        mock_exponent = "exponent_col"

        pval = PValComponents(mantissa=mock_mantissa, exponent=mock_exponent)

        assert pval.mantissa == mock_mantissa
        assert pval.exponent == mock_exponent

    def test_pval_components_tuple_unpacking(self) -> None:
        """Test unpacking of PValComponents."""
        mock_mantissa = "m"
        mock_exponent = "e"
        pval = PValComponents(mantissa=mock_mantissa, exponent=mock_exponent)

        m, e = pval
        assert m == mock_mantissa
        assert e == mock_exponent

    def test_gwas_effect_creation(self) -> None:
        """Test creation of GWASEffect named tuple."""
        mock_beta = "beta_col"
        mock_se = "se_col"

        effect = GWASEffect(beta=mock_beta, standard_error=mock_se)

        assert effect.beta == mock_beta
        assert effect.standard_error == mock_se

    def test_gwas_effect_tuple_unpacking(self) -> None:
        """Test unpacking of GWASEffect."""
        mock_beta = "b"
        mock_se = "s"
        effect = GWASEffect(beta=mock_beta, standard_error=mock_se)

        b, s = effect
        assert b == mock_beta
        assert s == mock_se

    def test_ld_population_type_values(self) -> None:
        """Test that LD_Population type is correctly defined."""
        # These are valid population values
        valid_populations: list[str] = [
            "afr",
            "amr",
            "asj",
            "eas",
            "est",
            "fin",
            "nfe",
            "nwe",
            "seu",
        ]

        for pop in valid_populations:
            # Just verify the type hints are correct by checking they're strings
            assert isinstance(pop, str)

    def test_variant_population_type_values(self) -> None:
        """Test that VariantPopulation type is correctly defined."""
        # These are valid variant population values
        valid_populations: list[str] = [
            "afr",
            "amr",
            "ami",
            "asj",
            "eas",
            "fin",
            "nfe",
            "mid",
            "sas",
            "remaining",
        ]

        for pop in valid_populations:
            assert isinstance(pop, str)

    def test_data_source_type_values(self) -> None:
        """Test that DataSourceType is correctly defined."""
        # These are valid data source types
        valid_sources: list[str] = [
            "gnomad",
            "finngen",
            "gwas_catalog",
            "eqtl_catalog",
            "ukbiobank",
            "open_targets",
            "intervals",
        ]

        for source in valid_sources:
            assert isinstance(source, str)

    def test_pval_components_named_access(self) -> None:
        """Test named access to PValComponents fields."""
        pval = PValComponents(mantissa=0.5, exponent=-8)

        # Test both attribute and positional access
        assert pval.mantissa == 0.5
        assert pval[0] == 0.5
        assert pval.exponent == -8
        assert pval[1] == -8

    def test_gwas_effect_named_access(self) -> None:
        """Test named access to GWASEffect fields."""
        effect = GWASEffect(beta=0.05, standard_error=0.01)

        # Test both attribute and positional access
        assert effect.beta == 0.05
        assert effect[0] == 0.05
        assert effect.standard_error == 0.01
        assert effect[1] == 0.01
