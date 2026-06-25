"""Types and type aliases used in the package."""

from typing import Literal, NamedTuple

from pyspark.sql import functions as f
from pyspark.sql.column import Column

LD_Population = Literal["afr", "amr", "asj", "eas", "est", "fin", "nfe", "nwe", "seu"]

VariantPopulation = Literal[
    "afr", "amr", "ami", "asj", "eas", "fin", "nfe", "mid", "sas", "remaining"
]
DataSourceType = Literal[
    "gnomad",
    "finngen",
    "gwas_catalog",
    "eqtl_catalog",
    "ukbiobank",
    "open_targets",
    "intervals",
]


class PValComponents(NamedTuple):
    """Components of p-value.

    Attributes:
        mantissa (Column): Mantissa of the p-value.
        exponent (Column): Exponent of the p-value.
    """

    mantissa: Column
    exponent: Column


class GWASEffect(NamedTuple):
    """Components of GWAS effect.

    Attributes:
        beta (Column): Effect.
        standard_error (Column): Effect standard error.
    """

    beta: Column
    standard_error: Column


class ReportedEffect(NamedTuple):
    """Components of reported effect.

    Attributes:
        beta (Column): Reported effect.
        standard_error (Column): Reported effect standard error.
    """

    beta: Column
    standard_error: Column
    p_value_mantissa: Column
    p_value_exponent: Column

    def to_struct(self) -> Column:
        """Convert to a struct column.

        Returns:
            Column: Struct column containing the reported effect components.
        """
        return f.struct(
            self.beta.alias("beta"),
            self.standard_error.alias("standardError"),
            self.p_value_mantissa.alias("pValueMantissa"),
            self.p_value_exponent.alias("pValueExponent"),
        )


class RescaledEffect(NamedTuple):
    """Components of rescaled effect.

    Attributes:
        abs_beta (Column): Rescaled effect.
        standard_error (Column): Rescaled effect standard error.
        direction_of_effect (Column): Direction of effect.
    """

    abs_beta: Column
    standard_error: Column
    direction_of_effect: Column

    def to_struct(self) -> Column:
        """Convert to a struct column.

        Returns:
            Column: Struct column containing the rescaled effect components.
        """
        return f.struct(
            self.abs_beta.alias("absBeta"),
            self.standard_error.alias("standardError"),
            self.direction_of_effect.alias("directionOfEffect"),
        )
