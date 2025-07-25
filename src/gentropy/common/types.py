"""Types and type aliases used in the package."""

from typing import Literal, NamedTuple

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
