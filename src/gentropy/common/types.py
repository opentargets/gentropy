"""Types and type aliases used in the package."""

from enum import StrEnum
from typing import Literal, NamedTuple

from pyspark.sql.column import Column

VariantPopulation = Literal[
    "afr", "amr", "ami", "asj", "eas", "fin", "nfe", "mid", "sas", "remaining"
]


class LDPopulation(StrEnum):
    """Enum representing GnomAD LD populations."""

    AFR = "afr"
    """African population"""
    AMR = "amr"
    """American population"""
    ASJ = "asj"
    """Ashkenazi Jewish population"""
    EAS = "eas"
    """East Asian population"""
    NFE = "nfe"
    """Non-Finnish European population"""
    EST = "est"
    """Estonian population"""
    FIN = "fin"
    """Finnish population"""
    NWE = "nwe"
    """North-Western European population"""
    SEU = "seu"
    """Southern European population"""


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
