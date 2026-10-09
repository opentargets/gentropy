"""eQTL Catalogue datasource classes."""

from __future__ import annotations

from enum import StrEnum


class QuantificationMethod(StrEnum):
    """QTL quantification methods."""

    GE = "ge"
    EXON = "exon"
    TX = "tx"
    MICROARRAY = "microarray"
    LEAFCUTTER = "leafcutter"
    APTAMER = "aptamer"
    TXREV = "txrev"
    MAJIQ = "majiq"


class StudyType(StrEnum):
    """QTL study types."""

    PQTL = "pqtl"
    EQTL = "eqtl"
    SQTL = "sqtl"
    TUQTL = "tuqtl"
    # Single-cell QTL types
    SC_EQTL = "sceqtl"
    SC_SQTL = "scsqtl"
    SC_PQTL = "scpqtl"
    SC_TUQTL = "sctuqtl"
