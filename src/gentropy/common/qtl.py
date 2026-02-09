"""Quantitative trait loci (QTL) related enumerations."""

from __future__ import annotations

from enum import StrEnum


class QTLStudyType(StrEnum):
    """QTL study types."""

    # Bulk QTL types
    PQTL = "pqtl"
    EQTL = "eqtl"
    SQTL = "sqtl"
    TUQTL = "tuqtl"
    # single cell QTL types
    SC_PQTL = "scpqtl"
    SC_EQTL = "sceqtl"
    SC_SQTL = "scsqtl"
    SC_TUQTL = "sctuqtl"

    @classmethod
    def valid_qtl_types(cls) -> list[str]:
        """Get a list of valid QTL types."""
        return [qtl_type.value for qtl_type in cls]


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

    @classmethod
    def method_to_qcl_type_mapping(cls) -> dict[str, str]:
        """Mapping of quantification methods to QTL types."""
        return {
            QuantificationMethod.GE.value: QTLStudyType.EQTL.value,
            QuantificationMethod.EXON.value: QTLStudyType.EQTL.value,
            QuantificationMethod.TX.value: QTLStudyType.EQTL.value,
            QuantificationMethod.MICROARRAY.value: QTLStudyType.EQTL.value,
            QuantificationMethod.LEAFCUTTER.value: QTLStudyType.SQTL.value,
            QuantificationMethod.APTAMER.value: QTLStudyType.PQTL.value,
            QuantificationMethod.TXREV.value: QTLStudyType.TUQTL.value,
        }
