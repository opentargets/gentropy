"""Molecular complex dataset.

This module defines the `MolecularComplex` dataset, which stores curated and
predicted macromolecular (protein) complexes ingested from the
[Complex Portal](https://www.ebi.ac.uk/complexportal/) via the ComplexTAB flat-file
format.

The dataset is used in the deCODE proteomics pipeline to annotate multi-protein
SomaScan aptamers with a ``molecularComplexId`` when the set of UniProt IDs measured
by an aptamer matches a known complex.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import types as t


@dataclass
class MolecularComplex(Dataset):
    """Curated and predicted macromolecular complex annotations.

    Each row represents one complex entry sourced from the Complex Portal and
    contains the following information:

    - **id** (`str`) – Complex Portal accession (e.g. ``CPX-1``)
    - **description** (`str`) – Free-text description of the complex.
    - **properties** (`str`) – Additional complex property annotations.
    - **assembly** (`str`) – Known or predicted assembly state.
    - **components** (`array<struct<id, stoichiometry, source>>`) – List of
      component UniProt protein IDs with stoichiometry and source database.
    - **evidenceCodes** (`array<str>`) – ECO evidence codes supporting the
      complex annotation.
    - **crossReferences** (`array<struct<source, id>>`) – External database
      cross-references (e.g. PDB, GO).
    - **source** (`struct<id, source>`) – PSI-MI ontology term describing the
      data source.

    The schema is defined in ``assets/schemas/molecular_complex.json``.
    """

    @classmethod
    def get_schema(cls: type[MolecularComplex]) -> t.StructType:
        """Return the enforced Spark schema for `MolecularComplex`.

        The schema is loaded from the bundled JSON schema file
        ``assets/schemas/molecular_complex.json``.

        Returns:
            t.StructType: Schema for the `MolecularComplex` dataset.
        """
        return parse_spark_schema("molecular_complex.json")
