"""Complex Portal protein-complex datasource.

This module ingests macromolecular complex data from the
[Complex Portal](https://www.ebi.ac.uk/complexportal/) in the **ComplexTAB**
flat-file format and converts it into the `MolecularComplex` dataset.

Two complementary files are expected:

- **Experimental** – complexes with direct experimental evidence.
- **Predicted** – computationally predicted complexes.

Both files are parsed with the same schema, unioned, and filtered to human
complexes only (NCBI taxonomy ID 9606).

The resulting `MolecularComplex` dataset is used downstream in the deCODE
proteomics pipeline to annotate multi-protein SomaScan aptamers with a
``molecularComplexId``.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.molecular_complex import MolecularComplex


class ComplexTab(Dataset):
    """Parser for the Complex Portal ComplexTAB flat-file format.

    This class reads the ComplexTAB TSV files distributed by the
    [Complex Portal](https://www.ebi.ac.uk/complexportal/) and transforms them
    into the `MolecularComplex` dataset.  It is not instantiated directly;
    use `from_complex_tab` to produce a `MolecularComplex` object.

    Class attributes:
        HUMAN_TAXONOMY_ID (int): NCBI taxonomy ID for *Homo sapiens* (9606).
            Rows with a different taxonomy are discarded during ingestion.
    """

    HUMAN_TAXONOMY_ID = 9606

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Return the raw Spark schema matching the ComplexTAB TSV column layout.

        The schema covers all columns present in the ComplexTAB export, including
        complex accession, taxonomy, participant list, evidence codes, cross
        references, and descriptive fields.

        Returns:
            t.StructType: Raw ComplexTAB schema with original column names.
        """
        return t.StructType(
            [
                t.StructField("#Complex ac", t.StringType(), True),
                t.StructField("Recommended name", t.StringType(), True),
                t.StructField("Aliases for complex", t.StringType(), True),
                t.StructField("Taxonomy identifier", t.IntegerType(), True),
                t.StructField(
                    "Identifiers (and stoichiometry) of molecules in complex",
                    t.StringType(),
                    True,
                ),
                t.StructField("Evidence Code", t.StringType(), True),
                t.StructField("Experimental evidence", t.StringType(), True),
                t.StructField("Go Annotations", t.StringType(), True),
                t.StructField("Cross references", t.StringType(), True),
                t.StructField("Description", t.StringType(), True),
                t.StructField("Complex properties", t.StringType(), True),
                t.StructField("Complex assembly", t.StringType(), True),
                t.StructField("Ligand", t.StringType(), True),
                t.StructField("Disease", t.StringType(), True),
                t.StructField("Agonist", t.StringType(), True),
                t.StructField("Antagonist", t.StringType(), True),
                t.StructField("Comment", t.StringType(), True),
                t.StructField("Source", t.StringType(), True),
                t.StructField("Expanded participant list", t.StringType(), True),
            ]
        )

    @classmethod
    def from_complex_tab(
        cls, session: Session, experimental: str, predicted: str
    ) -> MolecularComplex:
        """Parse experimental and predicted ComplexTAB files into a `MolecularComplex` dataset.

        Both input files are read with the ComplexTAB schema, unioned, and filtered
        to human complexes (taxonomy ID 9606). The following fields are extracted
        and renamed to match the `MolecularComplex` schema:

        - ``#Complex ac`` → ``id``
        - ``Description`` → ``description``
        - ``Complex properties`` → ``properties``
        - ``Complex assembly`` → ``assembly``
        - ``Expanded participant list`` → ``components`` (parsed by `_parse_components`)
        - ``Evidence Code`` → ``evidenceCodes`` (parsed by `_parse_evidence_code`)
        - ``Cross references`` → ``crossReferences`` (parsed by `_parse_cross_references`)
        - ``Source`` → ``source`` (parsed by `_parse_source`)

        Args:
            session (Session): Active Gentropy Spark session.
            experimental (str): Path to the experimental ComplexTAB TSV file.
            predicted (str): Path to the predicted ComplexTAB TSV file.

        Returns:
            MolecularComplex: Parsed and filtered molecular complex dataset.
        """
        e = session.load_data(
            experimental, fmt="tsv", schema=cls.get_schema(), nullValue="-"
        )
        pr = session.load_data(
            predicted, fmt="tsv", schema=cls.get_schema(), nullValue="-"
        )
        return MolecularComplex(
            _df=e.unionByName(pr)
            .filter(f.col("Taxonomy identifier") == cls.HUMAN_TAXONOMY_ID)
            .select(
                f.col("#Complex ac").alias("id"),
                f.col("Description").alias("description"),
                f.col("Complex properties").alias("properties"),
                f.col("Complex assembly").alias("assembly"),
                cls._parse_components(f.col("Expanded participant list")).alias(
                    "components"
                ),
                cls._parse_evidence_code(f.col("Evidence Code")).alias("evidenceCodes"),
                cls._parse_cross_references(f.col("Cross references")).alias(
                    "crossReferences"
                ),
                cls._parse_source(f.col("Source")).alias("source"),
            )
        )

    @staticmethod
    def _parse_source(c: Column) -> Column:
        """Parse the PSI-MI source field into a structured ``{id, source}`` record.

        The field has the format ``psi-mi:"MI:XXXX"(source name)``.

        Args:
            c (Column): Raw ``Source`` column.

        Returns:
            Column: Struct column with fields ``id`` (PSI-MI accession) and
                ``source`` (human-readable source name).
        """
        return f.struct(
            f.regexp_extract(c, r"psi-mi:\"(.*)\"\((.*)\)", 1).alias("id"),
            f.regexp_extract(c, r"psi-mi:\"(.*)\"\((.*)\)", 2).alias("source"),
        )

    @staticmethod
    def _parse_evidence_code(c: Column) -> Column:
        """Parse the pipe-delimited evidence code field into an array of ECO accessions.

        Each element of the pipe-separated list is expected to contain an ECO
        term in the form ``ECO:XXXXXXX``.

        Args:
            c (Column): Raw ``Evidence Code`` column.

        Returns:
            Column: Array of ECO accession strings (e.g. ``["ECO:0000353"]``).
        """
        return f.transform(
            f.split(c, r"\|"), lambda x: f.regexp_extract(x, r"(ECO:\d+)", 1)
        )

    @staticmethod
    def _parse_cross_references(c: Column) -> Column:
        """Parse the pipe-delimited cross-references field into an array of ``{source, id}`` records.

        Each element of the pipe-separated list is expected to have the format
        ``source:id(description)``.

        Args:
            c (Column): Raw ``Cross references`` column.

        Returns:
            Column: Array of structs with fields ``source`` (database name) and
                ``id`` (accession within that database).
        """
        return f.transform(
            f.split(c, r"\|"),
            lambda x: f.struct(
                f.regexp_extract(x, r"^(.*)\:(.*)\(.*\)$", 1).alias("source"),
                f.regexp_extract(x, r"^(.*)\:(.*)\(.*\)$", 2).alias("id"),
            ),
        )

    @staticmethod
    def _parse_components(c: Column) -> Column:
        """Parse the pipe-delimited expanded participant list into an array of component records.

        Each element of the pipe-separated list is expected to have the format
        ``UniProtID(stoichiometry)``, for example ``P04637(2)``.
        The ``source`` field is always set to ``"uniprot"``.

        Args:
            c (Column): Raw ``Expanded participant list`` column.

        Returns:
            Column: Array of structs with fields ``id`` (UniProt accession),
                ``stoichiometry`` (copy number as a string), and ``source``
                (always ``"uniprot"``).
        """
        return f.transform(
            f.split(c, r"\|"),
            lambda x: f.struct(
                f.regexp_extract(x, r"([\w-]+)\((\d+)\)", 1).alias("id"),
                f.regexp_extract(x, r"([\w-]+)\((\d+)\)", 2).alias("stoichiometry"),
                f.lit("uniprot").alias("source"),
            ),
        )
