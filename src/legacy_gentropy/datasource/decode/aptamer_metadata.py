"""deCODE aptamer metadata module.

This module provides the `AptamerMetadata` dataset that maps each SomaScan
aptamer identifier to one or more human protein targets. The mapping table is derived
from the SomaScan study tables distributed by deCODE and is used during study-index
creation to annotate each assay with curated gene symbols and UniProt protein IDs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.common.spark import safe_split
from gentropy.dataset.dataset import Dataset


class AptamerMetadata(Dataset):
    """Mapping from SomaScan aptamer identifiers to protein target metadata.

    Each row describes a single SomaScan aptamer and its one or more protein targets.
    Multi-target aptamers (i.e. those measuring a protein complex) are flagged with
    ``isProteinComplex = True`` and their ``targetMetadata`` array will contain more
    than one element.

    The dataset is created from the aptamer study table supplied by deCODE and is used
    in `deCODEStudyIndex.from_manifest`
    to join aptamer-level annotations onto the per-study manifest.
    """

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Return the enforced Spark schema for `AptamerMetadata`.

        Returns:
            t.StructType: Expected schema containing ``aptamerId``, ``targetName``,
                ``targetFullName``, ``isProteinComplex``, and ``targetMetadata``.
        """
        return t.StructType(
            [
                t.StructField("aptamerId", t.StringType(), nullable=False),
                t.StructField("targetName", t.StringType(), nullable=False),
                t.StructField("targetFullName", t.StringType(), nullable=True),
                t.StructField("isProteinComplex", t.BooleanType(), nullable=False),
                t.StructField(
                    "targetMetadata",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField(
                                    "geneSymbol", t.StringType(), nullable=True
                                ),
                                t.StructField(
                                    "proteinId", t.StringType(), nullable=True
                                ),
                            ]
                        )
                    ),
                ),
            ]
        )

    @classmethod
    def from_source(cls, session: Session, path: str) -> AptamerMetadata:
        """Load and parse the deCODE aptamer metadata file.

        The file at ``path`` is expected to be a TSV with the SomaScan study table
        layout (columns: ``seqid``, ``target_name``, ``target_full_name``, ``gene_name``,
        ``uniprot``). Each ``seqid`` value is normalised by stripping the ``SeqId.`` prefix
        and converting the underscore-separated aptamer identifier to a hyphen-separated one.

        Args:
            session (Session): Gentropy session object used to load the source data.
            path (str): Path (local or remote) to the aptamer metadata TSV file.

        Returns:
            AptamerMetadata: Validated `AptamerMetadata` dataset.
        """
        data = session.load_data(path, fmt="tsv")
        return cls._transform_source(data)

    @classmethod
    def _transform_source(cls, df: DataFrame) -> AptamerMetadata:
        """Transform a raw aptamer study table into a validated `AptamerMetadata` dataset.

        The input DataFrame is expected to contain at minimum the columns
        ``seqid``, ``target_name``, ``target_full_name``, ``gene_name``, and ``uniprot``.
        Multi-valued ``gene_name`` and ``uniprot`` fields (comma-separated) are split
        into arrays and zipped to form the ``targetMetadata`` struct array.

        Args:
            df (DataFrame): Raw study table as loaded from the aptamer metadata file.

        Returns:
            AptamerMetadata: Validated deCODE aptamer metadata dataset.
        """
        return cls(
            _df=df.select(
                "seqid",
                "target_name",
                "target_full_name",
                "gene_name",
                "uniprot",
            )
            .select(
                f.regexp_replace(f.trim("seqid"), "SeqId.", "").alias("aptamerId"),
                f.trim("target_name").alias("targetName"),
                f.trim("target_full_name").alias("targetFullName"),
                safe_split(f.trim("gene_name"), ",").alias("geneSymbol"),
                safe_split(f.trim("uniprot"), ",").alias("proteinId"),
            )
            .withColumn(
                "targetMetadata",
                f.arrays_zip("geneSymbol", "proteinId").alias("targetMetadata"),
            )
            .withColumn("isProteinComplex", f.size(f.col("targetMetadata")) > 1)
            .select(
                "aptamerId",
                "targetName",
                "targetFullName",
                "isProteinComplex",
                "targetMetadata",
            )
            .distinct()
        )
