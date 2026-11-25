"""Read the FinnGen LD matrix into the LD index."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session

# FinnGen R12 LD: one tab-separated, bgzipped file per chromosome.
FINNGEN_LD_MATRIX_PATH = (
    "gs://finngen-public-data-r12/ld_matrix/finngen_r12_chr*_ld.tsv.gz"
)

# LD population label of FinnGen, as in `ldPopulationStructure`.
FINNGEN_LD_POPULATION = "fin"

# Columns of the FinnGen LD files.
_FINNGEN_LD_SCHEMA = (
    "`#chr` string, pos int, variant1 string, variant2 string, r double, r2 double"
)


class FinnGenLDMatrix:
    """FinnGen LD matrix, as published by FinnGen (R12: pairs within 3 Mb at r2 > 0.01).

    The files hold every pair in both orders, with GRCh38 variant IDs such as
    ``chr22_10557967_C_T``, and ``r`` computed by PLINK 2 ``--r-unphased``.

    !!! warning "The sign of ``r`` is for the minor alleles"

        FinnGen's ``r`` is signed for the minor allele of each variant, not for
        the alternate allele of its ID: a pair where exactly one variant has its
        alternate allele as the major allele has the opposite sign. It is kept as
        published, so only ``r`` squared is meaningful for FinnGen. To sign it for
        the alternate alleles, multiply ``r`` by -1 for each variant of the pair
        whose alternate allele frequency in FinnGen is 0.5 or more (``AF`` in the
        R12 variant annotation file).
    """

    @staticmethod
    def read(session: Session, path: str = FINNGEN_LD_MATRIX_PATH) -> DataFrame:
        """Read the FinnGen LD files.

        Args:
            session (Session): Session object.
            path (str): Path or glob of the LD files.

        Returns:
            DataFrame: One row per ordered pair: ``variant1``, ``variant2``, ``r`` and ``r2``.
        """
        return session.spark.read.csv(
            path, sep="\t", header=True, schema=_FINNGEN_LD_SCHEMA
        )

    @staticmethod
    def as_ld_index(pairs: DataFrame, min_r2: float) -> LDIndex:
        """Build the FinnGen LD index from the pairs of the LD files.

        Pairs with ``|r|`` of at least ``sqrt(min_r2)`` are kept, with the ``chr``
        prefix taken off the variant IDs. The files do not hold a variant with itself,
        so every variant of the files is added as its own tag at ``r`` 1, which marks
        it as in the panel even when no other variant passes the threshold.

        Args:
            pairs (DataFrame): The pairs, as from ``read``.
            min_r2 (float): Smallest r2 kept.

        Returns:
            LDIndex: The FinnGen pairs, of population ``fin``.

        Examples:
            >>> pairs = spark.createDataFrame(
            ...     [("22", 10, "chr22_10_C_T", "chr22_20_A_G", 0.9, 0.81),
            ...      ("22", 20, "chr22_20_A_G", "chr22_10_C_T", 0.9, 0.81),
            ...      ("22", 10, "chr22_10_C_T", "chr22_30_G_A", -0.3, 0.09),
            ...      ("22", 30, "chr22_30_G_A", "chr22_10_C_T", -0.3, 0.09)],
            ...     _FINNGEN_LD_SCHEMA,
            ... )
            >>> index = FinnGenLDMatrix.as_ld_index(pairs, min_r2=0.5)
            >>> sorted((r.population, r.chromosome, r.variantId, r.tagVariantId, r.r)
            ...        for r in index.df.collect())  # doctest: +NORMALIZE_WHITESPACE
            [('fin', '22', '22_10_C_T', '22_10_C_T', 1.0),
             ('fin', '22', '22_10_C_T', '22_20_A_G', 0.9),
             ('fin', '22', '22_20_A_G', '22_10_C_T', 0.9),
             ('fin', '22', '22_20_A_G', '22_20_A_G', 1.0),
             ('fin', '22', '22_30_G_A', '22_30_G_A', 1.0)]
        """
        ids = pairs.select(
            f.regexp_replace("variant1", "^chr", "").alias("variantId"),
            f.regexp_replace("variant2", "^chr", "").alias("tagVariantId"),
            "r",
        )
        # Both orders of each pair are in the files, so the first variants are all of them.
        itself = (
            ids.select("variantId")
            .distinct()
            .select(
                "variantId",
                f.col("variantId").alias("tagVariantId"),
                f.lit(1.0).alias("r"),
            )
        )
        kept = ids.where(f.abs("r") >= min_r2**0.5).unionByName(itself)
        return LDIndex(
            _df=kept.select(
                f.lit(FINNGEN_LD_POPULATION).alias("population"),
                f.split("variantId", "_").getItem(0).alias("chromosome"),
                "variantId",
                "tagVariantId",
                "r",
            ),
            _schema=LDIndex.get_schema(),
        )
