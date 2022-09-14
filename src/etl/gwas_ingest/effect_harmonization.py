from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_reverse_complement(df: DataFrame, allele_col: str) -> DataFrame:
    """
    This function return with column of reverse complement allele of a spcified allele column

    Args:
      df (DataFrame): DataFrame
      allele_col (str): the name of the column containing the allele

    Returns:
      A dataframe with a new column called revcomp_{allele_col}
    """

    return df.withColumn(
        f"revcomp_{allele_col}",
        f.when(
            f.col(allele_col).rlike("[ACTG]+"),
            f.reverse(f.translate(f.col(allele_col), "ACTG", "TGAC")),
        ),
    )


def is_palindrom(df: DataFrame, allele_col: str) -> DataFrame:
    """
    It takes a dataframe and a column name, and returns a dataframe with a new column indicating
    whether allele is palindrom or not

    Args:
      df (DataFrame): DataFrame
      allele_col (str): str = 'allele'

    Returns:
      A dataframe with a new column called is_allele_palindrom.
    """

    return (
        df
        # Get reverse complement:
        .transform(lambda df: get_reverse_complement(df, "allele"))
        # Adding flag:
        .withColumn(
            f"is_{allele_col}_palindrom",
            f.when(f.col(f"revcomp_{allele_col}") == f.col(allele_col), True).otherwise(
                False
            ),
        ).drop(f"revcomp_{allele_col}")
    )
