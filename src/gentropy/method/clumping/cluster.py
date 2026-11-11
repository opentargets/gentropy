"""Split the variants of each study and chromosome into clusters at large gaps.

Window and locus-breaker clumping both start here. Window clumping solves each
cluster on its own to make pruning cheaper; locus-breaker clumping turns each
cluster into a locus.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def cluster(df: DataFrame, distance: int) -> DataFrame:
    """Number the clusters of each study and chromosome, adding ``clusterId``.

    Within each study and chromosome, variants are ordered by position. A
    variant starts a new cluster when it is the first one, or when the gap to
    the previous variant is greater than ``distance``. ``clusterId`` counts the
    cluster starts up to and including the variant, so the clusters of one
    study and chromosome are numbered 1, 2, 3 and so on from the left.

    Only gaps between adjacent variants are compared, so a cluster can be
    arbitrarily long: a chain of variants each 400 bp apart is one cluster for
    a distance of 500, however far its ends are from each other. A gap of
    exactly ``distance`` keeps the variants together.

    Args:
        df (DataFrame): Variants with ``studyId``, ``chromosome`` and
            ``position``.
        distance (int): The largest gap, in base pairs, that keeps two
            adjacent variants in one cluster.

    Returns:
        DataFrame: ``df`` with ``clusterId``, unique within a study and
            chromosome.

    Examples:
        A gap of 19 splits chromosome 1 in two; a gap of exactly 10 does not
        split chromosome 2. Each study and chromosome numbers its own
        clusters:

        >>> data = [
        ...     ("s1", "1", 2), ("s1", "1", 4), ("s1", "1", 12),
        ...     ("s1", "1", 31), ("s1", "1", 38),
        ...     ("s1", "2", 41), ("s1", "2", 51),
        ...     ("s2", "2", 55),
        ... ]
        >>> df = spark.createDataFrame(data, "studyId string, chromosome string, position int")
        >>> cluster(df, 10).orderBy("studyId", "chromosome", "position").show()
        +-------+----------+--------+---------+
        |studyId|chromosome|position|clusterId|
        +-------+----------+--------+---------+
        |     s1|         1|       2|        1|
        |     s1|         1|       4|        1|
        |     s1|         1|      12|        1|
        |     s1|         1|      31|        2|
        |     s1|         1|      38|        2|
        |     s1|         2|      41|        1|
        |     s1|         2|      51|        1|
        |     s2|         2|      55|        1|
        +-------+----------+--------+---------+
        <BLANKLINE>
    """
    by_position = Window.partitionBy("studyId", "chromosome").orderBy("position")
    gap = f.col("position") - f.lag("position").over(by_position)
    starts_cluster = f.when(gap.isNull() | (gap > distance), 1).otherwise(0)
    return df.withColumn(
        "clusterId",
        f.sum(starts_cluster).over(
            by_position.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
