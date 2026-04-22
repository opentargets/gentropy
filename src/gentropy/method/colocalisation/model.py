"""Models for colocalisation methods."""

from typing import Any, Protocol

from pyspark.sql import Column
from pyspark.sql import functions as f

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap


class ColocalisationMethodInterface(Protocol):
    """Colocalisation method interface."""

    METHOD_NAME: str
    METHOD_METRICS: list[str]

    @classmethod
    def colocalise(
        cls, overlapping_signals: StudyLocusOverlap, **kwargs: Any
    ) -> Colocalisation:
        """Method to generate the colocalisation.

        Args:
            overlapping_signals (StudyLocusOverlap): Overlapping study loci.
            **kwargs (Any): Additional keyword arguments to the colocalise method.


        Returns:
            Colocalisation: loci colocalisation

        Raises:
            NotImplementedError: Implement in derivative classes.
        """
        raise NotImplementedError("Implement in derivative classes.")

    @staticmethod
    def get_tag_variant_source(statistics: Column) -> Column:
        """Get the source of the tag variant for a locus-overlap row.

        Args:
            statistics (Column): statistics column

        Returns:
            Column: source of the tag variant

        Examples:
            >>> data = [('a', 'b'),(None, 'b'),('a', None),]
            >>> (
            ...     spark.createDataFrame(data, ['a', 'b'])
            ...     .select(
            ...         'a', 'b',
            ...         ColocalisationMethodInterface.get_tag_variant_source(
            ...             f.struct(
            ...                 f.col('a').alias('left_posteriorProbability'),
            ...                 f.col('b').alias('right_posteriorProbability'),
            ...             )
            ...         ).alias('source')
            ...     )
            ...     .show()
            ... )
            +----+----+------+
            |   a|   b|source|
            +----+----+------+
            |   a|   b|  both|
            |NULL|   b| right|
            |   a|NULL|  left|
            +----+----+------+
            <BLANKLINE>
        """
        return (
            # Both posterior probabilities are not null:
            f.when(
                statistics.left_posteriorProbability.isNotNull()
                & statistics.right_posteriorProbability.isNotNull(),
                f.lit("both"),
            )
            # Only the left posterior probability is not null:
            .when(statistics.left_posteriorProbability.isNotNull(), f.lit("left"))
            # It must be right only:
            .otherwise(f.lit("right"))
        )
