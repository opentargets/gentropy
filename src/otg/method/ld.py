"""Performing linkage disequilibrium (LD) operations."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

from otg.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from otg.common.session import Session
    from pyspark.sql import Column
    from otg.dataset.study_index import StudyIndexGWASCatalog
    from otg.dataset.study_locus import (
        StudyLocus,
        StudyLocusGWASCatalog,
    )

from hail.linalg import BlockMatrix


class LDAnnotatorGnomad:
    """Class to annotate linkage disequilibrium (LD) operations from GnomAD."""

    @staticmethod
    def _query_block_matrix(
        bm: BlockMatrix,
        idxs: list[int],
        starts: list[int],
        stops: list[int],
        min_r2: float,
    ) -> DataFrame:
        """Query block matrix for idxs rows sparsified by start/stop columns.

        Args:
            bm (BlockMatrix): LD matrix containing r values
            idxs (List[int]): Row indexes to query (distinct and incremenetal)
            starts (List[int]): Interval start column indexes (same size as idxs)
            stops (List[int]): Interval start column indexes (same size as idxs)
            min_r2 (float): Minimum r2 to keep

        Returns:
            DataFrame: i,j,r where i and j are the row and column indexes and r is the LD

        Examples:
            >>> import numpy as np
            >>> r = np.array([[1, 0.8, 0.7, 0.2],
            ...               [0.8, 1, 0.6, 0.1],
            ...               [0.7, 0.6, 1, 0.3],
            ...               [0.2, 0.1, 0.3, 1]])
            >>> bm_r = BlockMatrix.from_numpy(r) # doctest: +SKIP
            >>> LDAnnotatorGnomad._query_block_matrix(bm_r, [1, 2], [0, 1], [3, 4], 0.5).show() # doctest: +SKIP
            +---+---+---+
            |  i|  j|  r|
            +---+---+---+
            |  0|  0|0.8|
            |  0|  1|1.0|
            |  1|  2|1.0|
            +---+---+---+
            <BLANKLINE>
        """
        bm_sparsified = bm.filter_rows(idxs).sparsify_row_intervals(
            starts, stops, blocks_only=True
        )
        entries = bm_sparsified.entries(keyed=False)

        return (
            entries.rename({"entry": "r"})
            .to_spark()
            .filter(f.col("r") ** 2 >= min_r2)
            .withColumn("r", f.when(f.col("r") >= 1, f.lit(1)).otherwise(f.col("r")))
        )

    @staticmethod
    def _variant_coordinates_in_ldindex(
        variants_df: DataFrame,
        ld_index: LDIndex,
    ) -> DataFrame:
        """Idxs for variants, first variant in the region and last variant in the region in precomputed ld index.

        Args:
            variants_df (DataFrame): Lead variants from `_annotate_index_intervals` output
            ld_index (LDIndex): LD index precomputed

        Returns:
            DataFrame: LD coordinates [i, idxs, start_idx and stop_idx]
        """
        w = Window.orderBy("chromosome", "idx")
        return (
            variants_df.join(
                ld_index.df,
                on=["chromosome", "position", "referenceAllele", "alternateAllele"],
            )  # start idx > stop idx in rare occasions due to liftover
            .filter(f.col("start_idx") < f.col("stop_idx"))
            .groupBy("chromosome", "idx")
            .agg(
                f.min("start_idx").alias("start_idx"),
                f.max("stop_idx").alias("stop_idx"),
            )
            # necessary to resolve return of .entries() function
            .withColumn("i", f.row_number().over(w))
        )

    @staticmethod
    def weighted_r_overall(
        chromosome: Column,
        study_id: Column,
        variant_id: Column,
        tag_variant_id: Column,
        relative_sample_size: Column,
        r: Column,
    ) -> Column:
        """Aggregation of weighted R information using ancestry proportions.

        The method implements a simple average weighted by the relative population sizes.

        Args:
            chromosome (Column): Chromosome
            study_id (Column): Study identifier
            variant_id (Column): Variant identifier
            tag_variant_id (Column): Tag variant identifier
            relative_sample_size (Column): Relative sample size
            r (Column): Correlation

        Returns:
            Column: Estimates weighted R information

        Exmples:
            >>> data = [('t3', 0.25, 0.2), ('t3', 0.25, 0.2), ('t3', 0.5, 0.99)]
            >>> columns = ['tag_variant_id', 'relative_sample_size', 'r']
            >>> (
            ...    spark.createDataFrame(data, columns)
            ...     .withColumn('chr', f.lit('chr1'))
            ...     .withColumn('study_id', f.lit('s1'))
            ...     .withColumn('variant_id', f.lit('v1'))
            ...     .withColumn(
            ...         'r_overall',
            ...         LDAnnotatorGnomad.weighted_r_overall(
            ...             f.col('chr'),
            ...             f.col('study_id'),
            ...             f.col('variant_id'),
            ...             f.col('tag_variant_id'),
            ...             f.col('relative_sample_size'),
            ...             f.col('r')
            ...         )
            ...     )
            ...     .show()
            ... )
            +--------------+--------------------+----+----+--------+----------+---------+
            |tag_variant_id|relative_sample_size|   r| chr|study_id|variant_id|r_overall|
            +--------------+--------------------+----+----+--------+----------+---------+
            |            t3|                0.25| 0.2|chr1|      s1|        v1|    0.595|
            |            t3|                0.25| 0.2|chr1|      s1|        v1|    0.595|
            |            t3|                 0.5|0.99|chr1|      s1|        v1|    0.595|
            +--------------+--------------------+----+----+--------+----------+---------+
            <BLANKLINE>
        """
        pseudo_r = f.when(r >= 1, 0.9999995).otherwise(r)
        return f.round(
            f.sum(pseudo_r * relative_sample_size).over(
                Window.partitionBy(chromosome, study_id, variant_id, tag_variant_id)
            ),
            6,
        )

    @staticmethod
    def _flag_partial_mapped(
        study_id: Column, variant_id: Column, tag_variant_id: Column
    ) -> Column:
        """Generate flag for lead/tag pairs.

        Some lead variants can be resolved in one population but not in other. Those rows interfere with PICS calculation, so they needs to be dropped.

        Args:
            study_id (Column): Study identifier column
            variant_id (Column): Identifier of the lead variant
            tag_variant_id (Column): Identifier of the tag variant

        Returns:
            Column: Boolean

        Examples:
            >>> data = [
            ...     ('study_1', 'lead_1', 'tag_1'),  # <- keep row as tag available.
            ...     ('study_1', 'lead_1', 'tag_2'),  # <- keep row as tag available.
            ...     ('study_1', 'lead_2', 'tag_3'),  # <- keep row as tag available
            ...     ('study_1', 'lead_2', None),  # <- drop row as lead 2 is resolved.
            ...     ('study_1', 'lead_3', None)   # <- keep row as lead 3 is not resolved.
            ... ]
            >>> (
            ...     spark.createDataFrame(data, ['studyId', 'variantId', 'tagVariantId'])
            ...     .withColumn("flag_to_keep_tag", LDAnnotatorGnomad._flag_partial_mapped(f.col('studyId'), f.col('variantId'), f.col('tagVariantId')))
            ...     .show()
            ... )
            +-------+---------+------------+----------------+
            |studyId|variantId|tagVariantId|flag_to_keep_tag|
            +-------+---------+------------+----------------+
            |study_1|   lead_1|       tag_1|            true|
            |study_1|   lead_1|       tag_2|            true|
            |study_1|   lead_2|       tag_3|            true|
            |study_1|   lead_2|        null|           false|
            |study_1|   lead_3|        null|            true|
            +-------+---------+------------+----------------+
            <BLANKLINE>
        """
        return tag_variant_id.isNotNull() | ~f.array_contains(
            f.collect_set(tag_variant_id.isNotNull()).over(
                Window.partitionBy(study_id, variant_id)
            ),
            True,
        )

    @classmethod
    def variants_in_ld_in_gnomad_pop(
        cls: type[LDAnnotatorGnomad],
        variants_df: DataFrame,
        ld_matrix: BlockMatrix,
        ld_index: LDIndex,
        min_r2: float,
    ) -> DataFrame:
        """Return LD annotation for variants in specific gnomad population.

        Args:
            variants_df (DataFrame): variants to annotate
            ld_matrix (BlockMatrix): LD matrix
            ld_index (LDIndex): LD index precomputed
            min_r2 (float): minimum r2 to keep

        Returns:
            DataFrame: LD information in the columns ["variantId", "chromosome", "gnomadPopulation", "tagVariantId", "r"]
        """
        # map variants to precomputed LD indexes from gnomAD
        variants_ld_coordinates = LDAnnotatorGnomad._variant_coordinates_in_ldindex(
            variants_df, ld_index
        ).persist()

        # idxs for lead, first variant in the region and last variant in the region
        entries = LDAnnotatorGnomad._query_block_matrix(
            ld_matrix + ld_matrix.T,
            variants_ld_coordinates.rdd.map(lambda x: x.idxs).collect(),
            variants_ld_coordinates.rdd.map(lambda x: x.starts).collect(),
            variants_ld_coordinates.rdd.map(lambda x: x.stops).collect(),
            min_r2,
        )

        return (
            entries.join(
                f.broadcast(variants_ld_coordinates),
                on="i",
                how="inner",
            )
            .select("variantId", "chromosome", "gnomadPopulation", "j", "r")
            .alias("left")
            .join(
                ld_index.df.select(
                    f.col("chromosome"),
                    f.col("variantId").alias("tagVariantId"),
                    f.col("idx").alias("tag_idx"),
                ).alias("tags"),
                on=[
                    f.col("left.chromosome") == f.col("tags.chromosome"),
                    f.col("left.j") == f.col("tags.tag_idx"),
                ],
            )
            .select(
                "variantId", "leads.chromosome", "gnomadPopulation", "tagVariantId", "r"
            )
        )

    @classmethod
    def ld_annotation_by_locus_ancestry(
        cls: type[LDAnnotatorGnomad],
        session: Session,
        associations: StudyLocusGWASCatalog,
        studies: StudyIndexGWASCatalog,
        ld_populations: list[str],
        ld_index_template: str,
        ld_matrix_template: str,
        min_r2: float,
    ) -> DataFrame:
        """LD information for all locus and ancestries.

        Args:
            session (Session): Session
            associations (StudyLocusGWASCatalog): GWAS associations
            studies (StudyIndexGWASCatalog): study metadata of the associations
            ld_populations (list[str]): List of populations to annotate
            ld_index_template (str): Template path of the LD matrix index containing `{POP}` where the population is expected
            ld_matrix_template (str): Template path of the LD matrix containing `{POP}` where the population is expected
            min_r2 (float): minimum r2 to keep

        Returns:
            DataFrame: LD annotation ["variantId", "chromosome", "gnomadPopulation", "tagVariantId", "r"]
        """
        # Unique lead - population pairs:
        locus_ancestry = (
            associations.unique_study_locus_ancestries(studies)
            # Ignoring study information / relativeSampleSize to get unique lead-ancestry pairs
            .drop("studyId", "relativeSampleSize")
            .distinct()
            .persist()
        )

        # All gnomad populations captured in associations:
        assoc_populations = locus_ancestry.rdd.map(
            lambda x: x.gnomadPopulation
        ).collect()

        # Retrieve LD information from gnomAD
        ld_annotated_assocs = []
        for population in ld_populations:
            if population in assoc_populations:
                pop_parsed_ldindex_path = ld_index_template.format(POP=population)
                pop_matrix_path = ld_matrix_template.format(POP=population)
                variants_in_pop = locus_ancestry.filter(
                    f.col("gnomadPopulation") == population
                )
                ld_annotated_assocs.append(
                    LDAnnotatorGnomad.variants_in_ld_in_gnomad_pop(
                        variants_df=variants_in_pop,
                        ld_matrix=BlockMatrix.read(pop_matrix_path),
                        ld_index=LDIndex.from_parquet(session, pop_parsed_ldindex_path),
                        min_r2=min_r2,
                    )
                )
        return reduce(DataFrame.unionByName, ld_annotated_assocs)


class LDclumping:
    """LD clumping reports the most significant genetic associations in a region in terms of a smaller number of “clumps” of genetically linked SNPs."""

    @staticmethod
    def _is_lead_linked(
        study_id: Column,
        variant_id: Column,
        p_value_exponent: Column,
        p_value_mantissa: Column,
        credible_set: Column,
    ) -> Column:
        """Evaluates whether a lead variant is linked to a tag (with lowest p-value) in the same studyLocus dataset.

        Args:
            study_id (Column): studyId
            variant_id (Column): Lead variant id
            p_value_exponent (Column): p-value exponent
            p_value_mantissa (Column): p-value mantissa
            credible_set (Column): Credible set <array of structs>

        Returns:
            Column: Boolean in which True indicates that the lead is linked to another tag in the same dataset.
        """
        leads_in_study = f.collect_set(variant_id).over(Window.partitionBy(study_id))
        tags_in_study_locus = f.transform(credible_set, lambda x: x.tagVariantId)
        intersect_lead_tags = f.array_intersect(leads_in_study, tags_in_study_locus)
        rank = f.row_number().over(
            Window.partitionBy(study_id, intersect_lead_tags).orderBy(
                p_value_exponent, p_value_mantissa
            )
        )
        return rank > 1

    @classmethod
    def clump(cls: type[LDclumping], associations: StudyLocus) -> StudyLocus:
        """Perform clumping on studyLocus dataset.

        Args:
            associations (StudyLocus): StudyLocus dataset

        Returns:
            StudyLocus: including flag and removing credibleSet information for LD clumped loci.
        """
        return associations.clump()
