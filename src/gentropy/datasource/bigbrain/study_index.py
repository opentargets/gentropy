"""BigBrain eQTL/sQTL study index module.

This module provides `BigBrainStudyIndex`, a set of static factory helpers that
build a `StudyIndex` from the distinct set of features observed in the harmonised
summary statistics, together with a feature-to-gene mapping.

Unlike deCODE, BigBrain's eQTL `feature` is already a versioned Ensembl gene ID
(no protein-complex/TargetIndex resolution needed), and sQTL's `feature` (a
Leafcutter intron-cluster coordinate string) is resolved to a gene via the small
`top_assoc` file's `gene_id`/`gene_name` columns rather than a large external
reference.

The study ID format used throughout the pipeline is::

    BigBrain_{qtlType}_EUR_{feature}

For example::

    BigBrain_eqtl_EUR_ENSG00000177757.2
    BigBrain_sqtl_EUR_chr1_136708_136903
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.bigbrain import BigBrainPublicationMetadata

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Ensembl gene IDs in BigBrain carry a version suffix (e.g. "ENSG00000177757.2").
_ENSEMBL_VERSION_SUFFIX = r"\.\d+$"


class BigBrainStudyIndex:
    """Factory helpers for constructing the BigBrain eQTL/sQTL study index.

    This class is not instantiated directly. Its static methods build a
    feature-to-gene mapping (differently for eQTL vs sQTL) and combine it with
    the distinct features observed in the harmonised summary statistics to
    produce a `StudyIndex`.
    """

    @staticmethod
    def gene_map_from_feature(features: DataFrame) -> DataFrame:
        """Build a feature-to-gene mapping for eQTL, where `feature` is already the versioned Ensembl gene ID.

        Args:
            features (DataFrame): Single-column (`feature`) DataFrame of distinct
                features observed in the harmonised eQTL summary statistics.

        Returns:
            DataFrame: Columns `feature`, `geneId` (version-stripped), `traitFromSource`.
        """
        return features.select(
            f.col("feature"),
            f.regexp_replace(f.col("feature"), _ENSEMBL_VERSION_SUFFIX, "").alias(
                "geneId"
            ),
            f.col("feature").alias("traitFromSource"),
        )

    @staticmethod
    def gene_map_from_top_assoc(top_assoc: DataFrame) -> DataFrame:
        """Build a feature-to-gene mapping for sQTL from the `top_assoc` file's gene columns.

        The genome-wide `full_assoc` file carries no gene annotation for sQTL
        (`feature` is a Leafcutter intron-cluster coordinate string); the much
        smaller `top_assoc` file provides `gene_id`/`gene_name` per feature, which
        is used here as a lookup table.

        Args:
            top_assoc (DataFrame): Raw `top_assoc` DataFrame with `feature`, `gene_id`,
                `gene_name` columns.

        Returns:
            DataFrame: Columns `feature`, `geneId` (version-stripped), `traitFromSource`
                (gene symbol). A feature absent from `top_assoc` is simply missing from
                this mapping and will resolve to a null `geneId` in the study index.
        """
        return top_assoc.select(
            f.col("feature"),
            f.regexp_replace(f.col("gene_id"), _ENSEMBL_VERSION_SUFFIX, "").alias(
                "geneId"
            ),
            f.col("gene_name").alias("traitFromSource"),
        ).dropDuplicates(["feature"])

    @classmethod
    def from_source(
        cls,
        features: DataFrame,
        feature_gene_map: DataFrame,
        qtl_type: str,
    ) -> StudyIndex:
        """Build a BigBrain study index by joining distinct features against a feature-to-gene mapping.

        Args:
            features (DataFrame): Single-column (`feature`) DataFrame of distinct
                features observed in the harmonised summary statistics.
            feature_gene_map (DataFrame): Mapping produced by `gene_map_from_feature`
                (eQTL) or `gene_map_from_top_assoc` (sQTL).
            qtl_type (str): QTL type identifier, e.g. `BigBrainQtlType.EQTL.value` or
                `BigBrainQtlType.SQTL.value`.

        Returns:
            StudyIndex: One row per distinct feature, annotated with bibliographic
                and cohort metadata from `BigBrainPublicationMetadata`.
        """
        pub = BigBrainPublicationMetadata()

        joined = (
            features.join(feature_gene_map, on="feature", how="left")
            .withColumn(
                "studyId",
                f.concat_ws(
                    "_", f.lit("BigBrain"), f.lit(qtl_type), f.lit(pub.ANCESTRY), f.col("feature")
                ),
            )
            .withColumn(
                "projectId", f.lit(f"BigBrain-{qtl_type}-{pub.ANCESTRY}")
            )
            .withColumn("studyType", f.lit(qtl_type))
            .withColumn("biosampleFromSourceId", f.lit(pub.BIOSAMPLE_ID))
            .withColumn("pubmedId", f.lit(pub.PUBMED_ID))
            .withColumn("publicationFirstAuthor", f.lit(pub.PUB_FIRST_AUTHOR))
            .withColumn("publicationDate", f.lit(pub.PUB_DATE))
            .withColumn("publicationJournal", f.lit(pub.PUB_JOURNAL))
            .withColumn("publicationTitle", f.lit(pub.PUB_TITLE))
            .withColumn(
                "initialSampleSize",
                f.lit(f"{pub.SAMPLE_SIZE:,} donors ({pub.ANCESTRY})"),
            )
            .withColumn("nSamples", f.lit(pub.SAMPLE_SIZE))
            .withColumn(
                "discoverySamples",
                f.array(
                    f.struct(
                        f.lit(pub.SAMPLE_SIZE).alias("sampleSize"),
                        f.lit(pub.ANCESTRY).alias("ancestry"),
                    )
                ),
            )
            .withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
            .withColumn("cohorts", f.array(f.lit(pub.COHORTS)))
        )

        study_index_columns = [
            field.name
            for field in StudyIndex.get_schema().fields
            if field.name in joined.columns
        ]
        return StudyIndex(
            _df=joined.select(*study_index_columns), _schema=StudyIndex.get_schema()
        )
