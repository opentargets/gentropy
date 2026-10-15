"""BigBrain eQTL/sQTL study index module.

This module provides `BigBrainStudyIndex`, a set of static factory helpers that
build a `StudyIndex` from the distinct set of features in BigBrain's published
fine-mapping, together with a feature-to-gene mapping.

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

    from gentropy.dataset.target_index import TargetIndex

# Ensembl gene IDs in BigBrain carry a version suffix (e.g. "ENSG00000177757.2").
_ENSEMBL_VERSION_SUFFIX = r"\.\d+$"


class BigBrainStudyIndex:
    """Factory helpers for constructing the BigBrain eQTL/sQTL study index.

    This class is not instantiated directly. Its static methods build a
    feature-to-gene mapping (differently for eQTL vs sQTL) and combine it with
    the distinct features in the published fine-mapping to
    produce a `StudyIndex`.
    """

    @staticmethod
    def gene_map_from_feature(features: DataFrame) -> DataFrame:
        """Build a feature-to-gene mapping for eQTL, where `feature` is already the versioned Ensembl gene ID.

        Args:
            features (DataFrame): Single-column (`feature`) DataFrame of distinct
                features in the eQTL fine-mapping.

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
    def gene_map_from_top_assoc(
        top_assoc: DataFrame, target_index: TargetIndex
    ) -> DataFrame:
        """Build a feature-to-gene mapping for sQTL from the `top_assoc` file's gene columns.

        The genome-wide `full_assoc` file carries no gene annotation for sQTL
        (`feature` is a Leafcutter intron-cluster coordinate string); the much
        smaller `top_assoc` file provides `gene_id`/`gene_name` per feature, which
        is used here as a lookup table.

        A junction in overlapping genes is annotated with all of them, joined by `;`
        (about 10% of fine-mapped junctions). A study needs a single gene, so such a
        junction is resolved with `target_index` to the one gene whose body contains
        it, or failing that the one protein-coding gene that does. Otherwise `geneId`
        is left null and study validation excludes the study as an unresolved target.

        Args:
            top_assoc (DataFrame): Raw `top_assoc` DataFrame with `feature`, `gene_id`,
                `gene_name` columns, read with `NA` as null.
            target_index (TargetIndex): Gene coordinates and biotypes used to resolve
                multi-gene junctions.

        Returns:
            DataFrame: Columns `feature`, `geneId` (version-stripped), `traitFromSource`
                (gene symbol). A feature absent from `top_assoc` is simply missing from
                this mapping and will resolve to a null `geneId` in the study index.

        Examples:
            >>> from gentropy.dataset.target_index import TargetIndex
            >>> schema = TargetIndex.get_schema()
            >>> genes = [("ENSG01", 100, 300), ("ENSG02", 200, 400)]  # only ENSG01 contains 150-250
            >>> target_index = TargetIndex(_df=spark.createDataFrame([
            ...     {fld.name: None for fld in schema.fields}
            ...     | {"id": g, "biotype": "protein_coding",
            ...        "genomicLocation": {"chromosome": "1", "start": s, "end": e, "strand": 1}}
            ...     for g, s, e in genes
            ... ], schema=schema), _schema=schema)
            >>> top_assoc = spark.createDataFrame(
            ...     [("chr1_150_250", "ENSG01.3;ENSG02.1", "A;B"), ("chr1_900_950", "ENSG03.2", "C")],
            ...     "feature STRING, gene_id STRING, gene_name STRING",
            ... )
            >>> BigBrainStudyIndex.gene_map_from_top_assoc(top_assoc, target_index).orderBy("feature").show()
            +------------+------+---------------+
            |     feature|geneId|traitFromSource|
            +------------+------+---------------+
            |chr1_150_250|ENSG01|            A;B|
            |chr1_900_950|ENSG03|              C|
            +------------+------+---------------+
            <BLANKLINE>
        """
        features = top_assoc.select(
            "feature",
            f.col("gene_name").alias("traitFromSource"),
            f.transform(
                f.split(f.coalesce(f.col("gene_id"), f.lit("")), ";"),
                lambda g: f.regexp_replace(g, _ENSEMBL_VERSION_SUFFIX, ""),
            ).alias("genes"),
        ).dropDuplicates(["feature"])
        features = features.withColumn(
            "genes", f.filter(f.col("genes"), lambda g: g != "")
        )

        single = features.filter(f.size("genes") <= 1).select(
            "feature", f.col("genes")[0].alias("geneId"), "traitFromSource"
        )
        multi = features.filter(f.size("genes") > 1)
        coords = f.split(f.col("feature"), "_")
        containing = (
            multi.select(
                "feature",
                coords.getItem(1).cast("long").alias("junctionStart"),
                coords.getItem(2).cast("long").alias("junctionEnd"),
                f.explode("genes").alias("geneId"),
            )
            .join(
                target_index.df.select(
                    f.col("id").alias("geneId"),
                    f.col("biotype"),
                    f.col("genomicLocation.start").alias("geneStart"),
                    f.col("genomicLocation.end").alias("geneEnd"),
                ),
                on="geneId",
                how="inner",
            )
            .filter(
                (f.col("geneStart") <= f.col("junctionStart"))
                & (f.col("geneEnd") >= f.col("junctionEnd"))
            )
            .groupBy("feature")
            .agg(
                f.collect_set("geneId").alias("containing"),
                f.collect_set(
                    f.when(f.col("biotype") == "protein_coding", f.col("geneId"))
                ).alias("containingProteinCoding"),
            )
        )
        resolved = multi.join(containing, on="feature", how="left").select(
            "feature",
            f.when(f.size("containing") == 1, f.col("containing")[0])
            .when(
                f.size("containingProteinCoding") == 1,
                f.col("containingProteinCoding")[0],
            )
            .alias("geneId"),
            "traitFromSource",
        )
        return single.unionByName(resolved)

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
                features in the fine-mapping.
            feature_gene_map (DataFrame): Mapping produced by `gene_map_from_feature`
                (eQTL) or `gene_map_from_top_assoc` (sQTL).
            qtl_type (str): QTL type identifier, e.g. `StudyType.EQTL.value` or
                `StudyType.SQTL.value`.

        Returns:
            StudyIndex: One row per distinct feature, annotated with bibliographic
                and cohort metadata from `BigBrainPublicationMetadata`.

        Examples:
            >>> features = spark.createDataFrame([("ENSG00000177757.2",)], "feature STRING")
            >>> gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
            >>> si = BigBrainStudyIndex.from_source(features, gene_map, "eqtl")
            >>> si.df.select("studyId", "projectId", "studyType", "geneId", "nSamples").show(truncate=False)
            +-----------------------------------+---------+---------+---------------+--------+
            |studyId                            |projectId|studyType|geneId         |nSamples|
            +-----------------------------------+---------+---------+---------------+--------+
            |BigBrain_eqtl_EUR_ENSG00000177757.2|BigBrain |eqtl     |ENSG00000177757|10725   |
            +-----------------------------------+---------+---------+---------------+--------+
            <BLANKLINE>
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
                "projectId", f.lit("BigBrain")
            )
            .withColumn("studyType", f.lit(qtl_type))
            .withColumn("biosampleFromSourceId", f.lit(pub.BIOSAMPLE_ID))
            .withColumn("pubmedId", f.lit(pub.PUBMED_ID))
            .withColumn("publicationFirstAuthor", f.lit(pub.PUB_FIRST_AUTHOR))
            .withColumn("publicationDate", f.lit(pub.PUB_DATE))
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
                        f.lit(pub.ANCESTRY_LABEL).alias("ancestry"),
                    )
                ),
            )
            .withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
            .withColumn("cohorts", f.array(f.lit(pub.COHORTS)))
            .withColumn("summarystatsLocation", f.lit(pub.SUMSTATS_LOCATION[qtl_type]))
            # Summary statistics are published for every study (summarystatsLocation),
            # as for eQTL Catalogue; they are not ingested.
            .withColumn("hasSumstats", f.lit(True))
            .drop("feature")
        )

        return StudyIndex(_df=joined, _schema=StudyIndex.get_schema())
