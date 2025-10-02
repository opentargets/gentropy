"""Finngen meta analysis study index data source module."""

from __future__ import annotations

import operator
from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, StudyIndex
from gentropy.datasource.finngen_meta import (
    EFOCuration,
    FinngenMetaManifest,
    MetaAnalysisDataSource,
)


class FinngenMetaStudyIndex:
    """FinnGen meta-analysis study index."""

    @classmethod
    def from_source(
        cls: type[FinngenMetaStudyIndex],
        session: Session,
        manifest: FinngenMetaManifest,
        efo_curation: EFOCuration,
    ) -> StudyIndex:
        """Create the FinnGen meta-analysis study index from the manifest."""
        # 1. Read the mapping

        ancestry_cols = [
            f.col(c)
            for c in manifest.df.columns
            if c.endswith("_n_cases") or c.endswith("_n_controls")
        ]

        manifest.df.select(
            f.lit("gwas").alias("studyType"),
            f.lit(manifest.meta.value).alias("projectId"),
            f.concat_ws(
                "_",
                f.lit(manifest.meta.value),
                f.col("fg_phenotype"),
            ).alias("studyId"),
            f.col("name").alias("traitFromSource"),
            f.lit(True).alias("hasSumstats"),
            f.col("path_bucket").alias("summarystatsLocation"),
            cls.n_samples(*ancestry_cols).cast(t.IntegerType()).alias("nSamples"),
            f.filter(
                f.array(
                    f.struct(
                        (f.col("fg_n_cases") + f.col("fg_n_controls"))
                        .cast(t.IntegerType())
                        .alias("sampleSize"),
                        f.lit("Finnish").alias("ancestry"),
                    ),
                    f.struct(
                        (
                            f.col("ukbb_n_cases")
                            + f.col("ukbb_n_controls")
                            + f.col("MVP_EUR_n_cases")
                            + f.col("MVP_EUR_n_controls")
                        )
                        .cast("integer")
                        .alias("sampleSize"),
                        f.lit("European").alias("ancestry"),
                    ),
                    f.struct(
                        (f.col("MVP_AFR_n_cases") + f.col("MVP_AFR_n_controls"))
                        .cast(t.IntegerType())
                        .alias("sampleSize"),
                        f.lit("African unspecified").alias("ancestry"),
                    ),
                    f.struct(
                        (f.col("MVP_AMR_n_cases") + f.col("MVP_AMR_n_controls"))
                        .cast(t.IntegerType())
                        .alias("sampleSize"),
                        f.lit("Admixed American").alias("ancestry"),
                    ),
                ),
                lambda x: x.sampleSize > 0.0,
            ).alias("discoverySamples"),
        )
        # Add population structure.
        study_index_df = study_index_df.withColumn(
            "ldPopulationStructure",
            cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
        )
        # Create study index.
        study_index = StudyIndex(_df=study_index_df)
        # Add EFO mappings.

        spark.sparkContext.addFile(efo_curation_mapping_url)
        efo_curation_mapping = spark.read.csv(
            "file://" + SparkFiles.get(efo_curation_mapping_url.split("/")[-1]),
            sep="\t",
            header=True,
        )
        study_index = FinnGenStudyIndex.join_efo_mapping(
            study_index,
            efo_curation_mapping,
            finngen_release="R12",
        )
        return study_index

    @staticmethod
    def n_samples(*cols: Column) -> Column:
        """Get the total number of samples from multiple columns.

        Args:
            *cols (Column): Columns to sum.

        Returns:
            Column: Column representing the total number of samples.
        """
        return reduce(operator.add, cols).cast(t.IntegerType()).alias("nSamples")
