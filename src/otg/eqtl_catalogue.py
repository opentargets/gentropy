"""Step to run eQTL Catalogue study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

import pyspark.sql.functions as f
from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.eqtl_catalogue.study_index import EqtlStudyIndex
from otg.datasource.eqtl_catalogue.summary_stats import EqtlSummaryStats


@dataclass
class EqtlStep:
    """eQTL Catalogue ingestion step.

    Attributes:
        session (Session): Session object.
        eqtl_catalogue_paths_imported (str): eQTL Catalogue input files for the harmonised and imported data.
        eqtl_catalogue_study_index_out (str): Output path for the eQTL Catalogue study index dataset.
        eqtl_catalogue_summary_stats_out (str): Output path for the eQTL Catalogue summary stats.
    """

    session: Session = Session()

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_summary_stats_out: str = MISSING

    def __post_init__(self: EqtlStep) -> None:
        """Run step."""
        # Fetch study index.
        tsv_data = urlopen(self.eqtl_catalogue_paths_imported).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([tsv_data])
        df = self.session.spark.read.option("delimiter", "\t").csv(rdd)
        # Process partial study index.  At this point, it is not complete because we don't have the gene IDs, which we
        # will only get once the summary stats are ingested.
        study_index_df = EqtlStudyIndex.from_source(df).df

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index_df.collect()]
        summary_stats_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True
        )
        # Process summary stats.
        summary_stats_df = EqtlSummaryStats.from_source(summary_stats_df).df

        # Explode study index based on the list of genes. While the original list contains one entry per tissue, what we
        # consider as a single study is one mini-GWAS for an expression of a _particular gene_ in a particular study.
        # At this stage we have a study index with partial study IDs like "PROJECT_QTLGROUP", and a summary statistics
        # object with full study IDs like "PROJECT_QTLGROUP_GENEID", so we need to perform a merge and explosion to
        # obtain our final study index.
        partial_to_full_study_id = (
            summary_stats_df.select(f.col("studyId"))
            .distinct()
            .select(
                f.col("studyId").alias("fullStudyId"),  # PROJECT_QTLGROUP_GENEID
                f.regexp_extract(f.col("studyId"), r"(.*)_[\_]+", 1).alias(
                    "studyId"
                ),  # PROJECT_QTLGROUP
            )
            .groupBy("studyId")
            .agg(f.collect_list("fullStudyId").alias("fullStudyIdList"))
        )
        study_index_df = (
            study_index_df.join(partial_to_full_study_id, "studyId", "inner")
            .withColumn(f.explode("fullStudyIdList").alias("fullStudyId"))
            .drop("fullStudyIdList")
            .withColumn("geneId", f.regexp_extract(f.col("studyId"), r".*_([\_]+)", 1))
            .drop("fullStudyId")
        )

        # Write study index.
        study_index_df.write.mode(self.session.write_mode).parquet(
            self.eqtl_catalogue_study_index_out
        )
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("studyId", "chromosome")
            .mode(self.session.write_mode)
            .parquet(self.eqtl_catalogue_summary_stats_out)
        )
