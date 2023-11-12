"""Step to run eQTL Catalogue study table ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.request import urlopen

from omegaconf import MISSING
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from otg.common.session import Session
from otg.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex
from otg.datasource.eqtl_catalogue.summary_stats import EqtlCatalogueSummaryStats


@dataclass
class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step.

    Attributes:
        session (Session): Session object.
        eqtl_catalogue_paths_imported (str): eQTL Catalogue input files for the harmonised and imported data.
        eqtl_catalogue_study_index_out (str): Output path for the eQTL Catalogue study index dataset.
        eqtl_catalogue_summary_stats_out (str): Output path for the eQTL Catalogue summary stats.
    """

    session: Session = MISSING

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_summary_stats_out: str = MISSING

    # Schema needs to be specified manually here, because otherwise Spark emits the following error:
    # "pyspark.sql.utils.AnalysisException: Unable to infer schema for CSV. It must be specified manually."
    _summary_stats_schema = StructType(
        [
            StructField("variant", StringType(), True),
            StructField("r2", StringType(), True),
            StructField("pvalue", DoubleType(), True),
            StructField("molecular_trait_object_id", StringType(), True),
            StructField("molecular_trait_id", StringType(), True),
            StructField("maf", DoubleType(), True),
            StructField("gene_id", StringType(), True),
            StructField("median_tpm", DoubleType(), True),
            StructField("beta", DoubleType(), True),
            StructField("se", DoubleType(), True),
            StructField("an", LongType(), True),
            StructField("ac", LongType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", LongType(), True),
            StructField("ref", StringType(), True),
            StructField("alt", StringType(), True),
            StructField("type", StringType(), True),
            StructField("rsid", StringType(), True),
        ]
    )

    def __post_init__(self: EqtlCatalogueStep) -> None:
        """Run step."""
        # Fetch study index.
        tsv_data = urlopen(self.eqtl_catalogue_paths_imported).read().decode("utf-8")
        rdd = self.session.spark.sparkContext.parallelize([tsv_data])
        df = self.session.spark.read.option("delimiter", "\t").csv(rdd, header=True)
        # Process partial study index.  At this point, it is not complete because we don't have the gene IDs, which we
        # will only get once the summary stats are ingested.
        study_index_df = EqtlCatalogueStudyIndex.from_source(df).df

        # Fetch summary stats.
        input_filenames = [row.summarystatsLocation for row in study_index_df.collect()]
        summary_stats_df = self.session.spark.read.option("delimiter", "\t").csv(
            input_filenames, header=True, schema=self._summary_stats_schema
        )
        # Process summary stats.
        summary_stats_df = EqtlCatalogueSummaryStats.from_source(summary_stats_df).df

        # Add geneId column to the study index.
        study_index_df = EqtlCatalogueStudyIndex.add_gene_id_column(
            study_index_df,
            summary_stats_df,
        ).df

        # Write study index.
        study_index_df.write.mode(self.session.write_mode).parquet(
            self.eqtl_catalogue_study_index_out
        )
        # Write summary stats.
        (
            summary_stats_df.sortWithinPartitions("position")
            .write.partitionBy("studyId")
            .mode(self.session.write_mode)
            .parquet(self.eqtl_catalogue_summary_stats_out)
        )
