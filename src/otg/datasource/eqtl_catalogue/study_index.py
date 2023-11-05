"""Study Index for Finngen data source."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class EqtlStudyIndex(StudyIndex):
    """Study index dataset from eQTL Catalogue."""

    @classmethod
    def from_source(
        cls: type[EqtlStudyIndex],
        eqtl_studies: DataFrame,
    ) -> EqtlStudyIndex:
        """Ingest study level metadata from eQTL Catalogue."""
        return EqtlStudyIndex(
            _df=eqtl_studies.select(
                # Constant values.
                f.lit("EQTL_CATALOGUE").alias("projectId"),
                f.lit("eqtl").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                # Sample information.
                f.lit(838).alias("nSamples"),
                f.lit("838 (281 females and 557 males)").alias("initialSampleSize"),
                f.array(
                    f.struct(
                        f.lit(715).cast("long").alias("sampleSize"),
                        f.lit("European American").alias("ancestry"),
                    ),
                    f.struct(
                        f.lit(103).cast("long").alias("sampleSize"),
                        f.lit("African American").alias("ancestry"),
                    ),
                    f.struct(
                        f.lit(12).cast("long").alias("sampleSize"),
                        f.lit("Asian American").alias("ancestry"),
                    ),
                    f.struct(
                        f.lit(16).cast("long").alias("sampleSize"),
                        f.lit("Hispanic or Latino").alias("ancestry"),
                    ),
                ).alias("discoverySamples"),
                # Publication information.
                f.lit("32913098").alias("pubmedId"),
                f.lit(
                    "The GTEx Consortium atlas of genetic regulatory effects across human tissues"
                ).alias("publicationTitle"),
                f.lit("GTEx Consortium").alias("publicationFirstAuthor"),
                f.lit("publicationDate").alias("2020-09-11"),
                f.lit("Science").alias("publicationJournal"),
                # Study ID, example: "GTEx_V8_Adipose_Subcutaneous".
                f.concat(f.col("study"), f.lit("_"), f.col("qtl_group")).alias(
                    "studyId"
                ),
                # Human readable tissue label, example: "Adipose - Subcutaneous".
                f.col("tissue_label").alias("traitFromSource"),
                # Ontology identifier for the tissue, for example: "UBERON:0001157".
                f.array(
                    f.regexp_replace(
                        f.regexp_replace(
                            f.col("tissue_ontology_id"),
                            "UBER_",
                            "UBERON_",
                        ),
                        "_",
                        ":",
                    )
                ).alias("traitFromSourceMappedIds"),
                # Summary statistics location.
                f.col("ftp_path").alias("summarystatsLocation"),
            ).withColumn(
                "ldPopulationStructure",
                cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=cls.get_schema(),
        )
