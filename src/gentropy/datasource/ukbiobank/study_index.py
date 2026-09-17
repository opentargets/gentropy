"""Study Index for ukbiobank data source."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class UKBiobankStudyIndex:
    """Study index dataset from UKBiobank.

    The following information is extracted:

    - studyId
    - pubmedId
    - publicationDate
    - publicationJournal
    - publicationTitle
    - publicationFirstAuthor
    - traitFromSource
    - ancestry_discoverySamples
    - ancestry_replicationSamples
    - initialSampleSize
    - nCases
    - replicationSamples

    Some fields are populated as constants, such as projectID, studyType, and initial sample size.
    """

    @classmethod
    def from_source(
        cls: type[UKBiobankStudyIndex],
        ukbiobank_studies: DataFrame,
    ) -> StudyIndex:
        """This function ingests study level metadata from UKBiobank.

        The University of Michigan SAIGE analysis (N=1281) utilized PheCode derived phenotypes and a novel method that ensures accurate P values, even with highly unbalanced case-control ratios (Zhou et al., 2018).

        The Neale lab Round 2 study (N=2139) used GWAS with imputed genotypes from HRC to analyze all data fields in UK Biobank, excluding ICD-10 related traits to reduce overlap with the SAIGE results.

        Args:
            ukbiobank_studies (DataFrame): UKBiobank study manifest file loaded in spark session.

        Returns:
            StudyIndex: Annotated UKBiobank study table.
        """
        return StudyIndex(
            _df=(
                ukbiobank_studies.select(
                    f.col("code").alias("studyId"),
                    f.lit("UKBiobank").alias("projectId"),
                    f.lit("gwas").alias("studyType"),
                    f.col("trait").alias("traitFromSource"),
                    # Make publication and ancestry schema columns.
                    f.when(f.col("code").startswith("SAIGE_"), "30104761").alias(
                        "pubmedId"
                    ),
                    f.when(
                        f.col("code").startswith("SAIGE_"),
                        "Efficiently controlling for case-control imbalance and sample relatedness in large-scale genetic association studies",
                    )
                    .otherwise(None)
                    .alias("publicationTitle"),
                    f.when(f.col("code").startswith("SAIGE_"), "Wei Zhou").alias(
                        "publicationFirstAuthor"
                    ),
                    f.when(f.col("code").startswith("NEALE2_"), "2018-08-01")
                    .otherwise("2018-10-24")
                    .alias("publicationDate"),
                    f.when(f.col("code").startswith("SAIGE_"), "Nature Genetics").alias(
                        "publicationJournal"
                    ),
                    f.col("n_total").cast("string").alias("initialSampleSize"),
                    f.col("n_cases").cast("integer").alias("nCases"),
                    f.array(
                        f.struct(
                            f.col("n_total").cast("integer").alias("sampleSize"),
                            f.concat(f.lit("European="), f.col("n_total")).alias(
                                "ancestry"
                            ),
                        )
                    ).alias("discoverySamples"),
                    f.col("in_path").alias("summarystatsLocation"),
                    f.lit(True).alias("hasSumstats"),
                )
                .withColumn(
                    "traitFromSource",
                    f.when(
                        f.col("traitFromSource").contains(":"),
                        f.concat(
                            f.initcap(
                                f.split(f.col("traitFromSource"), ": ").getItem(1)
                            ),
                            f.lit(" | "),
                            f.lower(f.split(f.col("traitFromSource"), ": ").getItem(0)),
                        ),
                    ).otherwise(f.col("traitFromSource")),
                )
                .withColumn(
                    "ldPopulationStructure",
                    StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
                )
            ),
            _schema=StudyIndex.get_schema(),
        )
