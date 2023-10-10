"""Colocalisation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import get_record_with_maximum_value, pivot_df
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus


@dataclass
class Colocalisation(Dataset):
    """Colocalisation results for pairs of overlapping study-locus."""

    @classmethod
    def get_schema(cls: type[Colocalisation]) -> StructType:
        """Provides the schema for the Colocalisation dataset."""
        return parse_spark_schema("colocalisation.json")

    def get_max_llr_per_study_locus(
        self: Colocalisation, study_locus: StudyLocus, studies: StudyIndex
    ) -> DataFrame:
        """Get the maximum log likelihood ratio for each pair of overlapping study-locus by aggregating over all QTL datasets to be used in L2G.

        Args:
            study_locus (StudyLocus): Study locus dataset
            studies (StudyIndex): Study index dataset

        Returns:
            DataFrame: Maximum log likelihood ratio for each pair of study-loci
        """
        sentinel_study_locus = (
            study_locus.get_sentinels()
            .alias("sentinel_study_locus")
            # q: i need metadata about the sentinels, should i join with study_locus or bring everything from get_sentinels?
            .join(
                # bring coloc probability
                self.df.selectExpr("rightStudyLocusId as studyLocusId", "log2h4h3"),
                on="studyLocusId",
                how="inner",
            )
            .join(
                # bring study metadata
                studies.df.select("studyId", "studyType", "geneId"),
                on="studyId",
                how="left",
            )
        )
        local_max = get_record_with_maximum_value(
            sentinel_study_locus,
            ["studyType", "studyLocusId", "geneId"],
            "log2h4h3",
        ).transform(
            lambda df: pivot_df(df, "studyType", "log2h4h3", ["studyLocusId", "geneId"])
        )
        neighbourhood_max = (
            get_record_with_maximum_value(
                sentinel_study_locus,
                ["studyType", "studyLocusId", "geneId"],
                "log2h4h3",
            )
            .alias("neighbourhood_max")
            .transform(
                lambda df: (
                    pivot_df(df, "studyType", "log2h4h3", ["studyLocusId"]).select(
                        "studyType",
                        "studyLocusId",
                        "geneId",
                        *[
                            df[f"{col_name}"].alias(f"{col_name}_nbh")
                            for col_name in df.columns
                            if col_name not in ["studyType", "studyLocusId", "geneId"]
                        ],
                    )
                )
            )
        )

        return local_max.join(
            neighbourhood_max,
            on="studyLocusId",
            how="inner",
        )  # TODO: the neighbourhood max is not correct, i need to substract this prob from the local max

    def get_max_clpp(self: Colocalisation, study_locus: StudyLocus) -> DataFrame:
        """This function is exactly the same as get_max_llr, but it uses the clpp instead."""
        # TODO: refactor this to avoid code duplication
        pass
