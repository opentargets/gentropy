"""Colocalisation dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class Colocalisation(Dataset):
    """Colocalisation results for pairs of overlapping study-locus."""

    @classmethod
    def get_schema(cls: type[Colocalisation]) -> StructType:
        """Provides the schema for the Colocalisation dataset.

        Returns:
            StructType: Schema for the Colocalisation dataset
        """
        return parse_spark_schema("colocalisation.json")

    def append_study_metadata(
        self: Colocalisation,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        *,
        metadata_cols: list[str],
        colocalisation_side: str = "right",
    ) -> DataFrame:
        """Appends metadata from the study to the requested side of the colocalisation dataset.

        Args:
            study_locus (StudyLocus): Dataset containing study loci that links the colocalisation dataset and the study index via the studyId
            study_index (StudyIndex): Dataset containing study index that contains the metadata
            metadata_cols (list[str]): List of study columns to append
            colocalisation_side (str): Which side of the colocalisation dataset to append metadata to. Must be either 'right' or 'left'

        Returns:
            DataFrame: Colocalisation dataset with appended metadata of the study from the requested side

        Raises:
            ValueError: if colocalisation_side is not 'right' or 'left'
        """
        metadata_cols = ["studyId", *metadata_cols]
        if colocalisation_side not in ["right", "left"]:
            raise ValueError(
                f"colocalisation_side must be either 'right' or 'left', got {colocalisation_side}"
            )

        study_loci_w_metadata = (
            study_locus.df.select("studyLocusId", "studyId")
            .join(
                f.broadcast(study_index.df.select("studyId", *metadata_cols)),
                "studyId",
            )
            .distinct()
        )
        coloc_df = (
            # drop `rightStudyType` in case it is requested
            self.df.drop("rightStudyType")
            if "studyType" in metadata_cols and colocalisation_side == "right"
            else self.df
        )
        return (
            # Append that to the respective side of the colocalisation dataset
            study_loci_w_metadata.selectExpr(
                f"studyLocusId as {colocalisation_side}StudyLocusId",
                *[
                    f"{col} as {colocalisation_side}{col[0].upper() + col[1:]}"
                    for col in metadata_cols
                ],
            ).join(coloc_df, f"{colocalisation_side}StudyLocusId", "right")
        )

    def drop_trans_effects(
        self: Colocalisation, study_locus: StudyLocus
    ) -> Colocalisation:
        """Filters the colocalisation dataset to only include cis effects from QTLs (right study locus).

        Args:
            study_locus (StudyLocus): Dataset containing study loci that has metadata about the type of credible set

        Returns:
            Colocalisation: Colocalisation dataset filtered to only include cis effects from QTLs (right study locus)
        """
        cis_study_loci = study_locus.filter(
            (~f.col("isTransQtl")) | (f.col("isTransQtl").isNull())
        ).df.select("studyLocusId")
        filtered_coloc = self.df.join(
            cis_study_loci,
            self.df.rightStudyLocusId == cis_study_loci.studyLocusId,
            "inner",
        ).drop("studyLocusId")
        return Colocalisation(
            _df=filtered_coloc,
            _schema=self.get_schema(),
        )
