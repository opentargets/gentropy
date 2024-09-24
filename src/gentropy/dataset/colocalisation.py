"""Colocalisation dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import get_record_with_maximum_value
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.study_locus import StudyLocus

from functools import reduce


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

    def extract_maximum_coloc_probability_per_region_and_gene(
        self: Colocalisation,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        *,
        filter_by_colocalisation_method: str,
        filter_by_qtl: str | None = None,
    ) -> DataFrame:
        """Get maximum colocalisation probability for a (studyLocus, gene) window.

        Args:
            study_locus (StudyLocus): Dataset containing study loci to filter the colocalisation dataset on and the geneId linked to the region
            study_index (StudyIndex): Study index to use to get study metadata
            filter_by_colocalisation_method (str): optional filter to apply on the colocalisation dataset
            filter_by_qtl (str | None): optional filter to apply on the colocalisation dataset

        Returns:
            DataFrame: table with the maximum colocalisation scores for the provided study loci

        Raises:
            ValueError: if filter_by_qtl is not in the list of valid QTL types
            ValueError: if filter_by_colocalisation_method is not in the list of valid colocalisation methods
        """
        from gentropy.colocalisation import ColocalisationStep

        valid_qtls = list(EqtlCatalogueStudyIndex.method_to_study_type_mapping.values())
        if filter_by_qtl and filter_by_qtl not in valid_qtls:
            raise ValueError(f"There are no studies with QTL type {filter_by_qtl}")

        if filter_by_colocalisation_method not in [
            "ECaviar",
            "Coloc",
        ]:  # TODO: Write helper class to retrieve coloc method names
            raise ValueError(
                f"Colocalisation method {filter_by_colocalisation_method} is not supported."
            )

        method_colocalisation_metric = ColocalisationStep._get_colocalisation_class(
            filter_by_colocalisation_method
        ).METHOD_METRIC  # type: ignore

        coloc_filtering_expr = [
            f.col("rightGeneId").isNotNull(),
            f.lower("colocalisationMethod") == filter_by_colocalisation_method.lower(),
        ]
        if filter_by_qtl:
            coloc_filtering_expr.append(
                f.lower("rightStudyType") == filter_by_qtl.lower()
            )

        filtered_colocalisation = (
            # Bring rightStudyType and rightGeneId and filter by rows where the gene is null,
            # which is equivalent to filtering studyloci from gwas on the right side
            self.append_study_metadata(
                study_locus,
                study_index,
                metadata_cols=["studyType", "geneId"],
                colocalisation_side="right",
            )
            # it also filters based on method and qtl type
            .filter(reduce(lambda a, b: a & b, coloc_filtering_expr))
            # and filters colocalisation results to only include the subset of studylocus that contains gwas studylocusid
            .join(
                study_locus.df.selectExpr("studyLocusId as leftStudyLocusId"),
                "leftStudyLocusId",
            )
        )

        return get_record_with_maximum_value(
            filtered_colocalisation.withColumnRenamed(
                "leftStudyLocusId", "studyLocusId"
            ).withColumnRenamed("rightGeneId", "geneId"),
            ["studyLocusId", "geneId"],
            method_colocalisation_metric,
        )

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
        return (
            # Append that to the respective side of the colocalisation dataset
            study_loci_w_metadata.selectExpr(
                f"studyLocusId as {colocalisation_side}StudyLocusId",
                *[
                    f"{col} as {colocalisation_side}{col[0].upper() + col[1:]}"
                    for col in metadata_cols
                ],
            ).join(self.df, f"{colocalisation_side}StudyLocusId", "right")
        )
