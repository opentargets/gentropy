"""Colocalisation dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import get_record_with_maximum_value
from gentropy.dataset.dataset import Dataset
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
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
        study_loci: StudyLocus | L2GGoldStandard,
        filter_by_colocalisation_method: str,
        filter_by_qtl: str | None,
    ) -> DataFrame:
        """Get maximum colocalisation probability for a (studyLocus, gene) window.

        Args:
            study_loci (StudyLocus | L2GGoldStandard): Dataset containing study loci to filter the colocalisation dataset on and the geneId linked to the region
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
            (f.col("rightStudyType") != "gwas"),
            f.col("colocalisationMethod") == filter_by_colocalisation_method,
        ]
        if filter_by_qtl:
            coloc_filtering_expr.append(f.col("rightStudyType") == filter_by_qtl)

        return get_record_with_maximum_value(
            # Filter coloc dataset based on method and qtl type
            self.filter(reduce(lambda a, b: a & b, coloc_filtering_expr))
            # Join with study loci to get geneId
            .df.join(study_loci.df.select("studyLocusId", "geneId"), "studyLocusId"),
            ["studyLocusId", "geneId"],
            method_colocalisation_metric,
        )
