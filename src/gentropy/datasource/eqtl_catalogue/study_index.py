"""Study Index for eQTL Catalogue data source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gentropy.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


class EqtlCatalogueStudyIndex:
    """Study index dataset from eQTL Catalogue."""

    raw_studies_metadata_schema: StructType = StructType(
        [
            StructField("study_id", StringType(), True),
            StructField("dataset_id", StringType(), True),
            StructField("study_label", StringType(), True),
            StructField("sample_group", StringType(), True),
            StructField("tissue_id", StringType(), True),
            StructField("tissue_label", StringType(), True),
            StructField("condition_label", StringType(), True),
            StructField("sample_size", IntegerType(), True),
            StructField("quant_method", StringType(), True),
        ]
    )
    raw_studies_metadata_path = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/data_tables/dataset_metadata.tsv"

    @classmethod
    def _identify_study_type(
        cls: type[EqtlCatalogueStudyIndex], study_label_col: Column
    ) -> Column:
        """Identify the study type based on the study label.

        Args:
            study_label_col (Column): column with the study label that identifies the supporting publication.

        Returns:
            Column: The study type.
        """
        return f.when(
            study_label_col == "Sun_2018",
            f.lit("pqtl"),
        ).otherwise(f.lit("eqtl"))

    @classmethod
    def from_susie_results(
        cls: type[EqtlCatalogueStudyIndex],
        processed_finemapping_df: DataFrame,
    ) -> StudyIndex:
        """Ingest study level metadata from eQTL Catalogue.

        Args:
            processed_finemapping_df (DataFrame): processed fine mapping results with study metadata.

        Returns:
            StudyIndex: eQTL Catalogue study index dataset derived from the selected SuSIE results.
        """
        study_index_cols = [
            field.name
            for field in StudyIndex.get_schema().fields
            if field.name in processed_finemapping_df.columns
        ]
        return StudyIndex(
            _df=processed_finemapping_df.select(study_index_cols).distinct(),
            _schema=StudyIndex.get_schema(),
        )
