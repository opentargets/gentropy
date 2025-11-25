"""Study Index for eQTL Catalogue data source."""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.eqtl_catalogue import QuantificationMethod, StudyType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


class EqtlCatalogueStudyIndex:
    """Study index dataset from eQTL Catalogue.

    We extract study level metadata from eQTL Catalogue's fine mapping results. All available studies can be found [here](https://www.ebi.ac.uk/eqtl/Studies/).

    One study from the eQTL Catalogue clusters together all the molecular QTLs (mQTLs) that were found:

        - in the same publication (e.g. Alasoo_2018)
        - in the same cell type or tissue (e.g. monocytes)
        - and for the same measured molecular trait (e.g. ENSG00000141510)

    """

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
            StructField("pmid", StringType(), True),
            StructField("study_type", StringType(), True),
        ]
    )
    method_to_qtl_type_mapping = {
        QuantificationMethod.GE.value: StudyType.EQTL.value,
        QuantificationMethod.EXON.value: StudyType.EQTL.value,
        QuantificationMethod.TX.value: StudyType.EQTL.value,
        QuantificationMethod.MICROARRAY.value: StudyType.EQTL.value,
        QuantificationMethod.LEAFCUTTER.value: StudyType.SQTL.value,
        QuantificationMethod.APTAMER.value: StudyType.PQTL.value,
        QuantificationMethod.TXREV.value: StudyType.TUQTL.value,
    }

    @classmethod
    def _identify_study_type(
        cls: type[EqtlCatalogueStudyIndex],
    ) -> Column:
        """Identify the qtl type based on the quantification method and eqtl catalogue study type.

        Returns:
            Column: The study type.

        Examples:
            >>> df = spark.createDataFrame([("ge", "bulk"), ("leafcutter", "bulk"), ("tx", "single-cell")], ["quant_method", "study_type"])
            >>> df.withColumn("studyType", EqtlCatalogueStudyIndex._identify_study_type()).show()
            +------------+-----------+---------+
            |quant_method| study_type|studyType|
            +------------+-----------+---------+
            |          ge|       bulk|     eqtl|
            |  leafcutter|       bulk|     sqtl|
            |          tx|single-cell|   sceqtl|
            +------------+-----------+---------+
            <BLANKLINE>
        """
        qtl_type_mapping = f.create_map(
            *[f.lit(x) for x in chain(*cls.method_to_qtl_type_mapping.items())]
        )[f.col("quant_method")]
        return f.when(
            f.col("study_type") == "single-cell",
            f.concat(f.lit("sc"), qtl_type_mapping),
        ).otherwise(qtl_type_mapping)

    @classmethod
    def get_studies_of_interest(
        cls: type[EqtlCatalogueStudyIndex],
        studies_metadata: DataFrame,
    ) -> list[str]:
        """Filter studies of interest from the raw studies metadata.

        Args:
            studies_metadata (DataFrame): raw studies metadata filtered with studies of interest.

        Returns:
            list[str]: QTD IDs defining the studies of interest for ingestion.
        """
        return (
            studies_metadata.select("dataset_id")
            .distinct()
            .toPandas()["dataset_id"]
            .tolist()
        )

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

    @classmethod
    def read_studies_from_source(
        cls: type[EqtlCatalogueStudyIndex],
        metadata_path: str,
        mqtl_quantification_methods_blacklist: list[str],
    ) -> DataFrame:
        """Read raw studies metadata from eQTL Catalogue.

        Args:
            metadata_path: Path to the studies metadata file.
            mqtl_quantification_methods_blacklist (list[str]): Molecular trait quantification methods that we don't want to ingest. Available options in https://github.com/eQTL-Catalogue/eQTL-Catalogue-resources/blob/master/data_tables/dataset_metadata.tsv

        Returns:
            DataFrame: raw studies metadata.

        Raises:
            ValueError: If an invalid quantification method is provided in the blacklist.

        Example metadata_path: "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/fe3c4b4ed911b3a184271a6aadcd8c8769a66aba/data_tables/dataset_metadata.tsv"
        """
        session = Session.find()
        for method in mqtl_quantification_methods_blacklist:
            if method not in cls.method_to_qtl_type_mapping:
                raise ValueError(
                    f"Quantification method '{method}' is not supported. "
                    + f"Available options are: {list(cls.method_to_qtl_type_mapping.keys())}"
                )
        return session.load_data(
            metadata_path, schema=cls.raw_studies_metadata_schema
        ).filter(~(f.col("quant_method").isin(mqtl_quantification_methods_blacklist)))
