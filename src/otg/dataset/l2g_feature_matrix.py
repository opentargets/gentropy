"""Feature matrix of study locus pairs annotated with their functional genomics features."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Type

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import _convert_from_long_to_wide
from otg.dataset.colocalisation import Colocalisation
from otg.dataset.dataset import Dataset
from otg.dataset.study_index import StudyIndex
from otg.dataset.study_locus import StudyLocus
from otg.dataset.v2g import V2G
from otg.method.l2g_utils.feature_factory import (
    ColocalisationFactory,
    StudyLocusFactory,
)

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class L2GFeatureMatrix(Dataset):
    """Dataset with features for Locus to Gene prediction."""

    _schema: StructType = parse_spark_schema(
        "l2g_feature.json"
    )  # TODO: define wide schema once I have all features

    @classmethod
    def from_parquet(
        cls: Type[L2GFeatureMatrix],
        etl: ETLSession,
        path: str,
    ) -> L2GFeatureMatrix:
        """Initialise L2GFeatureMatrix from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            L2GFeatureMatrix: Locus to gene feature matrix
        """
        return super().from_parquet(etl, path, cls._schema)

    @classmethod
    def generate_features(
        cls: Type[L2GFeatureMatrix],
        etl: ETLSession,
        study_locus_path: str,
        study_index_path: str,
        variant_gene_path: str,
        colocalisation_path: str,
    ) -> L2GFeatureMatrix:
        """Generate features from the OTG datasets."""
        # Load datasets
        study_locus = StudyLocus.from_parquet(etl, study_locus_path)
        studies = StudyIndex.from_parquet(etl, study_index_path)
        distances = V2G.from_parquet(etl, variant_gene_path)
        coloc = Colocalisation.from_parquet(etl, colocalisation_path)

        # Extract features
        coloc_features = ColocalisationFactory._get_coloc_features_df(
            study_locus, studies, coloc
        )
        distance_features = StudyLocusFactory._get_tss_distance_features(distances)

        print(coloc_features.df.printSchema())
        print(distance_features.df.printSchema())

        fm = reduce(
            lambda x, y: x.unionByName(y),
            [coloc_features._df, distance_features._df],
        )

        return cls(_df=_convert_from_long_to_wide(fm))

    def train_test_split(
        self: L2GFeatureMatrix, fraction: float
    ) -> tuple[L2GFeatureMatrix, L2GFeatureMatrix]:
        """Split the dataset into training and test sets.

        Args:
            fraction (float): Fraction of the dataset to use for training

        Returns:
            tuple[L2GFeatureMatrix, L2GFeatureMatrix]: Training and test datasets
        """
        train, test = self._df.randomSplit([fraction, 1 - fraction], seed=42)
        return (
            L2GFeatureMatrix(_df=train),
            L2GFeatureMatrix(_df=test),
        )

    def fill_na(self: L2GFeatureMatrix) -> type[NotImplementedError]:
        """Fill NA values."""
        return NotImplementedError


@classmethod
class L2G(Dataset):
    """Output L2G."""

    pass
