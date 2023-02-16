"""Feature matrix of study locus pairs annotated with their functional genomics features."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class L2GFeature:
    """Property of a study locus pair."""

    study_id: str  # TODO: think about moving this to a trait id - so that we can extract the best study for that trait to train on
    locus_id: str
    gene_id: str
    feature_name: str
    feature_value: float


@dataclass
class L2GFeatureMatrix(Dataset):
    """Dataset with features for Locus to Gene prediction."""

    schema: StructType = parse_spark_schema("l2g_feature_matrix_schema.json")

    @classmethod
    def from_parquet(
        cls: type[L2GFeatureMatrix], etl: ETLSession, path: str
    ) -> Dataset:
        """Initialise L2GFeatureMatrix from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            Dataset: Locus to gene feature matrix
        """
        return super().from_parquet(etl, path), cls.schema

    def train_test_split(
        self: L2GFeatureMatrix, fraction: float
    ) -> tuple[L2GFeatureMatrix, L2GFeatureMatrix]:
        """Split the dataset into training and test sets.

        Args:
            fraction (float): Fraction of the dataset to use for training

        Returns:
            tuple[L2GFeatureMatrix, L2GFeatureMatrix]: Training and test datasets
        """
        train, test = (
            L2GFeatureMatrix(),
            L2GFeatureMatrix(),
        )  # TODO: Split L2GFeatureMatrix into 2 sets
        return (
            L2GFeatureMatrix(_df=train),
            L2GFeatureMatrix(_df=test),
        )

    def fill_na(self: L2GFeatureMatrix) -> type[NotImplementedError]:
        """Fill NA values."""
        return NotImplementedError

    def generate_features(self: L2GFeatureMatrix) -> type[NotImplementedError]:
        """Generate features."""
        return NotImplementedError


@classmethod
class L2G(Dataset):
    """Output L2G."""

    pass
