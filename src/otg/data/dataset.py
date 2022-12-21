"""Dataset class for OTG."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from omegaconf import MISSING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class DatasetFromFileConfig:
    """Read dataset from file configuration."""

    path: str = MISSING


@dataclass
class Dataset:
    """Dataset class for OTG."""

    df: DataFrame
    path: str | None
    schema: StructType

    @classmethod
    def from_parquet(cls: type[Dataset], etl: ETLSession, path: str) -> Dataset:
        """Reads a parquet file into a Dataset with a given schema.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            Dataset: Dataset with given schema
        """
        df = etl.read_parquet(path=path, schema=cls.schema)
        return cls(df=df, schema=cls.schema, path=path)

    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema based on JSON.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self.__class__.schema  # type: ignore[attr-defined]
        observed_schema = self.__class__.df.schema  # type: ignore[attr-defined]
        # Observed fields no    t in schema
        missing_struct_fields = [x for x in observed_schema if x not in expected_schema]
        error_message = f"The {missing_struct_fields} StructFields are not included in DataFrame schema: {expected_schema}"
        if missing_struct_fields:
            raise ValueError(error_message)

        # Required fields not in dataset
        required_fields = [x for x in expected_schema if not x.nullable]
        missing_required_fields = [
            x for x in required_fields if x not in observed_schema
        ]
        error_message = f"The {missing_required_fields} StructFields are required but missing from the DataFrame schema: {expected_schema}"
        if missing_required_fields:
            raise ValueError(error_message)
