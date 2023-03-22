"""Dataset class for OTG."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class Dataset:
    """Open Targets Genetics Dataset.

    `Dataset` is a wrapper around a Spark DataFrame with a predefined schema. Schemas for each child dataset are described in the `schemas` module.
    """

    _df: DataFrame
    _schema: StructType

    def __post_init__(self: Dataset) -> None:
        """Post init."""
        self.validate_schema()  # FIXME: This is causing issues with the schema validation

    @property
    def df(self: Dataset) -> DataFrame:
        """Dataframe included in the Dataset."""
        return self._df

    @df.setter
    def df(self: Dataset, new_df: DataFrame) -> None:  # noqa: CCE001
        """Dataframe setter."""
        self._df = new_df
        self.validate_schema()

    @property
    def schema(self: Dataset) -> StructType:
        """Dataframe expected schema."""
        return self._schema

    @classmethod
    def from_parquet(
        cls: type[Dataset], session: Session, path: str, schema: StructType
    ) -> Dataset:
        """Reads a parquet file into a Dataset with a given schema.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file
            schema (StructType): Schema to use

        Returns:
            Dataset: Dataset with given schema
        """
        df = session.read_parquet(path=path, schema=schema)
        return cls(_df=df, _schema=schema)

    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self._schema  # type: ignore[attr-defined]
        observed_schema = self._df.schema  # type: ignore[attr-defined]

        # Observed fields not in schema
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

    def persist(self: Dataset) -> Dataset:
        """Persist DataFrame included in the Dataset."""
        self._df = self.df.persist()
        return self
