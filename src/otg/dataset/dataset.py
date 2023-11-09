"""Dataset class for OTG."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import Self

from otg.common.schemas import flatten_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class Dataset(ABC):
    """Open Targets Genetics Dataset.

    `Dataset` is a wrapper around a Spark DataFrame with a predefined schema. Schemas for each child dataset are described in the `schemas` module.
    """

    _df: DataFrame
    _schema: StructType

    def __post_init__(self: Dataset) -> None:
        """Post init."""
        self.validate_schema()

    @property
    def df(self: Dataset) -> DataFrame:
        """Dataframe included in the Dataset.

        Returns:
            DataFrame: Dataframe included in the Dataset
        """
        return self._df

    @df.setter
    def df(self: Dataset, new_df: DataFrame) -> None:  # noqa: CCE001
        """Dataframe setter.

        Args:
            new_df (DataFrame): New dataframe to be included in the Dataset
        """
        self._df: DataFrame = new_df
        self.validate_schema()

    @property
    def schema(self: Dataset) -> StructType:
        """Dataframe expected schema.

        Returns:
            StructType: Dataframe expected schema
        """
        return self._schema

    @classmethod
    @abstractmethod
    def get_schema(cls: type[Self]) -> StructType:
        """Abstract method to get the schema. Must be implemented by child classes.

        Returns:
            StructType: Schema for the Dataset
        """
        pass

    @classmethod
    def from_parquet(
        cls: type[Self],
        session: Session,
        path: str,
        **kwargs: bool | float | int | str | None,
    ) -> Self:
        """Reads a parquet file into a Dataset with a given schema.

        Args:
            session (Session): Spark session
            path (str): Path to the parquet file
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.parquet

        Returns:
            Self: Dataset with the parquet file contents
        """
        schema = cls.get_schema()
        df = session.read_parquet(path=path, schema=schema, **kwargs)
        return cls(_df=df, _schema=schema)

    def validate_schema(self: Dataset) -> None:
        """Validate DataFrame schema against expected class schema.

        Raises:
            ValueError: DataFrame schema is not valid
        """
        expected_schema = self._schema
        expected_fields = flatten_schema(expected_schema)
        observed_schema = self._df.schema
        observed_fields = flatten_schema(observed_schema)

        # Unexpected fields in dataset
        if unexpected_field_names := [
            x.name
            for x in observed_fields
            if x.name not in [y.name for y in expected_fields]
        ]:
            raise ValueError(
                f"The {unexpected_field_names} fields are not included in DataFrame schema: {expected_fields}"
            )

        # Required fields not in dataset
        required_fields = [x.name for x in expected_schema if not x.nullable]
        if missing_required_fields := [
            req
            for req in required_fields
            if not any(field.name == req for field in observed_fields)
        ]:
            raise ValueError(
                f"The {missing_required_fields} fields are required but missing: {required_fields}"
            )

        # Fields with duplicated names
        if duplicated_fields := [
            x for x in set(observed_fields) if observed_fields.count(x) > 1
        ]:
            raise ValueError(
                f"The following fields are duplicated in DataFrame schema: {duplicated_fields}"
            )

        # Fields with different datatype
        observed_field_types = {
            field.name: type(field.dataType) for field in observed_fields
        }
        expected_field_types = {
            field.name: type(field.dataType) for field in expected_fields
        }
        if fields_with_different_observed_datatype := [
            name
            for name, observed_type in observed_field_types.items()
            if name in expected_field_types
            and observed_type != expected_field_types[name]
        ]:
            raise ValueError(
                f"The following fields present differences in their datatypes: {fields_with_different_observed_datatype}."
            )

    def persist(self: Self) -> Self:
        """Persist in memory the DataFrame included in the Dataset.

        Returns:
            Self: Persisted Dataset
        """
        self.df = self._df.persist()
        return self

    def unpersist(self: Self) -> Self:
        """Remove the persisted DataFrame from memory.

        Returns:
            Self: Unpersisted Dataset
        """
        self.df = self._df.unpersist()
        return self
