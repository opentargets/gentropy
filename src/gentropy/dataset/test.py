"""Study locus dataset."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.dataset import Dataset


@dataclass
class TestClass(Dataset):
    """Plain test class demonstrating the use of automatic  ID generation and instantiation without schema."""

    # This list defines the uniqueness of the dataset - these have to be mandatory fields:
    _unique_fields = ["a", "b", "c"]
    _id_column = "testId"

    def __post_init__(self: Dataset) -> None:
        """Class specific post-init, overwriting the parent method."""
        # Upon initialising, we assign schema - this is based on the class method, so it would work without instantiating the class:
        self._schema = self.get_schema()

        # Only calculating the identifier if it is not present in the dataframe already:
        if "testId" not in self._df.columns:
            self._df = self._df.withColumn(
                self._id_column, self._generate_identifier(self._unique_fields)
            )

        # Calling schema validation:
        return super().__post_init__()

    @classmethod
    def get_schema(cls: type[TestClass]) -> StructType:
        """Hardwired schema for the test class.

        Returns:
            StructType: Schema of the dataset.
        """
        return StructType(
            [
                StructField("testId", StringType(), False),
                StructField("a", StringType(), False),
                StructField("b", LongType(), False),
                StructField("c", DoubleType(), False),
                StructField("d", StringType(), True),
            ]
        )
