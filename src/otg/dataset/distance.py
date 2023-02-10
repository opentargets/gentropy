"""Interval dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from otg.dataset.v2g import V2G

if TYPE_CHECKING:

    from otg.common.session import ETLSession


@dataclass
class Distance(V2G):
    """Distance dataset links genes to variants based on genome interaction studies."""

    @classmethod
    def from_parquet(cls: Type[Distance], etl: ETLSession, path: str) -> Distance:
        """Initialise Intervals from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            Distance: Distance dataset
        """
        return super().from_parquet(etl, path, cls.schema)
