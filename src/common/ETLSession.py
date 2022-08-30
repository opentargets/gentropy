from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from common import Log4j

if TYPE_CHECKING:
    from omegaconf import DictConfig


class ETLSession:
    def __init__(self: ETLSession, cfg: DictConfig) -> None:
        # create session and retrieve Spark logger object
        self.spark = (
            SparkSession.builder.master(cfg.environment.sparkUri)
            .appName(cfg.etl.name)
            .getOrCreate()
        )

        self.logger = Log4j.Log4j(self.spark)
