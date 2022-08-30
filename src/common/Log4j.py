from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class Log4j:
    def __init__(self: Log4j, spark: SparkSession) -> None:
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j

        message_prefix = f"<{app_name}-{app_id}>"
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self: Log4j, message: str) -> None:
        """Log an error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self: Log4j, message: str) -> None:
        """Log a warning.

        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self: Log4j, message: str) -> None:
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
