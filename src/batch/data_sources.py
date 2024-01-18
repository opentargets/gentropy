"""Definitions of all data sources which can be ingested."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from batch_common import DataSourceBase
from spark_prep import SparkPrep


@dataclass
class EqtlCatalogue(DataSourceBase):
    """A dataclass for ingesting the eQTL Catalogue."""
    data_source_name: str = "eqtl_catalogue"
    study_index_path: str = "https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths_imported.tsv"

    def __post_init__(self) -> None:
        """Read the study index."""
        self.study_index = pd.read_table(self.study_index_path)

    def _get_number_of_tasks(self) -> int:
        """Calculate the number of ingestion tasks based on the study index.

        Returns:
            int: Total number of ingestion tasks.
        """
        return len(self.study_index)

    def ingest(self, task_index: int) -> None:
        """Ingest a single study from the data source.

        Args:
            task_index (int): The index of the current study being ingested across all studies in the study index.
        """
        # Read the study index and select one study.
        record = self.study_index.loc[task_index].to_dict()
        qtl_group = record["qtl_group"]
        # Process the study.
        worker = SparkPrep(
            number_of_cores = self.cpu_per_task,
            input_uri=record["ftp_path"],
            separator="\t",
            chromosome_column_name="chromosome",
            drop_columns=[],
            output_base_path=f"{self.gcp_output_sumstats}/{self.data_source_name}/qtl_group={qtl_group}",
        )
        worker.process()


all_data_source_classes = [EqtlCatalogue]
data_source_look_up = {c.data_source_name: c for c in all_data_source_classes}
