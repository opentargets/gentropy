"""Shared facilities for running non-Spark ingestion on Google Batch."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from subprocess import run


@dataclass
class DataSourceBase:
    """A base dataclass to describe data source parameters for ingestion."""

    # GCP parameters.
    gcp_project = "open-targets-genetics-dev"
    gcp_region = "europe-west1"

    # GCP paths.
    gcp_staging_path = "gs://genetics_etl_python_playground/batch"
    gcp_output_sumstats = (
        "gs://genetics_etl_python_playground/1_smart_mirror/summary_stats"
    )

    # Data source parameters.
    data_source_name: str  # Data source name to use as the main identifier for Google Batch submission.
    max_parallelism: int = 50  # How many ingestion tasks to run concurrently.
    cpu_per_task: int = 4  # How many CPUs use per ingestion task.
    mem_per_task_gb: float = (
        4.0  # How many GB of RAM to allocate per CPU for each ingestion job.
    )

    def _get_number_of_tasks(self) -> int:
        """Return the total number of ingestion tasks for the data source.

        Returns:
            int: Total number of ingestion tasks.

        Raises:
            NotImplementedError: Always, because this method needs to be implemented by each specific data source class.
        """
        raise NotImplementedError(
            "The get_number_of_tasks() method must be implemented by data source classes."
        )

    def _generate_job_config(
        self,
        job_id: str,
        output_filename: str,
    ) -> None:
        """Generate configuration for a Google Batch job.

        Args:
            job_id (str): A unique job ID to identify a Google Batch job.
            output_filename (str): Output filename to store generated job config.
        """
        number_of_tasks = self._get_number_of_tasks()
        config = {
            "taskGroups": [
                {
                    "taskSpec": {
                        "runnables": [
                            {
                                "script": {
                                    "text": f"bash /mnt/share/code/runner.sh {job_id} {self.data_source_name}",
                                }
                            }
                        ],
                        "computeResource": {
                            "cpuMilli": self.cpu_per_task * 1000,
                            "memoryMib": int(
                                self.cpu_per_task * self.mem_per_task_gb * 1024
                            ),
                        },
                        "volumes": [
                            {
                                "gcs": {
                                    "remotePath": "genetics_etl_python_playground/batch"
                                },
                                "mountPath": "/mnt/share",
                            }
                        ],
                        "maxRetryCount": 1,
                        "maxRunDuration": "3600s",
                    },
                    "taskCount": number_of_tasks,
                    "parallelism": min(number_of_tasks, self.max_parallelism),
                }
            ],
            "allocationPolicy": {
                "instances": [
                    {
                        "policy": {
                            "machineType": f"n2d-standard-{self.cpu_per_task}",
                            "provisioningModel": "SPOT",
                        }
                    }
                ]
            },
            "logsPolicy": {"destination": "CLOUD_LOGGING"},
        }
        with open(output_filename, "w") as outfile:
            outfile.write(json.dumps(config, indent=4))

    def deploy_code_to_storage(self) -> None:
        """Deploy code to Google Storage."""
        run(
            [
                "gsutil",
                "-m",
                "cp",
                "-r",
                ".",
                f"{self.gcp_staging_path}/code",
            ], check=False
        )

    def submit(self) -> None:
        """Submit job for processing on Google Batch."""
        # Build job ID.
        current_utc_time = datetime.now(timezone.utc)
        formatted_time = current_utc_time.strftime("%Y%m%d-%H%M%S")
        batch_safe_name = self.data_source_name.replace("_", "-")
        job_id = f"{batch_safe_name}-{formatted_time}"

        # Generate job config.
        job_config_file = tempfile.NamedTemporaryFile(delete=False)
        self._generate_job_config(job_id, job_config_file.name)

        # Submit Google Batch job.
        run(
            [
                "gcloud",
                "batch",
                "jobs",
                "submit",
                job_id,
                f"--config={job_config_file.name}",
                f"--project={self.gcp_project}",
                f"--location={self.gcp_region}",
            ], check=False
        )
        os.remove(job_config_file.name)

    def ingest(self, task_index: int) -> None:
        """Ingest data for a single file from the data source.

        Args:
            task_index (int): The index of the current study being ingested across all studies in the study index.

        Raises:
            NotImplementedError: Always, because this method needs to be implemented by each specific data source class.
        """
        raise NotImplementedError(
            "The ingest() method must be implemented by data source classes."
        )
