"""L2G benchmark build step."""

from __future__ import annotations

from uuid import uuid4

from gentropy.common.session import Session
from gentropy.dataset.l2g_benchmark import L2GBenchmark
from gentropy.dataset.l2g_benchmark_build_report import L2GBenchmarkBuildReport


class L2GBenchmarkStep:
    """Build benchmark artifacts for L2G development."""

    def __init__(
        self,
        session: Session,
        *,
        gold_standard_curation_path: str,
        benchmark_path: str,
        build_report_path: str,
        benchmark_version: str,
        release_version: str,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Build and persist the initial benchmark artifact family.

        Args:
            session (Session): Session object containing the Spark session.
            gold_standard_curation_path (str): Path to the raw OTG curation input.
            benchmark_path (str): Output path for the main benchmark table.
            build_report_path (str): Output path for the benchmark build report.
            benchmark_version (str): Benchmark version identifier.
            release_version (str): Open Targets release identifier.
            pipeline_run_id (str | None): Optional build run identifier.
        """
        self.session = session
        self.pipeline_run_id = pipeline_run_id or uuid4().hex

        gold_standard_curation = session.load_data(gold_standard_curation_path, "json")

        benchmark = L2GBenchmark.from_otg_curation(
            gold_standard_curation=gold_standard_curation,
            benchmark_version=benchmark_version,
            release_version=release_version,
            pipeline_run_id=self.pipeline_run_id,
        )
        (
            benchmark.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(benchmark_path)
        )

        build_report = L2GBenchmarkBuildReport.from_benchmark(benchmark)
        (
            build_report.df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(build_report_path)
        )

        session.logger.info("L2G benchmark seed artifacts saved successfully.")
