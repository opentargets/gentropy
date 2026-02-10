"""Test manifest generation."""

from datetime import datetime
from pathlib import Path

from pyspark.sql import Row

from gentropy import Session
from gentropy.datasource.decode.manifest import deCODEManifest


class TestdeCODEManifest:
    """Test methods of deCODEManifest."""

    listing_path = "tests/gentropy/data_samples/aws_bucket_listing.txt"
    s3_config_path = "tests/gentropy/data_samples/example_s3_config.json"
    expected_rows = [
        Row(
            projectId="deCODE-proteomics-smp",
            studyId="deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",
            hasSumstats=True,
            summarystatsLocation="s3a://my_bucket/some_folder/Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001.txt.gz",
            size="927.2 MiB",
            accessionTimestamp=datetime(2022, 5, 29, 9, 27, 28),
        ),
        Row(
            projectId="deCODE-proteomics-raw",
            studyId="deCODE-proteomics-raw_Proteomics_PC0_10001_1_GENE2__SOME_PROTEIN_2_00000001",
            hasSumstats=True,
            summarystatsLocation="s3a://my_bucket/some_folder/Proteomics_PC0_10001_1_GENE2__SOME_PROTEIN_2_00000001.txt.gz",
            size="926.0 MiB",
            accessionTimestamp=datetime(2022, 5, 29, 9, 27, 35),
        ),
    ]

    def test_manifest_from_bucket_listing(self, session: Session) -> None:
        """Test building manifest from bucket listing."""
        manifest = deCODEManifest.from_bucket_listing(
            session, self.listing_path, self.s3_config_path
        )
        assert isinstance(manifest, deCODEManifest), "should return deCODEManifest"
        assert manifest.df.count() == 2, "should have 2 entries"

        assert manifest.df.collect() == self.expected_rows, (
            "should collect expected rows"
        )

    def test_from_parquet(self, session: Session, tmp_path: Path) -> None:
        """Test round-trip: write expected rows to parquet and reload via from_parquet."""
        manifest_path = (tmp_path / "manifest").as_posix()
        session.spark.createDataFrame(
            self.expected_rows, schema=deCODEManifest.get_schema()
        ).write.mode("overwrite").parquet(manifest_path)
        manifest = deCODEManifest.from_parquet(session, manifest_path)
        assert isinstance(manifest, deCODEManifest), "should return deCODEManifest"
        assert manifest.df.count() == 2, "should have 2 entries"
        assert manifest.df.collect() == self.expected_rows, (
            "should collect expected rows"
        )

    def test_get_summary_statistics_paths(self, session: Session) -> None:
        """get_summary_statistics_paths should return all summarystatsLocation values."""
        df = session.spark.createDataFrame(
            self.expected_rows, schema=deCODEManifest.get_schema()
        )
        manifest = deCODEManifest(_df=df)
        paths = manifest.get_summary_statistics_paths()
        expected_paths = [row.summarystatsLocation for row in self.expected_rows]
        assert sorted(paths) == sorted(expected_paths)
