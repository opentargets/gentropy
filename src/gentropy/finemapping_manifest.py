"""Module for generating fine-mapping manifest."""

from pyspark.sql import functions as f

from gentropy import Session, StudyIndex
from gentropy.common.spark import order_array_of_structs_by_field
from gentropy.dataset.fine_mapping import FineMappingManifest, FineMappingPlanner
from gentropy.method.ld import LDAnnotator


class GWASCatalogFineMappingManifestGenerator:
    """Class for generating a fine-mapping manifest from a study index."""

    def __init__(
        self,
        session: Session,
        study_index_path: str,
        fine_mapping_planner_path: str,
        output_path: str,
        summary_statistics_glob: str | None = None,
    ) -> None:
        """Generate a fine-mapping manifest from a study index with GWAS Catalog studies.

        Args:
            session (Session): Session object.
            study_index_path (str): Path to the input study index.
            fine_mapping_planner_path (str): Path to the fine-mapping planner parquet file.
            output_path (str): Path to write the fine-mapping manifest to.
            summary_statistics_glob (str | None): Glob pattern for summary statistics files.
                If not provided, the manifest will include sumstat paths from StudyIndex.
        """
        self._session = session
        study_index = StudyIndex.from_parquet(session, study_index_path)
        planner = FineMappingPlanner.from_parquet(session, fine_mapping_planner_path)
        if summary_statistics_glob is not None:
            s_paths = session.list_hadoop_paths(summary_statistics_glob)
            study_index = self._update_study_index_with_sumstat_paths(
                study_index, s_paths
            )

        self.generate_manifest(study_index, planner).df.toPandas().to_csv(
            output_path, sep="\t", index=False
        )

    def generate_manifest(
        self,
        si: StudyIndex,
        planner: FineMappingPlanner,
    ) -> FineMappingManifest:
        """Generate a fine-mapping manifest from a study index and fine-mapping planner.

        Args:
            si (StudyIndex): StudyIndex object.
            planner (FineMappingPlanner): FineMappingPlanner object.

        Returns:
            FineMappingManifest: A FineMappingManifest object containing the generated manifest.
        """
        si_df = (
            si.df.filter(f.col("projectId") == f.lit("GCST"))
            .filter("hasSumstats")
            .filter(f.col("summarystatsLocation").isNotNull())
            .filter(f.col("traitFromSourceMappedIds").isNotNull())
            .filter(f.col("ldPopulationStructure").isNotNull())
            .select(
                "studyId",
                "ldPopulationStructure",
                "summarystatsLocation",
                "traitFromSourceMappedIds",
            )
            .withColumn(
                "ldPopulationStructure",
                order_array_of_structs_by_field(
                    "ldPopulationStructure", "relativeSampleSize"
                ).alias("ldPopulationStructure"),
            )
            .withColumn(
                "majorAncestry",
                LDAnnotator._get_major_population(f.col("ldPopulationStructure")),
            )
            .withColumn(
                "traitFromSourceMappedIds",
                f.concat_ws(
                    ",",
                    f.array_sort(f.array_distinct(f.col("traitFromSourceMappedIds"))),
                ),
            )
            .select(
                "studyId",
                "summarystatsLocation",
                "majorAncestry",
                "traitFromSourceMappedIds",
            )
        ).persist()
        self._session.logger.info(
            f"StudyIndex filtered to {si_df.count()} GWAS Catalog studies with summary statistics."
        )

        planner_df = (
            planner.df.select("runId", "studyId", "route")
            .filter(f.col("runId").isNotNull())
            .persist()
        )
        expected_output_row_count = planner_df.count()
        self._session.logger.info(
            f"FineMappingPlanner filtered to {expected_output_row_count} studies with valid runId."
        )

        result = planner_df.join(si_df, on="studyId", how="inner").persist()
        actual_output_row_count = result.count()
        self._session.logger.info(
            f"Joined FineMappingPlanner and StudyIndex to {actual_output_row_count} studies with valid runId and summary statistics."
        )
        if actual_output_row_count < expected_output_row_count:
            self._session.logger.warning(
                f"Some studies in the FineMappingPlanner do not have corresponding summary statistics in the StudyIndex. Expected {expected_output_row_count} rows, but got {actual_output_row_count} rows."
            )

        return FineMappingManifest(
            _df=result.select(
                "runId",
                "studyId",
                "route",
                "summarystatsLocation",
                "majorAncestry",
                "traitFromSourceMappedIds",
            )
        )

    @staticmethod
    def _update_study_index_with_sumstat_paths(
        si: StudyIndex, summary_statistics_paths: list[str]
    ) -> StudyIndex:
        """Update the StudyIndex with summary statistics paths based on a glob pattern.

        Args:
            si (StudyIndex): The original StudyIndex object.
            summary_statistics_paths (list[str]): List of summary statistics file paths.

        Returns:
            StudyIndex: Updated StudyIndex object with summary statistics paths.
        """
        df = (
            si.df.join(
                si.df.sparkSession.createDataFrame(
                    [(path,) for path in summary_statistics_paths], schema="path STRING"
                )
                .withColumn("studyId", f.regexp_extract("path", r".*(GCST\d+)", 1))
                .distinct(),
                on="studyId",
                how="left",
            )
            .withColumn(
                "summarystatsLocation",
                f.coalesce(f.col("path"), f.col("summarystatsLocation")),
            )
            .drop("path")
        )

        # Assert no study has multiple summary statistics.
        duplicate_studies = (
            df.groupBy("studyId")
            .count()
            .filter(f.col("count") > 1)
            .select("studyId")
            .collect()
        )
        if duplicate_studies:
            raise ValueError(
                f"Duplicate studies found in the StudyIndex after updating with summary statistics paths: {duplicate_studies}"
            )
        return StudyIndex(_df=df)
