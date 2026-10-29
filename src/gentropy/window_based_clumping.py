"""Step to run window based clumping on summary statistics datasts."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.annotation import annotate_locus
from gentropy.method.clumping.window import WindowClumping


def study_folders(files: list[str], study_ids: set[str]) -> list[str]:
    """The folders named after the given studies, among the folders of the files.

    A folder named after a study holds that study's summary statistics, as in the
    harmonised GWAS Catalog layout ``<path>/<studyId>/part-*.parquet``. A folder is kept
    when its name is one of ``study_ids``; the path up to that folder is returned once.

    Args:
        files (list[str]): Paths of the summary statistics files.
        study_ids (set[str]): Identifiers of the studies to keep.

    Returns:
        list[str]: The sorted, distinct paths of the folders named after the studies.

    Examples:
        >>> files = [
        ...     "gs://b/sumstats/GCST1/part-0.parquet",
        ...     "gs://b/sumstats/GCST1/part-1.parquet",
        ...     "gs://b/sumstats/GCST2/part-0.parquet",
        ...     "gs://b/sumstats/GCST3.parquet",
        ... ]
        >>> study_folders(files, {"GCST1", "GCST3"})
        ['gs://b/sumstats/GCST1']
    """
    folders = set()
    for file in files:
        parts = file.split("/")
        for depth, name in enumerate(parts[:-1]):
            if name in study_ids:
                folders.add("/".join(parts[: depth + 1]))
                break
    return sorted(folders)


class WindowBasedClumpingStep:
    """Apply window based clumping on summary statistics datasets."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_locus_output_path: str,
        distance: int = WindowBasedClumpingStepConfig().distance,
        gwas_significance: float = WindowBasedClumpingStepConfig().gwas_significance,
        collect_locus: bool = WindowBasedClumpingStepConfig().collect_locus,
        inclusion_list_path: str
        | None = WindowBasedClumpingStepConfig().inclusion_list_path,
        recursive_file_lookup: bool = WindowBasedClumpingStepConfig().recursive_file_lookup,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
            distance (int): Base pairs on each side of a lead, both the clumping window and the half-width of each locus. Optional.
            gwas_significance (float): GWAS significance threshold. Defaults to 1e-8.
            collect_locus (bool): Whether to collect the summary statistics inside each locus into `locus`.
                The statistics are read from the folders named after the studies with a lead
                (see `study_folders`), or from all files when no folder is named after one. Optional.
            inclusion_list_path (str | None): Path to the inclusion list (list of white-listed study identifier). Optional.
            recursive_file_lookup (bool): Whether to recursively look for summary statistics files in the input path. Defaults to `True`.
                Note that if an inclusion list is provided, this flag is set to `True` always.

        Check WindowBasedClumpingStepConfig object for default values.
        """
        # If inclusion list path is provided, only these studies will be read:
        if inclusion_list_path:
            study_ids_to_ingest = [
                f"{summary_statistics_input_path}/{row['studyId']}.parquet"
                for row in session.spark.read.parquet(inclusion_list_path).collect()
            ]
            # Force recursive file lookup if inclusion list is provided
            recursive_file_lookup = True
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [summary_statistics_input_path]

        ss = SummaryStatistics.from_parquet(
            session, study_ids_to_ingest, recursiveFileLookup=recursive_file_lookup
        )

        # Clumping:
        study_locus = WindowClumping(
            distance=distance, gwas_significance=gwas_significance
        ).clump(ss)
        # Optional locus collection:
        if collect_locus:
            # annotate_locus reads the loci twice, for the rows and for their bounds;
            # persist them so the summary statistics are clumped once.
            study_locus = study_locus.persist()
            lead_studies = {
                row.studyId
                for row in study_locus.df.select("studyId").distinct().collect()
            }
            # Only the studies with a lead have a locus to collect.
            folders = study_folders(ss.df.inputFiles(), lead_studies)
            if folders:
                ss = SummaryStatistics.from_parquet(
                    session, folders, recursiveFileLookup=True
                )
            study_locus = annotate_locus(study_locus, ss)

        study_locus.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(study_locus_output_path)
