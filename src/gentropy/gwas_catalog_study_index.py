"""Step to generate an GWAS Catalog study identifier inclusion and exclusion list."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.datasource.gwas_catalog.study_index import StudyIndexGWASCatalogParser
from gentropy.datasource.gwas_catalog.study_index_ot_curation import (
    StudyIndexGWASCatalogOTCuration,
)


class GWASCatalogStudyIndexGenerationStep:
    """GWAS Catalog study index generation.

    This step generates a study index from the GWAS Catalog studies and ancestry files. It can also add additional curation information and summary statistics QC information when available.

    ''' warning
    This step does not generate study index for gwas catalog top hits.

    This step provides several optional arguments to add additional information to the study index:

    - gwas_catalog_study_curation_file: csv file or URL containing the curation table. If provided it annotates the study index with the additional curation information performed by the Open Targets team.
    - sumstats_qc_path: Path to the summary statistics QC table. If provided it annotates the study index with the summary statistics QC information in the `sumstatQCValues` columns (e.g. `n_variants`, `n_variants_sig` etc.).
    """

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        study_index_path: str,
        gwas_catalog_study_curation_file: str | None = None,
        sumstats_qc_path: str | None = None,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session objecct.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            study_index_path (str): Output GWAS catalog studies path.
            gwas_catalog_study_curation_file (str | None): csv file or URL containing the curation table. Optional.
            sumstats_qc_path (str | None): Path to the summary statistics QC table. Optional.

        Raises:
            ValueError: If the curation file is provided but not a CSV file or URL.
        """
        # Core Study Index Generation:
        study_index = StudyIndexGWASCatalogParser.from_source(
            session.spark.read.csv(list(catalog_study_files), sep="\t", header=True),
            session.spark.read.csv(list(catalog_ancestry_files), sep="\t", header=True),
        )

        # Annotate with curation if provided:
        if gwas_catalog_study_curation_file:
            if gwas_catalog_study_curation_file.endswith(
                ".tsv"
            ) | gwas_catalog_study_curation_file.endswith(".tsv"):
                gwas_catalog_study_curation = StudyIndexGWASCatalogOTCuration.from_csv(
                    session, gwas_catalog_study_curation_file
                )
            elif gwas_catalog_study_curation_file.startswith("http"):
                gwas_catalog_study_curation = StudyIndexGWASCatalogOTCuration.from_url(
                    session, gwas_catalog_study_curation_file
                )
            else:
                raise ValueError(
                    "Only CSV/TSV files or URLs are accepted as curation file."
                )
            study_index = study_index.annotate_from_study_curation(
                gwas_catalog_study_curation
            )

        # Annotate with sumstats QC if provided:
        if sumstats_qc_path:
            sumstats_qc = SummaryStatisticsQC.from_parquet(
                session=session,
                path=sumstats_qc_path,
                recursiveFileLookup=True,
            )
            study_index_with_qc = study_index.annotate_sumstats_qc(sumstats_qc)
            # Write the study
            study_index_with_qc.df.coalesce(session.output_partitions).write.mode(
                session.write_mode
            ).parquet(study_index_path)
        else:
            study_index.df.coalesce(session.output_partitions).write.mode(
                session.write_mode
            ).parquet(study_index_path)
