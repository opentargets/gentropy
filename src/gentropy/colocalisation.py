"""Step to generate colocalisation results."""

from __future__ import annotations

from functools import partial
from typing import Any

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.method.colocalisation import Coloc, ColocPIP, ECaviar


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        coloc_path: str,
        colocalisation_method: str,
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
        colocalisation_method_params: dict[str, Any] | None = None,
    ) -> None:
        """Run Colocalisation step.

        This step allows for running two colocalisation methods: ecaviar and coloc. The default behaviour is all gwas vs all gwas plus all gwas vs all molecular-QTLs.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            coloc_path (str): Output path.
            colocalisation_method (str): Colocalisation method. Use 'coloc_pip_ecaviar' to run both ColocPIP and eCAVIAR and merge results.
            restrict_right_studies (list[str] | None): List of study IDs to restrict the right side of the colocalisation overlaps to, e.g. all gwas vs a single studyId. Defaults to None.
            gwas_v_qtl_overlap_only (bool): If True, restricts the right side of colocalisation overlaps to only molecular-QTL studies, e.g. all gwas vs all molQTLs. Defaults to False.
            colocalisation_method_params (dict[str, Any] | None): Keyword arguments passed to the colocalise method of Colocalisation class. Defaults to None

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4. For coloc method only.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4. For coloc method only.
            priorc12 (float): Prior on variant being causal for both traits. Defaults to 1e-5. For coloc method only.
            overlap_size_cutoff (int): Minimum number of overlapping variants bfore filtering. Defaults to 0.
            posterior_cutoff (float): Minimum overlapping Posterior probability cutoff for small overlaps. Defaults to 0.0.
        """
        colocalisation_method = colocalisation_method.lower()

        # Extract
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recusiveFileLookup=True
        )

        match colocalisation_method:
            case "coloc":
                # Limit coloc to only SuSiE, as it relies on logBF values
                credible_set = credible_set.filter(
                    f.col("finemappingMethod").isin(
                        FinemappingMethod.SUSIE.value,
                        FinemappingMethod.SUSIE_INF.value,
                    )
                )
                # Call overlaps over the filtered credible sets.
                overlaps = credible_set.find_overlaps(
                    restrict_right_studies=restrict_right_studies,
                    gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
                )
                # Run coloc
                colocalisation_results = Coloc.colocalise(
                    overlaps, **colocalisation_method_params or {}
                )
            case "ecaviar":
                # Call overlaps on full credible sets
                overlaps = credible_set.find_overlaps(
                    restrict_right_studies=restrict_right_studies,
                    gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
                )
                # Run eCAVIAR
                colocalisation_results = ECaviar.colocalise(
                    overlaps, **colocalisation_method_params or {}
                )
            case "coloc_pip_ecaviar":
                # Call overlaps on full credible sets
                overlaps = credible_set.find_overlaps(
                    restrict_right_studies=restrict_right_studies,
                    gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
                )

                # Run ColocPIP
                coloc_pip_results = ColocPIP.colocalise(
                    overlaps, **colocalisation_method_params or {}
                )

                # Run eCAVIAR
                ecaviar_results = ECaviar.colocalise(overlaps)

                # Merge results: join on key columns and combine metrics
                colocalisation_results = ColocPIP.merge_ecaviar_results(
                    coloc_pip_results, ecaviar_results
                )
            case _:
                raise ValueError(
                    f"Colocalisation method {colocalisation_method} is not supported."
                )

        # Load
        colocalisation_results.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(coloc_path)
