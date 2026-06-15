"""Step to generate colocalisation results."""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.method.colocalisation import ColocalisationMethod


class ColocalisationMethodParams(TypedDict):
    """Colocalisation method parameters."""

    priorc1: NotRequired[float]
    """Prior on variant being causal for trait 1. Defaults to 1e-4. For coloc, coloc_pip, coloc_pip_ecaviar method only."""
    priorc2: NotRequired[float]
    """Prior on variant being causal for trait 2. Defaults to 1e-4. For coloc, coloc_pip, coloc_pip_ecaviar method only."""
    priorc12: NotRequired[float]
    """Prior on variant being causal for both traits. Defaults to 1e-5. For coloc, coloc_pip, coloc_pip_ecaviar method only."""
    overlap_size_cutoff: NotRequired[int]
    """Minimum number of overlapping variants before filtering. Defaults to 0. For coloc method only."""
    posterior_cutoff: NotRequired[float]
    """Minimum overlapping Posterior probability cutoff for small overlaps. Defaults to 0.0. For coloc method only."""
    pseudocutoff: NotRequired[float]
    """Pseudocount to avoid log(0). Defaults to 1e-10. For coloc method only."""


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        coloc_path: str,
        colocalisation_method: Literal[
            "coloc", "ecaviar", "coloc_pip", "coloc_pip_ecaviar"
        ],
        restrict_right_studies: list[str] | None = None,
        gwas_v_qtl_overlap_only: bool = False,
        colocalisation_method_params: ColocalisationMethodParams | None = None,
    ) -> None:
        """Run Colocalisation step.

        This step allows for running two colocalisation methods: ecaviar and coloc. The default behaviour is all gwas vs all gwas plus all gwas vs all molecular-QTLs.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            coloc_path (str): Output path.
            colocalisation_method (Literal["coloc", "ecaviar", "coloc_pip", "coloc_pip_ecaviar"]): Colocalisation method. Use 'coloc_pip_ecaviar' to run both ColocPIP and eCAVIAR and merge results.
            restrict_right_studies (list[str] | None): List of study IDs to restrict the right side of the colocalisation overlaps to, e.g. all gwas vs a single studyId. Defaults to None.
            gwas_v_qtl_overlap_only (bool): If True, restricts the right side of colocalisation overlaps to only molecular-QTL studies, e.g. all gwas vs all molQTLs. Defaults to False.
            colocalisation_method_params (ColocalisationMethodParams | None): Keyword arguments passed to the colocalise method of Colocalisation class. Defaults to None

        Keyword Args:
            priorc1 (float): Prior on variant being causal for trait 1. Defaults to 1e-4. For coloc method only.
            priorc2 (float): Prior on variant being causal for trait 2. Defaults to 1e-4. For coloc method only.
            priorc12 (float): Prior on variant being causal for both traits. Defaults to 1e-5. For coloc method only.
            overlap_size_cutoff (int): Minimum number of overlapping variants before filtering. Defaults to 0.
            posterior_cutoff (float): Minimum overlapping Posterior probability cutoff for small overlaps. Defaults to 0.0.
            pseudocutoff (float): Pseudocount to avoid log(0). Defaults to 1e-10. For coloc method only.
        """
        cm = ColocalisationMethod.get_method_class(colocalisation_method)
        cs = StudyLocus.from_parquet(session, credible_set_path)

        if colocalisation_method.upper() == ColocalisationMethod.COLOC.name:
            cs = cs.filter(
                f.col("finemappingMethod").isin(FinemappingMethod.methods_with_lbf())
            )

        # No `.persist()` on the aligned overlaps here: the production
        # `coloc_pip_ecaviar` path consumes them once (a single fused groupBy), so
        # caching would be pure overhead. `find_overlaps` already persists the
        # genuinely-reused intermediate (`loci_to_overlap`).
        overlaps = cs.find_overlaps(
            restrict_right_studies=restrict_right_studies,
            gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
        )
        params = colocalisation_method_params or {}
        result = cm.colocalise(overlapping_signals=overlaps, **params)

        (
            result.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(coloc_path)
        )
