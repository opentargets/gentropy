"""Step to generate colocalisation results."""

from __future__ import annotations

from functools import partial
from typing import Any

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.method.colocalisation import (
    Coloc,
    ColocalisationMethodInterface,
    ColocPIP,
    ECaviar,
)


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    __coloc_methods__ = {
        method.METHOD_NAME.lower(): method
        for method in ColocalisationMethodInterface.__subclasses__()
    }

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

        if colocalisation_method == "coloc_pip_ecaviar":
            # Transform - find overlaps once
            overlaps = credible_set.find_overlaps(
                restrict_right_studies=restrict_right_studies,
                gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
            )

            # Run ColocPIP

            coloc_pip = ColocPIP.colocalise
            if colocalisation_method_params:
                coloc_pip = partial(coloc_pip, **colocalisation_method_params)
            coloc_pip_results = coloc_pip(overlaps)

            # Run eCAVIAR

            ecaviar_results = ECaviar.colocalise(overlaps)

            # Merge results: join on key columns and combine metrics
            join_keys = [
                "leftStudyLocusId",
                "rightStudyLocusId",
                "chromosome",
                "rightStudyType",
            ]

            colocalisation_results = Colocalisation(
                _df=coloc_pip_results.df.alias("pip")
                .join(
                    ecaviar_results.df.alias("ecav").select(
                        *join_keys,
                        f.col("clpp").alias("clpp_ecaviar"),
                        f.col("numberColocalisingVariants").alias(
                            "numberColocalisingVariants_ecaviar"
                        ),
                    ),
                    on=join_keys,
                    how="inner",
                )
                .select(
                    f.col("pip.leftStudyLocusId"),
                    f.col("pip.rightStudyLocusId"),
                    f.col("pip.rightStudyType"),
                    f.col("pip.chromosome"),
                    # Use a combined method name
                    f.lit("COLOC_PIP_ECAVIAR").alias("colocalisationMethod"),
                    # Use the max number of colocalising variants from both methods
                    f.greatest(
                        f.col("pip.numberColocalisingVariants"),
                        f.col("numberColocalisingVariants_ecaviar"),
                    ).alias("numberColocalisingVariants"),
                    # Keep h3 and h4 from ColocPIP
                    f.col("pip.h3"),
                    f.col("pip.h4"),
                    # Add clpp from eCAVIAR
                    f.col("clpp_ecaviar").alias("clpp"),
                    # Keep beta ratio from ColocPIP
                    f.col("pip.betaRatioSignAverage"),
                ),
                _schema=Colocalisation.get_schema(),
            )
        else:
            colocalisation_class = self._get_colocalisation_class(colocalisation_method)

            if colocalisation_method == Coloc.METHOD_NAME.lower():
                credible_set = credible_set.filter(
                    f.col("finemappingMethod").isin(
                        FinemappingMethod.SUSIE.value, FinemappingMethod.SUSIE_INF.value
                    )
                )

            # Transform
            overlaps = credible_set.find_overlaps(
                restrict_right_studies=restrict_right_studies,
                gwas_v_qtl_overlap_only=gwas_v_qtl_overlap_only,
            )

            # Make a partial caller to ensure that colocalisation_method_params are added to the call only when dict is not empty
            coloc = colocalisation_class.colocalise
            if colocalisation_method_params:
                coloc = partial(coloc, **colocalisation_method_params)
            colocalisation_results = coloc(overlaps)

        # Load
        colocalisation_results.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(coloc_path)

    @classmethod
    def _get_colocalisation_class(
        cls, method: str
    ) -> type[ColocalisationMethodInterface]:
        """Get colocalisation class.

        Args:
            method (str): Colocalisation method.

        Returns:
            type[ColocalisationMethodInterface]: Class that implements the ColocalisationMethodInterface.

        Raises:
            ValueError: if method not available.

        Examples:
            >>> ColocalisationStep._get_colocalisation_class("ECaviar")
            <class 'gentropy.method.colocalisation.ECaviar'>
        """
        method = method.lower()
        if method not in cls.__coloc_methods__:
            raise ValueError(f"Colocalisation method {method} not available.")
        return cls.__coloc_methods__[method]
