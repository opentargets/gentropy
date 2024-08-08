"""Step to generate colocalisation results."""

from __future__ import annotations

import inspect
from importlib import import_module

from pyspark.sql.functions import col

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.method.colocalisation import Coloc


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        session: Session,
        credible_set_path: str,
        study_index_path: str,
        coloc_path: str,
        colocalisation_method: str,
    ) -> None:
        """Run Colocalisation step.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            study_index_path (str): Input study index path.
            coloc_path (str): Output Colocalisation path.
            colocalisation_method (str): Colocalisation method.
        """
        colocalisation_class = self._get_colocalisation_class(colocalisation_method)
        # Extract
        credible_set = (
            StudyLocus.from_parquet(
                session, credible_set_path, recursiveFileLookup=True
            ).filter(col("finemappingMethod").isin("SuSie", "SuSiE-inf"))
            if colocalisation_class is Coloc
            else StudyLocus.from_parquet(
                session, credible_set_path, recursiveFileLookup=True
            )
        )
        si = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )

        # Transform
        overlaps = credible_set.filter_credible_set(
            CredibleInterval.IS95
        ).find_overlaps(si)
        colocalisation_results = colocalisation_class.colocalise(overlaps)  # type: ignore

        # Load
        colocalisation_results.df.write.mode(session.write_mode).parquet(
            f"{coloc_path}/{colocalisation_method.lower()}"
        )

    @classmethod
    def _get_colocalisation_class(cls: type[ColocalisationStep], method: str) -> type:
        """Get colocalisation class.

        Args:
            method (str): Colocalisation method.

        Returns:
            type: Colocalisation class.

        Raises:
            ValueError: if method not available.

        Examples:
            >>> ColocalisationStep._get_colocalisation_class("ECaviar")
            <class 'gentropy.method.colocalisation.ECaviar'>
        """
        module_name = "gentropy.method.colocalisation"
        module = import_module(module_name)

        available_methods = []
        for class_name, class_obj in inspect.getmembers(module, inspect.isclass):
            if class_obj.__module__ == module_name:
                available_methods.append(class_name)
                if class_name == method:
                    return class_obj
        raise ValueError(
            f"Method {method} is not supported. Available: {(', ').join(available_methods)}"
        )
