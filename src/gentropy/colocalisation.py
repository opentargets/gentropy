"""Step to generate colocalisation results."""
from __future__ import annotations

from pyspark.sql.functions import col

from gentropy.common.session import Session
from gentropy.config import ColocalisationMethod
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.method.colocalisation import Coloc, ECaviar


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
        colocalisation_method: ColocalisationMethod,
    ) -> None:
        """Run Colocalisation step.

        Args:
            session (Session): Session object.
            credible_set_path (str): Input credible sets path.
            study_index_path (str): Input study index path.
            coloc_path (str): Output Colocalisation path.
            colocalisation_method (ColocalisationMethod): Colocalisation method. Available methods are: ECAVIAR, COLOC.
        """
        # Extract
        credible_set = (
            StudyLocus.from_parquet(
                session, credible_set_path, recursiveFileLookup=True
            ).filter(col("finemappingMethod") == "SuSie")
            if colocalisation_method.value == "coloc"
            else StudyLocus.from_parquet(
                session, credible_set_path, recursiveFileLookup=True
            )
        )
        si = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )

        # Transform
        colocalisation_class = self._get_colocalisation_class(
            colocalisation_method.value
        )
        overlaps = credible_set.filter_credible_set(
            CredibleInterval.IS95
        ).find_overlaps(si)
        colocalisation_results = colocalisation_class.colocalise(overlaps)  # type: ignore

        # Load
        colocalisation_results.df.write.mode(session.write_mode).parquet(
            f"{coloc_path}/{colocalisation_method.value}"
        )

    @classmethod
    def _get_colocalisation_class(cls: type[ColocalisationStep], method: str) -> type:
        """Get colocalisation class.

        Args:
            method (str): Colocalisation method.

        Returns:
            type: Colocalisation class.

        Examples:
            >>> ColocalisationStep._get_colocalisation_class("ecaviar")
            <class 'gentropy.method.colocalisation.ECaviar'>
        """
        method_to_class = {
            "coloc": Coloc,
            "ecaviar": ECaviar,
        }
        return method_to_class[method]
