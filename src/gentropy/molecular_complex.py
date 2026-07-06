"""Steps to ingest molecular complex data into a `MolecularComplex` Parquet dataset."""

from __future__ import annotations

from gentropy import Session
from gentropy.datasource.complex_portal import ComplexTab


class MolecularComplexIngestionStep:
    """Ingest predicted and experimental protein-complex data into a `MolecularComplex` Parquet dataset.

    The molecular complex dataset is derived from ComplexTAB files from the Complex Portal.
    """

    def __init__(
        self,
        session: Session,
        predicted_complex_tab_path: str,
        experimental_complex_tab_path: str,
        output_path: str,
    ) -> None:
        """Initialise and execute the molecular complex ingestion step.

        Args:
            session (Session): Active Gentropy Spark session.
            predicted_complex_tab_path (str): Path to the predicted protein-complex
                tab-separated file.
            experimental_complex_tab_path (str): Path to the experimental protein-complex
                tab-separated file.
            output_path (str): Destination path for the merged
                `MolecularComplex` Parquet dataset.
        """
        ComplexTab.from_complex_tab(
            session=session,
            experimental=experimental_complex_tab_path,
            predicted=predicted_complex_tab_path,
        ).coalesce(session.output_partitions).df.write.mode("overwrite").parquet(
            output_path
        )
