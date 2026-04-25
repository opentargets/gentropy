"""Steps to ingest molecular complex data into a `MolecularComplex` Parquet dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy import Session
from gentropy.datasource.complex_portal import ComplexTab


class MolecularComplexIngestionStepConfig(BaseModel, frozen=True):
    """Config for MolecularComplexIngestionStep."""

    predicted_complex_tab_path: Annotated[
        str,
        Field(description="Path to the predicted protein-complex tab-separated file."),
    ]
    experimental_complex_tab_path: Annotated[
        str,
        Field(
            description="Path to the experimental protein-complex tab-separated file."
        ),
    ]
    output_path: Annotated[
        str,
        Field(
            description="Destination path for the merged MolecularComplex Parquet dataset."
        ),
    ]


class MolecularComplexIngestionStep:
    """Ingest predicted and experimental protein-complex data into a `MolecularComplex` Parquet dataset.

    The molecular complex dataset is derived from ComplexTAB files from the Complex Portal.
    """

    def __init__(
        self,
        session: Session,
        config: MolecularComplexIngestionStepConfig,
    ) -> None:
        """Initialise and execute the molecular complex ingestion step.

        Args:
            session (Session): Active Gentropy Spark session.
            config: Configuration for the step.
        """
        ComplexTab.from_complex_tab(
            session=session,
            experimental=config.experimental_complex_tab_path,
            predicted=config.predicted_complex_tab_path,
        ).coalesce(session.output_partitions).df.write.mode("overwrite").parquet(
            config.output_path
        )
