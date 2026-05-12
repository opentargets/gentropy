"""Step to generate biosample index dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.biosample_ontologies.utils import extract_ontology_from_json


class BiosampleIndexDefaults(BaseModel, frozen=True):
    """Defaults for BiosampleIndexStep.

    All fields are mandatory input/output paths - no defaults.
    """

    cell_ontology_input_path: Annotated[str, Field(description="Path to cell ontology input file.")]
    uberon_input_path: Annotated[str, Field(description="Path to Uberon ontology input file.")]
    efo_input_path: Annotated[str, Field(description="Path to EFO ontology input file.")]
    biosample_index_path: Annotated[str, Field(description="Output path for biosample index dataset.")]


class BiosampleIndexStep:
    """Biosample index step.

    This step generates a Biosample index dataset from the various ontology sources. Currently Cell Ontology and Uberon are supported.
    """

    def __init__(
        self,
        config: BiosampleIndexDefaults,
        session: Session,
    ) -> None:
        """Run Biosample index generation step.

        Args:
            config: Step configuration defaults.
            session: Active gentropy session.
        """
        cell_ontology_index = extract_ontology_from_json(
            config.cell_ontology_input_path, session.spark
        )
        uberon_index = extract_ontology_from_json(config.uberon_input_path, session.spark)
        efo_index = extract_ontology_from_json(
            config.efo_input_path, session.spark
        ).retain_rows_with_ancestor_id(["CL_0000000"])

        biosample_index = cell_ontology_index.merge_indices([uberon_index, efo_index])

        biosample_index.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(config.biosample_index_path)
