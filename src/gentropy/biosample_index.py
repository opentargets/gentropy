"""Step to generate biosample index dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.datasource.ontologies.utils import extract_ontology_from_json, merge_biosample_indices


class BiosampleIndexStep:
    """Biosample index step.

    This step generates a Biosample index dataset from the various ontology sources. Currently Cell Ontology and Uberon are supported.
    """

    def __init__(
        self,
        session: Session,
        cell_ontology_input_path: str,
        uberon_input_path: str,
        biosample_index_output_path: str,
    ) -> None:
        """Run Biosample index generation step.

        Args:
            session (Session): Session object.
            cell_ontology_input_path (str): Input cell ontology dataset path.
            uberon_input_path (str): Input uberon dataset path.
            biosample_index_output_path (str): Output gene index dataset path.
        """
        cell_ontology_index = extract_ontology_from_json(cell_ontology_input_path, session.spark)
        uberon_index = extract_ontology_from_json(uberon_input_path, session.spark)
        
        biosample_index = merge_biosample_indices([cell_ontology_index, uberon_index])
        
        biosample_index.df.write.mode(session.write_mode).parquet(biosample_index_output_path)
        
