"""Step to generate biosample index dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.open_targets.target import OpenTargetsTarget
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.datasource.cell_ontology.biosample_index import CellOntologyBiosampleIndex
from gentropy.datasource.uberon.biosample_index import UberonBiosampleIndex


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
        cell_ontology_index = CellOntologyBiosampleIndex.extract_celltypes_from_source(
            session, cell_ontology_input_path
        )
        uberon_index = UberonBiosampleIndex.extract_tissue_from_source(
            session, uberon_input_path
        )
        biosample_index = BiosampleIndex.merge([cell_ontology_index, uberon_index])
        biosample_index.write_parquet(biosample_index_output_path)
        
