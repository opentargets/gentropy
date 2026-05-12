"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.config import EqtlCatalogueConfig
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


class EqtlCatalogueDefaults(BaseModel, frozen=True):
    """Defaults for EqtlCatalogueStep.

    All values are frozen - create a new instance to override.
    """

    mqtl_quantification_methods_blacklist: Annotated[
        list[str],
        Field(description="Molecular trait quantification methods blacklist."),
    ]
    eqtl_catalogue_paths_imported: Annotated[
        str, Field(description="Input eQTL Catalogue fine mapping results path.")
    ]
    eqtl_catalogue_study_index_out: Annotated[
        str, Field(description="Output eQTL Catalogue study index path.")
    ]
    eqtl_catalogue_credible_sets_out: Annotated[
        str, Field(description="Output eQTL Catalogue credible sets path.")
    ]
    eqtl_catalogue_metadata_path: Annotated[
        str,
        Field(
            default=EqtlCatalogueConfig().eqtl_catalogue_metadata_path,
            description="Path to the data_table hosted on the eQTL Catalogue github.",
        ),
    ]
    eqtl_lead_pvalue_threshold: Annotated[
        float,
        Field(
            default=EqtlCatalogueConfig().eqtl_lead_pvalue_threshold,
            description="Lead p-value threshold.",
        ),
    ]


class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step.

    From SuSIE fine mapping results (available at [their FTP](https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/susie/) ), we extract credible sets and study index datasets from gene expression QTL studies.
    """

    def __init__(
        self,
        config: EqtlCatalogueDefaults,
        session: Session,
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            config: Configuration for the step.
            session: Session object.
        """
        # Extract
        studies_metadata = EqtlCatalogueStudyIndex.read_studies_from_source(
            config.eqtl_catalogue_metadata_path,
            list(config.mqtl_quantification_methods_blacklist),
            session=session,
        )

        # Load raw data only for the studies we are interested in ingestion. This makes the proces much lighter.
        studies_to_ingest = EqtlCatalogueStudyIndex.get_studies_of_interest(
            studies_metadata
        )
        credible_sets_df = EqtlCatalogueFinemapping.read_credible_set_from_source(
            credible_set_path=[
                f"{config.eqtl_catalogue_paths_imported}/{qtd_id}.credible_sets.tsv"
                for qtd_id in studies_to_ingest
            ],
            session=session,
        )
        lbf_df = EqtlCatalogueFinemapping.read_lbf_from_source(
            lbf_path=[
                f"{config.eqtl_catalogue_paths_imported}/{qtd_id}.lbf_variable.txt"
                for qtd_id in studies_to_ingest
            ],
            session=session,
        )

        # Transform
        processed_susie_df = EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets_df, lbf_df, studies_metadata
        )

        (
            EqtlCatalogueStudyIndex.from_susie_results(processed_susie_df)
            # Writing the output:
            .coalesce(1)
            .df.write.mode(session.write_mode)
            .parquet(config.eqtl_catalogue_study_index_out)
        )

        (
            EqtlCatalogueFinemapping.from_susie_results(processed_susie_df)
            # Flagging sub-significnat loci:
            .validate_lead_pvalue(pvalue_cutoff=config.eqtl_lead_pvalue_threshold)
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(config.eqtl_catalogue_credible_sets_out)
        )
