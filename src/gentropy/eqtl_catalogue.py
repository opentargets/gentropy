"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import EqtlCatalogueConfig
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step.

    From SuSIE fine mapping results (available at [their FTP](https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/susie/) ), we extract credible sets and study index datasets from gene expression QTL studies.
    """

    def __init__(
        self,
        session: Session,
        mqtl_quantification_methods_blacklist: list[str],
        eqtl_catalogue_paths_imported: str,
        eqtl_catalogue_study_index_out: str,
        eqtl_catalogue_credible_sets_out: str,
        eqtl_lead_pvalue_threshold: float = EqtlCatalogueConfig().eqtl_lead_pvalue_threshold,
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            session (Session): Session object.
            mqtl_quantification_methods_blacklist (list[str]): Molecular trait quantification methods that we don't want to ingest. Available options in https://github.com/eQTL-Catalogue/eQTL-Catalogue-resources/blob/master/data_tables/dataset_metadata.tsv
            eqtl_catalogue_paths_imported (str): Input eQTL Catalogue fine mapping results path.
            eqtl_catalogue_study_index_out (str): Output eQTL Catalogue study index path.
            eqtl_catalogue_credible_sets_out (str): Output eQTL Catalogue credible sets path.
            eqtl_lead_pvalue_threshold (float, optional): Lead p-value threshold. Defaults to EqtlCatalogueConfig().eqtl_lead_pvalue_threshold.
        """
        # Extract
        studies_metadata = EqtlCatalogueStudyIndex.read_studies_from_source(
            session, list(mqtl_quantification_methods_blacklist)
        )

        # Load raw data only for the studies we are interested in ingestion. This makes the proces much lighter.
        studies_to_ingest = EqtlCatalogueStudyIndex.get_studies_of_interest(
            studies_metadata
        )
        credible_sets_df = EqtlCatalogueFinemapping.read_credible_set_from_source(
            session,
            credible_set_path=[
                f"{eqtl_catalogue_paths_imported}/{qtd_id}.credible_sets.tsv"
                for qtd_id in studies_to_ingest
            ],
        )
        lbf_df = EqtlCatalogueFinemapping.read_lbf_from_source(
            session,
            lbf_path=[
                f"{eqtl_catalogue_paths_imported}/{qtd_id}.lbf_variable.txt"
                for qtd_id in studies_to_ingest
            ],
        )

        # Transform
        processed_susie_df = EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets_df, lbf_df, studies_metadata
        )

        (
            EqtlCatalogueStudyIndex.from_susie_results(processed_susie_df)
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(eqtl_catalogue_study_index_out)
        )

        (
            EqtlCatalogueFinemapping.from_susie_results(processed_susie_df)
            # Flagging sub-significnat loci:
            .validate_lead_pvalue(pvalue_cutoff=eqtl_lead_pvalue_threshold)
            # Writing the output:
            .df.write.mode(session.write_mode)
            .parquet(eqtl_catalogue_credible_sets_out)
        )
