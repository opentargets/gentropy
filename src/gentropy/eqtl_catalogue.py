"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

import pandas as pd
import pyspark.sql.functions as f

from gentropy.common.session import Session
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
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            session (Session): Session object.
            mqtl_quantification_methods_blacklist (list[str]): Molecular trait quantification methods that we don't want to ingest. Available options in https://github.com/eQTL-Catalogue/eQTL-Catalogue-resources/blob/master/data_tables/dataset_metadata.tsv
            eqtl_catalogue_paths_imported (str): Input eQTL Catalogue fine mapping results path.
            eqtl_catalogue_study_index_out (str): Output eQTL Catalogue study index path.
            eqtl_catalogue_credible_sets_out (str): Output eQTL Catalogue credible sets path.
        """
        # Extract
        pd.DataFrame.iteritems = pd.DataFrame.items
        studies_metadata = session.spark.createDataFrame(
            pd.read_csv(EqtlCatalogueStudyIndex.raw_studies_metadata_path, sep="\t"),
            schema=EqtlCatalogueStudyIndex.raw_studies_metadata_schema,
        ).filter(
            ~(f.col("quant_method").isin(list(mqtl_quantification_methods_blacklist)))
        )

        # Load raw data only for the studies we are interested in ingestion. This makes the proces much lighter.
        studies_to_ingest = EqtlCatalogueStudyIndex.get_studies_of_interest(
            studies_metadata
        )
        credible_sets_df = session.spark.read.csv(
            [
                f"{eqtl_catalogue_paths_imported}/{qtd_id}.credible_sets.tsv"
                for qtd_id in studies_to_ingest
            ],
            sep="\t",
            header=True,
            schema=EqtlCatalogueFinemapping.raw_credible_set_schema,
        )
        lbf_df = session.spark.read.csv(
            [
                f"{eqtl_catalogue_paths_imported}/{qtd_id}.lbf_variable.txt"
                for qtd_id in studies_to_ingest
            ],
            sep="\t",
            header=True,
            schema=EqtlCatalogueFinemapping.raw_lbf_schema,
        )

        # Transform
        processed_susie_df = EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets_df, lbf_df, studies_metadata
        )
        credible_sets = EqtlCatalogueFinemapping.from_susie_results(processed_susie_df)
        study_index = EqtlCatalogueStudyIndex.from_susie_results(processed_susie_df)

        # Load
        study_index.df.write.mode(session.write_mode).parquet(
            eqtl_catalogue_study_index_out
        )
        credible_sets.df.write.mode(session.write_mode).parquet(
            eqtl_catalogue_credible_sets_out
        )
