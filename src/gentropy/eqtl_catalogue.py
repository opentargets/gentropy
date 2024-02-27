"""Step to run eQTL Catalogue credible set and study index ingestion."""

from __future__ import annotations

import pandas as pd

from gentropy.common.session import Session
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


class EqtlCatalogueStep:
    """eQTL Catalogue ingestion step."""

    def __init__(
        self,
        session: Session,
        eqtl_catalogue_paths_imported: str,
        eqtl_catalogue_study_index_out: str,
        eqtl_catalogue_credible_sets_out: str,
    ) -> None:
        """Run eQTL Catalogue ingestion step.

        Args:
            session (Session): Session object.
            eqtl_catalogue_paths_imported (str): Input eQTL Catalogue fine mapping results path.
            eqtl_catalogue_study_index_out (str): Output eQTL Catalogue study index path.
            eqtl_catalogue_credible_sets_out (str): Output eQTL Catalogue credible sets path.
        """
        # Extract
        pd.DataFrame.iteritems = pd.DataFrame.items
        credible_sets = session.spark.read.csv(
            f"{eqtl_catalogue_paths_imported}.credible_sets.tsv",
            sep="\t",
            header=True,
            schema=EqtlCatalogueFinemapping.raw_credible_set_schema,
        )
        lbf = session.spark.read.csv(
            f"{eqtl_catalogue_paths_imported}.lbf_variable.txt",
            sep="\t",
            header=True,
            schema=EqtlCatalogueFinemapping.raw_lbf_schema,
        )
        studies_metadata = session.spark.createDataFrame(
            pd.read_csv(EqtlCatalogueStudyIndex.raw_studies_metadata_path, sep="\t"),
            schema=EqtlCatalogueStudyIndex.raw_studies_metadata_schema,
        )

        # Transform
        processed_susie_df = EqtlCatalogueFinemapping.parse_susie_results(
            credible_sets, lbf, studies_metadata
        ).persist()
        credible_sets = EqtlCatalogueFinemapping.from_susie_results(processed_susie_df)
        study_index = EqtlCatalogueStudyIndex.from_source(processed_susie_df)

        # Load
        study_index.write.mode(session.write_mode).parquet(
            eqtl_catalogue_study_index_out
        )
        credible_sets.write.mode(session.write_mode).parquet(
            eqtl_catalogue_credible_sets_out
        )
