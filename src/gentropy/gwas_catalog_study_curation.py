"""Step to update GWAS Catalog study curation file based on newly released GWAS Catalog dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalogParser,
)
from gentropy.datasource.gwas_catalog.study_index_ot_curation import (
    StudyIndexGWASCatalogOTCuration,
)


class GWASCatalogStudyCurationDefaults(BaseModel, frozen=True):
    """Defaults for GWASCatalogStudyCurationStep.

    All values are frozen - create a new instance to override.
    """

    catalog_study_files: Annotated[
        list[str], Field(description="List of raw GWAS catalog studies file.")
    ]
    catalog_ancestry_files: Annotated[
        list[str],
        Field(description="List of raw ancestry annotations files from GWAS Catalog."),
    ]
    gwas_catalog_study_curation_out: Annotated[
        str, Field(description="Path for the updated curation table.")
    ]
    gwas_catalog_study_curation_file: Annotated[
        str | None,
        Field(
            default=None,
            description="Path to the original curation table. Optional.",
        ),
    ] = None


class GWASCatalogStudyCurationStep:
    """Annotate GWAS Catalog studies with additional curation and create a curation backlog."""

    def __init__(
        self,
        session: Session,
        config: GWASCatalogStudyCurationDefaults,
    ) -> None:
        """Run step to annotate and create backlog.

        Args:
            session: Session object.
            config: Configuration for the step.
        """
        catalog_studies = session.spark.read.csv(
            list(config.catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            list(config.catalog_ancestry_files), sep="\t", header=True
        )

        gwas_catalog_study_curation = None
        if config.gwas_catalog_study_curation_file:
            if config.gwas_catalog_study_curation_file.endswith(".csv"):
                gwas_catalog_study_curation = StudyIndexGWASCatalogOTCuration.from_csv(
                    session, config.gwas_catalog_study_curation_file
                )
            elif config.gwas_catalog_study_curation_file.startswith("http"):
                gwas_catalog_study_curation = StudyIndexGWASCatalogOTCuration.from_url(
                    session, config.gwas_catalog_study_curation_file
                )
            else:
                raise ValueError(
                    "Only CSV files or URLs are accepted as curation file."
                )

        # Process GWAS Catalog studies and get list of studies for curation:
        (
            StudyIndexGWASCatalogParser.from_source(catalog_studies, ancestry_lut)
            # Adding existing curation:
            .annotate_from_study_curation(gwas_catalog_study_curation)
            # Extract new studies for curation:
            .extract_studies_for_curation(gwas_catalog_study_curation)
            # Save table:
            .toPandas()
            .to_csv(config.gwas_catalog_study_curation_out, sep="\t", index=False)
        )
