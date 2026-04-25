"""Step to dump a filtered version of a LD matrix (block matrix) and GnomAD variants."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.common.types import VariantPopulation
from gentropy.config import GnomadVariantConfig
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE, VariantDirection
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from gentropy.datasource.gnomad.variants import (
    GnomADVariantFrequencies,
    GnomADVariantRsIds,
)


class LDIndexDefaults(BaseModel, frozen=True):
    """Defaults for LDIndexStep.

    All values are frozen - create a new instance to override.
    """

    ld_matrix_template: Annotated[
        str,
        Field(
            default="gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm",
            description="Input path to the gnomAD LD file with placeholder for population.",
        ),
    ]
    ld_index_raw_template: Annotated[
        str,
        Field(
            default="gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht",
            description="Input path to the raw gnomAD LD indices file with placeholder for population string.",
        ),
    ]
    ld_populations: Annotated[
        list[str],
        Field(
            default=["afr", "amr", "eas", "fin", "nfe"],
            description="Population names derived from the LD file paths.",
        ),
    ]
    liftover_ht_path: Annotated[
        str,
        Field(
            default="gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht",
            description="Path to the liftover HT file.",
        ),
    ]
    grch37_to_grch38_chain_path: Annotated[
        str,
        Field(
            default="gs://hail-common/references/grch37_to_grch38.over.chain.gz",
            description="Path to the chain file used to lift over the coordinates.",
        ),
    ]
    min_r2: Annotated[
        float,
        Field(
            default=0.5,
            description="Minimum r2 to consider when considering variants within a window.",
        ),
    ]


class LDIndexStep:
    """LD index step.

    !!! warning "This step is resource intensive"

        Suggested params: high memory machine, 5TB of boot disk, no SSDs.

    """

    def __init__(
        self,
        session: Session,
        config: LDIndexDefaults,
        ld_index_out: str,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
            ld_index_out (str): Output LD index path. (required)
        """
        (
            GnomADLDMatrix(
                ld_matrix_template=config.ld_matrix_template,
                ld_index_raw_template=config.ld_index_raw_template,
                grch37_to_grch38_chain_path=config.grch37_to_grch38_chain_path,
                ld_populations=config.ld_populations,
                liftover_ht_path=config.liftover_ht_path,
            )
            .as_ld_index(config.min_r2)
            .df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(ld_index_out)
        )
        session.logger.info(ld_index_out)


class GnomadVariantIndexStep:
    """A step to generate variant index dataset from gnomad data.

    Variant annotation step produces a dataset of the type `VariantIndex` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table.
    This dataset is used to validate variants and as a source of annotation.
    """

    def __init__(
        self,
        session: Session,
        variant_annotation_path: str = GnomadVariantConfig().variant_annotation_path,
        gnomad_genomes_path: str = GnomadVariantConfig().gnomad_genomes_path,
        gnomad_joint_path: str = GnomadVariantConfig().gnomad_joint_path,
        gnomad_variant_populations: list[
            VariantPopulation | str
        ] = GnomadVariantConfig().gnomad_variant_populations,
    ) -> None:
        """Run Variant Annotation step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Output path for the variant annotation dataset.
            gnomad_genomes_path (str): Path to the gnomAD genomes hail table.
            gnomad_joint_path (str): Path to the gnomAD joint hail table.
            gnomad_variant_populations (list[VariantPopulation | str]): List of populations to include in the annotation.

        All defaults are stored in the GnomadVariantConfig.
        """
        # amend data source version to output path
        session.logger.info("Gnomad variant annotation path:")
        session.logger.info(variant_annotation_path)

        gnomad_rsids = GnomADVariantRsIds(
            gnomad_genomes_path=gnomad_genomes_path,
        ).as_variant_index()

        gnomad_allele_frequencies = GnomADVariantFrequencies(
            gnomad_joint_path=gnomad_joint_path,
            gnomad_variant_populations=gnomad_variant_populations,
        ).as_variant_index()

        # Parse variant info from source.
        (
            gnomad_allele_frequencies.add_annotation(gnomad_rsids)
            .df.repartitionByRange("chromosome", "position")
            .sortWithinPartitions("chromosome", "position")
            .write.mode(session.write_mode)
            .parquet(variant_annotation_path)
        )


class GnomadVariantDirectionStep:
    """A step to generate variant direction dataset from gnomad variant index."""

    def __init__(
        self,
        session: Session,
        variant_index_path: str,
        variant_direction_path: str,
        window_size: int = DEFAULT_WINDOW_SIZE,
    ) -> None:
        """Run variant direction step.

        Args:
            session (Session): Session object.
            variant_index_path (str): Path to the variant index dataset.
            variant_direction_path (str): Output path for the variant direction dataset.
            window_size (int): Window size to consider when determining variant direction.

        """
        # amend data source version to output path
        session.logger.info("Gnomad variant direction path:")
        session.logger.info(variant_direction_path)

        # Parse variant info from source.
        (
            VariantDirection.from_variant_index(
                variant_index=VariantIndex.from_parquet(
                    session=session, path=variant_index_path
                ),
                window_size=window_size,
            )
            .df.write.mode(session.write_mode)
            .partitionBy("strand", "chromosome", "rangeId")
            .option("maxRecordsPerFile", 50_000_000)
            .parquet(variant_direction_path)
        )
