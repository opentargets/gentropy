"""Step to build the variant index from GnomAD variants."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.common.types import VariantPopulation
from gentropy.config import GnomadVariantConfig
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE, VariantDirection
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gnomad.variants import (
    GnomADVariantFrequencies,
    GnomADVariantRsIds,
)


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
