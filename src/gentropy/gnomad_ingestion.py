"""Step to dump a filtered version of a LD matrix (block matrix) and GnomAD variants."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.common.types import LD_Population, VariantPopulation
from gentropy.config import GnomadVariantConfig, LDIndexConfig
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from gentropy.datasource.gnomad.variants import (
    GnomADVariantFrequencies,
    GnomADVariantRsIds,
)


class LDIndexStep:
    """LD index step.

    !!! warning "This step is resource intensive"

        Suggested params: high memory machine, 5TB of boot disk, no SSDs.

    """

    def __init__(
        self,
        session: Session,
        ld_index_out: str,
        min_r2: float = LDIndexConfig().min_r2,
        ld_matrix_template: str = LDIndexConfig().ld_matrix_template,
        ld_index_raw_template: str = LDIndexConfig().ld_index_raw_template,
        ld_populations: list[LD_Population | str] = LDIndexConfig().ld_populations,
        liftover_ht_path: str = LDIndexConfig().liftover_ht_path,
        grch37_to_grch38_chain_path: str = LDIndexConfig().grch37_to_grch38_chain_path,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            ld_index_out (str): Output LD index path. (required)
            min_r2 (float): Minimum r2 to consider when considering variants within a window.
            ld_matrix_template (str): Input path to the gnomAD ld file with placeholder for population
            ld_index_raw_template (str): Input path to the raw gnomAD LD indices file with placeholder for population string
            ld_populations (list[LD_Population | str]): Population names derived from the ld file paths
            liftover_ht_path (str): Path to the liftover ht file
            grch37_to_grch38_chain_path (str): Path to the chain file used to lift over the coordinates.

        Default values are provided in LDIndexConfig.
        """
        (
            GnomADLDMatrix(
                ld_matrix_template=ld_matrix_template,
                ld_index_raw_template=ld_index_raw_template,
                grch37_to_grch38_chain_path=grch37_to_grch38_chain_path,
                ld_populations=ld_populations,
                liftover_ht_path=liftover_ht_path,
            )
            .as_ld_index(min_r2)
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
