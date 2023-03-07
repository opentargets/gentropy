"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class Config:
    """Configuration for OTG ETL.

    Two parameters are required:
    - etl: ETL session name
    - step: Step to be run. This is one of the step config classes defined in the config store file.
    """

    etl: str = MISSING
    step: str = MISSING


def register_configs() -> None:
    """Register step configs - each config class has all the parameters needed to run a step."""
    cs = ConfigStore.instance()
    cs.store(name="base_config", node=Config)
    # cs.store(
    #     group="dataset",
    #     name="base_dataset_from_file",
    #     node=DatasetFromFileConfig,
    # )
    # cs.store(
    #     group="dataset",
    #     name="base_variant_annotation_gnomad",
    #     node=VariantAnnotationGnomadConfig,
    # )
    # cs.store(
    #     group="dataset",
    #     name="base_variant_index_credsets",
    #     node=VariantIndexCredsetConfig,
    # )
    # cs.store(name="config", node=Config)
    cs.store(group="step", name="locus_to_gene", node=LocusToGeneConfig)


# Each of these classes is a config class for a specific step


@dataclass
class VariantAnnotationGnomadConfig:
    """Variant annotation from gnomad configuration."""

    path: str | None = None
    gnomad_file: str = MISSING
    chain_file: str = MISSING
    populations: list = MISSING


@dataclass
class VariantIndexCredsetConfig:
    """Variant index from credible sets configuration."""

    path: str | None = None
    variant_annotation_path: str = MISSING
    credible_sets_path: str = MISSING


@dataclass
class EtlConfig:
    """Local ETL session."""

    spark_uri: str
    app_name: str
    write_mode: str
    _target_: str


class LocusToGeneMode(Enum):
    """Locus to Gene step mode."""

    # TODO: configure them as groups

    TRAIN = "train"
    PREDICT = "predict"


@dataclass
class LocusToGeneConfig:
    """Config for Locus to Gene classifier."""

    run_mode: str = MISSING  # FIXME: define it as LocusToGeneMode
    study_locus_path: str = MISSING
    variant_gene_path: str = MISSING
    colocalisation_path: str = MISSING
    study_index_path: str = MISSING
    study_locus_overlap_path: str = MISSING
    gold_standard_curation_path: str = MISSING
    gene_interactions_path: str = MISSING
    hyperparameters: dict = MISSING
    l2g_model_path: Optional[str] = None
    etl: EtlConfig = MISSING
    id: str = "locus_to_gene"
    _target_: str = "otg.steps.locus_to_gene.LocusToGene"
