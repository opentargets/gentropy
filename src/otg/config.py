"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING

from otg.data.dataset import DatasetFromFileConfig
from otg.data.variant_annotation import VariantAnnotationGnomadConfig
from otg.data.variant_index import VariantIndexCredsetConfig


@dataclass
class Config:
    """Configuration for OTG ETL."""

    etl: str = MISSING
    step: str = MISSING


def register_configs() -> None:
    """Register configs."""
    cs = ConfigStore.instance()
    cs.store(name="base_config", node=Config)
    cs.store(
        group="dataset",
        name="base_dataset_from_file",
        node=DatasetFromFileConfig,
    )
    cs.store(
        group="dataset",
        name="base_variant_annotation_gnomad",
        node=VariantAnnotationGnomadConfig,
    )
    cs.store(
        group="dataset",
        name="base_variant_index_credsets",
        node=VariantIndexCredsetConfig,
    )
