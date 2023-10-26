"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class Config:
    """Configuration for otg."""

    defaults: List[Dict[str, str]] = field(
        default_factory=lambda: [{"step": "???"}, {"session": "session_config"}]
    )

    step: Any = MISSING
    session: Any = MISSING


@dataclass
class SessionConfig:
    """ETL config."""

    _target_: str = "otg.common.session.Session"
    app_name: str = "otgenetics"
    spark_uri: str = "local[*]"
    write_mode: str = "overwrite"
    hail_home: Optional[str] = None


@dataclass
class VariantIndexStepConfig:
    """Variant index step requirements.

    Attributes:
        variant_annotation_path (str): Input variant annotation path.
        study_locus_path (str): Input study-locus path.
        variant_index_path (str): Output variant index path.
    """

    _target_: str = "otg.variant_index.VariantIndexStep"
    variant_annotation_path: str = MISSING
    study_locus_path: str = MISSING
    variant_index_path: str = MISSING


# Register all configs
def register_configs() -> None:
    """Register configs."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(name="session_config", group="session", node=SessionConfig)
