"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class Config:
    """Configuration for OTG ETL.

    Two parameters are required:
    - step: Step to be run. This is one of the step config classes defined in the config store file.
    - session: Spark session configuration.
    """

    defaults: list[Dict[str, str]] = field(
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
    hail_home: str | None = None


# Register all configs
def register_configs() -> None:
    """Register step configs - each config class has all the parameters needed to run a step."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(name="session_config", group="session", node=SessionConfig)
