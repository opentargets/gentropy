"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import OmegaConf

from otg.config import Config, register_configs

register_configs()


@hydra.main(version_base=None, config_path="config", config_name="config")
def run_step(cfg: Config) -> None:
    """OTG ETL CLI.

    Args:
        cfg (Config): hydra configuration object
    """
    print(OmegaConf.to_yaml(cfg))

    step = instantiate(
        cfg.step
    )  # cfg.step contains the whole YAML corresponding to the step
    step.run()


if __name__ == "__main__":
    run_step()
