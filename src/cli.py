"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import OmegaConf

from otg.config import Config, register_configs

register_configs()


@hydra.main(version_base=None, config_name="config")
def run_step(cfg: Config) -> None:
    """OTG ETL CLI.

    Args:
        cfg (Config): hydra configuration object
    """
    # Print config
    print(OmegaConf.to_yaml(cfg))
    # Instantiate ETL session
    session = instantiate(cfg.session)
    # Initialise and run step
    step = instantiate(cfg.step, session=session)
    step.run()


if __name__ == "__main__":
    run_step()
