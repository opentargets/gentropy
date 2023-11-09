"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf


@hydra.main(version_base="1.3", config_path=None, config_name="config")
def main(cfg: DictConfig) -> None:
    """OTG ETL CLI.

    Args:
        cfg (DictConfig): hydra configuration object
    """
    print(OmegaConf.to_yaml(cfg))
    # Initialise and run step.
    instantiate(cfg.step)


if __name__ == "__main__":
    main()
