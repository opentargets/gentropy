"""Configuration helper."""
from __future__ import annotations

import hydra
from omegaconf import DictConfig, OmegaConf


@hydra.main(version_base=None, config_path=None, config_name="my_config")
def configure(cfg: DictConfig) -> None:
    """Prints the configuration.

    Args:
        cfg (DictConfig): configuration object
    """
    print(OmegaConf.to_yaml(cfg))


if __name__ == "__main__":
    configure()
