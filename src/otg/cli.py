"""CLI for OTG."""
from __future__ import annotations

import hydra
from hydra.utils import instantiate
from omegaconf import OmegaConf
from pyspark.conf import SparkConf

from otg.config import Config, register_configs

register_configs()


@hydra.main(version_base="1.1", config_path=None, config_name="config")
def main(cfg: Config) -> None:
    """OTG ETL CLI.

    Args:
        cfg (Config): hydra configuration object
    """
    print(OmegaConf.to_yaml(cfg))
    # Instantiate ETL session
    step_spark_conf = (
        SparkConf().setAll(cfg.step.custom_spark_conf.items())
        if cfg.step.custom_spark_conf
        else None
    )
    session = instantiate(cfg.session, extended_conf=step_spark_conf)
    # Initialise and run step
    step = instantiate(
        cfg.step, session=session
    )  # cfg.step contains the whole YAML corresponding to the step
    step.run()


if __name__ == "__main__":
    main()
