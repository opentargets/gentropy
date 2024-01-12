"""Test the command-line interface (CLI)."""
from unittest.mock import patch

import pytest
from hydra.errors import ConfigCompositionException
from omegaconf.errors import MissingMandatoryValue
from otg.cli import main


def test_main_no_step() -> None:
    """Test the main function of the CLI without a valid step."""
    override_key = "step"
    available_steps = [
        "colocalisation",
        "eqtl_catalogue",
        "finngen_studies",
        "finngen_sumstat_preprocess",
        "gene_index",
        "gwas_catalog_ingestion",
        "gwas_catalog_study_curation",
        "gwas_catalog_study_inclusion",
        "gwas_catalog_sumstat_preprocess",
        "ld_based_clumping",
        "ld_index",
        "locus_to_gene",
        "overlaps",
        "pics",
        "ukbiobank",
        "variant_annotation",
        "variant_index",
        "variant_to_gene",
        "window_based_clumping",
    ]
    opts = "\n\t".join(available_steps)
    expected = f"You must specify '{override_key}', e.g, {override_key}=<OPTION>\nAvailable options:\n\t{opts}"

    with patch("sys.argv", ["cli.py"]), pytest.raises(
        ConfigCompositionException, match=expected
    ):
        main()


def test_main_step() -> None:
    """Test the main function of the CLI complains about mandatory values."""
    with patch("sys.argv", ["cli.py", "step=gene_index"]), pytest.raises(
        MissingMandatoryValue, match="Missing mandatory value: step.target_path"
    ):
        main()
