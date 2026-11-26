"""Tests of the LDSC heritability step's ancestry inference."""

from __future__ import annotations

import pytest

from gentropy.ldsc import HeritabilityEstimateStep


@pytest.mark.parametrize(
    ("ld_population_structure", "expected"),
    [
        pytest.param([("nfe", 1.0)], "nfe", id="nfe"),
        pytest.param([("csa", 1.0)], "eas", id="csa uses eas"),
        pytest.param([("mid", 1.0)], "eas", id="mid uses eas"),
        pytest.param(
            [("csa", 0.3), ("eas", 0.3), ("nfe", 0.4)], "eas", id="csa adds to eas"
        ),
        pytest.param([("nfe", 0.5), ("mid", 0.5)], "nfe", id="tie prefers nfe"),
    ],
)
def test_infer_ld_ancestry(
    ld_population_structure: list[tuple[str, float]], expected: str
) -> None:
    """Every LD population label has LD scores, with csa and mid using eas."""
    structure = [
        {"ldPopulation": pop, "relativeSampleSize": size}
        for pop, size in ld_population_structure
    ]
    assert HeritabilityEstimateStep._infer_ld_ancestry(structure) == expected
