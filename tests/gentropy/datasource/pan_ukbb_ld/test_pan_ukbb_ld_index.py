"""Tests of the Pan-UKBB LD index extraction, on a small hail block matrix."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import hail as hl
import numpy as np
import pytest
from hail.linalg import BlockMatrix
from pyspark.sql import DataFrame, SparkSession

from gentropy.common.session import Session
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix
from utils.spark import get_spark_testing_conf

# (variantId, chromosome, alleleOrder) of matrix indices 0-5.
VARIANTS = [
    ("1_10_A_G", "1", 1),
    ("1_20_C_T", "1", -1),
    ("1_30_G_A", "1", 1),
    ("1_40_T_C", "1", 1),
    ("1_50_A_C", "1", 1),
    ("2_5_C_G", "2", 1),
]


def _matrix() -> np.ndarray:
    """Symmetric r of the six variants.

    Returns:
        np.ndarray: r, with ones on the diagonal.
    """
    r = np.eye(6)
    for i, j, value in [
        (0, 1, 0.9),  # same block
        (0, 2, 0.3),  # below the threshold
        (1, 2, 0.8),  # next block
        (0, 4, 0.75),  # two blocks apart
        (4, 5, 0.95),  # across chromosomes
    ]:
        r[i, j] = r[j, i] = value
    return r


def _expected() -> set[tuple[str, str, float]]:
    """The index built by hand: both orders, signed by the allele orders.

    Returns:
        set[tuple[str, str, float]]: (variantId, tagVariantId, r) rows.
    """
    r = _matrix()
    rows = set()
    for i, (id_i, chr_i, order_i) in enumerate(VARIANTS):
        for j, (id_j, chr_j, order_j) in enumerate(VARIANTS):
            if chr_i == chr_j and abs(r[i, j]) >= 0.5**0.5:
                rows.add((id_i, id_j, round(r[i, j] * order_i * order_j, 6)))
    return rows


def _stop_spark() -> None:
    """Stop hail and the active Spark session, if any."""
    hl.stop()
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()


@pytest.fixture()
def hail_session() -> Generator[Session, None, None]:
    """A session of its own, with hail.

    Yields:
        Session: The session.
    """
    _stop_spark()
    yield Session(
        spark_uri="local[1]",
        start_hail=True,
        extended_spark_conf=dict(get_spark_testing_conf().getAll()),
    )
    _stop_spark()


@pytest.mark.no_shared_spark
def test_as_ld_index_matches_a_hand_built_index(
    hail_session: Session, tmp_path: Path
) -> None:
    """Pairs above the threshold on one chromosome, however far apart, signed and in both orders."""
    path = (tmp_path / "UKBB.EUR.ldadj.bm").as_posix()
    # Pan-UKBB stores one triangle of the matrix.
    BlockMatrix.from_numpy(np.tril(_matrix()), block_size=2).write(path)
    aligned = hail_session.spark.createDataFrame(
        [(id_, chr_, order, idx) for idx, (id_, chr_, order) in enumerate(VARIANTS)],
        "variantId string, chromosome string, alleleOrder int, idx long",
    )

    index = PanUKBBLDMatrix(pan_ukbb_bm_path=path.replace("EUR", "{POP}")).as_ld_index(
        "EUR", aligned, min_r2=0.5
    )

    rows = index.df.collect()
    assert {row.population for row in rows} == {"nfe"}
    assert len(rows) == len(_expected())
    assert {
        (row.variantId, row.tagVariantId, round(row.r, 6)) for row in rows
    } == _expected()


@pytest.mark.no_shared_spark
@pytest.mark.parametrize(
    ("aligned_indices", "expected"),
    [
        # Variants of every block: every stored block is read, including the one two apart.
        (
            range(6),
            {
                (0, 0),
                (1, 0),
                (1, 1),
                (2, 1),
                (2, 2),
                (3, 3),
                (4, 0),
                (4, 4),
                (5, 4),
                (5, 5),
            },
        ),
        # Variants of the first two blocks: the third block is not read.
        (range(4), {(0, 0), (1, 0), (1, 1), (2, 1), (2, 2), (3, 3)}),
    ],
    ids=["all-variants", "variants-of-two-blocks"],
)
def test_as_ld_index_reads_only_the_variants_blocks(
    hail_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    aligned_indices: range,
    expected: set[tuple[int, int]],
) -> None:
    """With restrict_to_variants, the entries read are those of blocks covering the variants."""
    path = (tmp_path / "UKBB.EUR.ldadj.bm").as_posix()
    BlockMatrix.from_numpy(np.tril(_matrix()), block_size=2).write(path)
    aligned = hail_session.spark.createDataFrame(
        [
            (id_, chr_, order, idx)
            for idx, (id_, chr_, order) in enumerate(VARIANTS)
            if idx in aligned_indices
        ],
        "variantId string, chromosome string, alleleOrder int, idx long",
    )
    read: list[tuple[int, int]] = []

    def capture(pairs: DataFrame, aligned: DataFrame, population: str) -> DataFrame:
        """Record the entries read, and resolve them as usual.

        Args:
            pairs (DataFrame): Matrix entries.
            aligned (DataFrame): Aligned variants.
            population (str): LD population label.

        Returns:
            DataFrame: The resolved pairs.
        """
        read.extend((row.i, row.j) for row in pairs.collect())
        return resolve(pairs, aligned, population)

    resolve = PanUKBBLDMatrix._resolve_pairs
    monkeypatch.setattr(PanUKBBLDMatrix, "_resolve_pairs", staticmethod(capture))

    PanUKBBLDMatrix(pan_ukbb_bm_path=path.replace("EUR", "{POP}")).as_ld_index(
        "EUR", aligned, min_r2=0.5, restrict_to_variants=True
    )

    assert set(read) == expected
