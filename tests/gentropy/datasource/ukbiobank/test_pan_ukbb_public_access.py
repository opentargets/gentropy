"""Opt-in network smoke test for public Pan-UKBB BlockMatrix access."""

import pytest

from gentropy.common.session import Session


@pytest.mark.network
@pytest.mark.download_jars_from_web
def test_public_pan_ukbb_block_matrix_is_readable() -> None:
    """Read a minimal slice from the public CSA Pan-UKBB BlockMatrix."""
    from hail.linalg import BlockMatrix

    session = Session(
        start_hail=True,
        add_s3_connector=True,
        s3_configuration={"anonymous": True},
        dynamic_allocation=False,
    )
    try:
        matrix = BlockMatrix.read(
            "s3a://pan-ukb-us-east-1/ld_release/UKBB.CSA.ldadj.bm"
        )
        assert matrix[0:1, 0:1].collect().shape == (1, 1)
    finally:
        session.spark.stop()
