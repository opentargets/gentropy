"""Tests on the L2G model wrapper."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import skops.io as sio
from xgboost import XGBClassifier

from gentropy.method.l2g.model import LocusToGeneModel

if TYPE_CHECKING:
    from gentropy.common.session import Session


@pytest.fixture()
def fitted_model_bytes() -> bytes:
    """A minimal fitted classifier, serialised the way `save` writes it."""
    model = XGBClassifier(n_estimators=2, max_depth=2)
    model.fit(np.array([[0.0], [1.0], [0.0], [1.0]]), np.array([0, 1, 0, 1]))
    return sio.dumps(model)


def test_load_from_disk_reads_a_gcs_directory(
    session: Session, fitted_model_bytes: bytes
) -> None:
    """Test that a gs:// directory is resolved to the right bucket and blob.

    Building the path with pathlib collapses "gs://bucket/dir" to "gs:/bucket/dir", which sends
    the read down the local-filesystem branch and fails. This pins the bucket and blob names the
    GCS branch asks for.
    """
    blob = MagicMock()
    blob.download_as_string.return_value = fitted_model_bytes
    storage = MagicMock()
    storage.Blob.return_value = blob

    with (
        patch.dict("sys.modules", {"google.cloud": MagicMock(storage=storage)}),
        patch("google.cloud.storage", storage),
    ):
        model = LocusToGeneModel.load_from_disk(session, "gs://a-bucket/some/model/dir")

    assert isinstance(model.model, XGBClassifier)
    assert storage.Bucket.call_args.kwargs["name"] == "a-bucket"
    assert storage.Blob.call_args.kwargs["name"] == "some/model/dir/classifier.skops"


def test_load_from_disk_reads_a_local_directory(
    session: Session, fitted_model_bytes: bytes, tmp_path: object
) -> None:
    """Test that a local directory still works, and that a trailing slash is tolerated."""
    from pathlib import Path

    directory = Path(str(tmp_path)) / "model"
    directory.mkdir()
    (directory / "classifier.skops").write_bytes(fitted_model_bytes)

    for given in (directory.as_posix(), directory.as_posix() + "/"):
        model = LocusToGeneModel.load_from_disk(session, given)
        assert isinstance(model.model, XGBClassifier)
