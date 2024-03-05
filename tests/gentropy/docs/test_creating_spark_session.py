"""Testing creating spark session docs."""
from gentropy.common.session import Session

from docs.src_snippets.howto.python_api.a_creating_spark_session import (
    custom_session,
    default_session,
)


def test_default_session() -> None:
    """Test default session."""
    session = default_session()
    assert isinstance(session, Session)


def test_custom_session() -> None:
    """Test custom session."""
    session = custom_session()
    assert isinstance(session, Session)
