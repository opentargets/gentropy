"""Docs to create a default Spark Session."""
from gentropy import Session


def default_session() -> Session:
    """Create a default Spark Session.

    Returns:
        Session: Spark Session.
    """
    # --8<-- [start:default_session]
    from gentropy import Session

    # Create a default Spark Session
    session = Session()
    # --8<-- [end:default_session]
    return session


def custom_session() -> Session:
    """Create a custom Spark Session.

    Returns:
        Session: Spark Session.
    """
    # --8<-- [start:custom_session]
    from gentropy import Session

    # Create a Spark session with increased driver memory
    session = Session(extended_spark_conf={"spark.driver.memory": "4g"})
    # --8<-- [end:custom_session]
    return session
