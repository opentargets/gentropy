from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql.types import StructType

from etl.json import SCHEMA_DIR

if TYPE_CHECKING:
    from pytest import Metafunc


def pytest_generate_tests(metafunc: Metafunc) -> None:
    schemas = [f for f in os.listdir(SCHEMA_DIR) if f.endswith(".json")]
    metafunc.parametrize("schema_json", schemas)


def test_schema(schema_json: str) -> None:
    core_schema = json.loads(Path(SCHEMA_DIR, schema_json).read_text(encoding="utf-8"))
    isinstance(StructType.fromJson(core_schema), StructType)
