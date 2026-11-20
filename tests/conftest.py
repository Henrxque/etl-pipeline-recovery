import json
import shutil

import pytest

from recovery.engine import Engine
from recovery.runtime import create_spark
from recovery.source import FIXTURES, load_sources
from recovery.store import Store
from recovery.transform import recover


@pytest.fixture(scope="session")
def spark():
    session = create_spark()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def golden_plan(spark):
    load_sources(spark, FIXTURES)
    return recover(spark, "2026-11-18")


@pytest.fixture
def inputs(tmp_path):
    path = tmp_path / "inputs"
    path.mkdir()
    for name in ["order_lines.csv", "customer_history.csv"]:
        shutil.copyfile(FIXTURES / name, path / name)
    return path


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "target.sqlite3")


@pytest.fixture
def engine(store, spark):
    return Engine(store, lambda: spark)


@pytest.fixture
def oracle():
    return json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))
