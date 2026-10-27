from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from frameflow import FrameflowDataFrame, Session, UnsupportedEngineError


def test_session_does_not_leak_pandas_dataframe(session: Session, sales_csv: str) -> None:
    df = session.read_csv(sales_csv)

    assert isinstance(df, FrameflowDataFrame)
    assert not isinstance(df, pd.DataFrame)


def test_read_json(session: Session, tmp_path: Path) -> None:
    path = tmp_path / "sales.json"
    pd.DataFrame({"product": ["A"], "amount": [100]}).to_json(path, orient="records")

    df = session.read_json(str(path))

    assert df.collect() == [{"product": "A", "amount": 100}]


def test_session_only_reads_data(session: Session) -> None:
    # Transformations and writes live on FrameflowDataFrame, not the session
    for name in ("select", "filter", "join", "write_csv"):
        assert not hasattr(session, name)


@pytest.mark.parametrize("engine", ["polars", "spark"])
def test_planned_engines_raise_a_clear_error(engine: str) -> None:
    with pytest.raises(UnsupportedEngineError, match="planned but not implemented"):
        Session(engine=engine)


def test_unknown_engine_lists_supported_engines() -> None:
    with pytest.raises(UnsupportedEngineError, match="Supported engines: pandas"):
        Session(engine="duckdb")


def test_unsupported_engine_error_is_still_a_value_error() -> None:
    # Code that caught the old ValueError keeps working
    with pytest.raises(ValueError):
        Session(engine="duckdb")
