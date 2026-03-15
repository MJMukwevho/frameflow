from __future__ import annotations

from pathlib import Path

import pandas as pd

from frameflow.api.dataframe import FrameflowDataFrame
from frameflow.session import Session


def test_session_does_not_leak_pandas_dataframe(tmp_path: Path) -> None:
    input_path = tmp_path / "sales.csv"

    source_df = pd.DataFrame(
        {
            "product": ["A", "B", "C"],
            "amount": [100, 250, 50],
        }
    )
    source_df.to_csv(input_path, index=False)

    session = Session(engine="pandas")

    df = session.read_csv(str(input_path))

    assert isinstance(df, FrameflowDataFrame)
    assert not isinstance(df, pd.DataFrame)


def test_session_select_does_not_leak_pandas_dataframe(tmp_path: Path) -> None:
    input_path = tmp_path / "sales.csv"

    source_df = pd.DataFrame(
        {
            "product": ["A", "B", "C"],
            "amount": [100, 250, 50],
        }
    )
    source_df.to_csv(input_path, index=False)

    session = Session(engine="pandas")

    df = session.read_csv(str(input_path))
    selected = session.select(df, ["product", "amount"])

    assert isinstance(selected, FrameflowDataFrame)
    assert not isinstance(selected, pd.DataFrame)


def test_session_filter_does_not_leak_pandas_dataframe(tmp_path: Path) -> None:
    input_path = tmp_path / "sales.csv"

    source_df = pd.DataFrame(
        {
            "product": ["A", "B", "C"],
            "amount": [100, 250, 50],
        }
    )
    source_df.to_csv(input_path, index=False)

    session = Session(engine="pandas")

    df = session.read_csv(str(input_path))
    filtered = session.filter(df, lambda frame: frame["amount"] > 100)

    assert isinstance(filtered, FrameflowDataFrame)
    assert not isinstance(filtered, pd.DataFrame)