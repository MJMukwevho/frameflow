from __future__ import annotations

from pathlib import Path

import pandas as pd

from frameflow.adapters.pandas import PandasAdapter
from frameflow.session import Session


def test_session_uses_pandas_adapter() -> None:
    session = Session(engine="pandas")
    assert isinstance(session.adapter, PandasAdapter)


def test_pandas_session_read_select_filter_collect(tmp_path: Path) -> None:
    # Arrange
    input_path = tmp_path / "sales.csv"

    source_df = pd.DataFrame(
        {
            "product": ["A", "B", "C"],
            "amount": [100, 250, 50],
            "category": ["x", "y", "x"],
        }
    )
    source_df.to_csv(input_path, index=False)

    session = Session(engine="pandas")

    # Act
    df = session.read_csv(str(input_path))
    df = session.select(df, ["product", "amount"])
    df = session.filter(df, lambda frame: frame["amount"] > 100)
    result = session.collect(df)

    # Assert
    assert result == [
        {"product": "B", "amount": 250},
    ]