from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from frameflow import Session


@pytest.fixture
def session() -> Session:
    return Session(engine="pandas")


@pytest.fixture
def sales_csv(tmp_path: Path) -> str:
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "product": ["A", "B", "C", "A"],
            "amount": [100, 250, 50, 30],
        }
    ).to_csv(path, index=False)
    return str(path)
