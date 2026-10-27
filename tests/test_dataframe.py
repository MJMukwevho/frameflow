from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from frameflow import FrameflowDataFrame, GroupedFrame, MissingDependencyError, Session


def test_operations_chain_and_do_not_leak(session: Session, sales_csv: str) -> None:
    result = (
        session.read_csv(sales_csv)
        .filter(lambda frame: frame["amount"] > 40)
        .select(["product", "amount"])
        .sort("amount", descending=True)
        .head(2)
    )

    assert isinstance(result, FrameflowDataFrame)
    assert result.collect() == [
        {"product": "B", "amount": 250},
        {"product": "A", "amount": 100},
    ]


def test_group_by_wraps_the_engine_grouping(session: Session, sales_csv: str) -> None:
    grouped = session.read_csv(sales_csv).group_by("product")

    assert isinstance(grouped, GroupedFrame)
    assert not isinstance(grouped, pd.core.groupby.DataFrameGroupBy)

    totals = grouped.agg({"amount": "sum"}).sort("product")

    assert isinstance(totals, FrameflowDataFrame)
    assert totals.collect() == [
        {"product": "A", "amount": 130},
        {"product": "B", "amount": 250},
        {"product": "C", "amount": 50},
    ]


def test_join_and_union(session: Session, sales_csv: str, tmp_path: Path) -> None:
    names_path = tmp_path / "names.csv"
    pd.DataFrame({"product": ["A", "B"], "name": ["Apple", "Banana"]}).to_csv(
        names_path, index=False
    )
    sales = session.read_csv(sales_csv)
    names = session.read_csv(str(names_path))

    joined = sales.join(names, on="product").sort(["product", "amount"])
    assert joined.collect() == [
        {"product": "A", "amount": 30, "name": "Apple"},
        {"product": "A", "amount": 100, "name": "Apple"},
        {"product": "B", "amount": 250, "name": "Banana"},
    ]

    assert len(sales.union(sales).collect()) == 8


def test_write_csv_round_trips(session: Session, sales_csv: str, tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    df = session.read_csv(sales_csv)

    df.write_csv(str(out))

    assert session.read_csv(str(out)).collect() == df.collect()


def test_missing_optional_dependency_names_the_extra(
    session: Session, sales_csv: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_pyarrow(*args: object, **kwargs: object) -> None:
        raise ImportError("Unable to find a usable engine; tried using: 'pyarrow'")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", no_pyarrow)
    df = session.read_csv(sales_csv)

    with pytest.raises(MissingDependencyError, match=r"pip install 'frameflow\[parquet\]'"):
        df.write_parquet(str(tmp_path / "out.parquet"))
