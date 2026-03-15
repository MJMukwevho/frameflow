# src/frameflow/adapters/pandas/adapter.py

from __future__ import annotations

from typing import Any

import pandas as pd

from frameflow.adapters.base import EngineAdapter


class PandasAdapter(EngineAdapter):
    """Pandas implementation of the Frameflow engine adapter contract."""

    # -------------------------
    # Read operations
    # -------------------------
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        return pd.read_csv(path, **kwargs)

    def read_json(self, path: str, **kwargs) -> pd.DataFrame:
        return pd.read_json(path, **kwargs)

    def read_parquet(self, path: str, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(path, **kwargs)

    def read_excel(self, path: str, **kwargs) -> pd.DataFrame:
        return pd.read_excel(path, **kwargs)

    # -------------------------
    # Write operations
    # -------------------------
    def write_csv(self, df: pd.DataFrame, path: str, **kwargs) -> None:
        df.to_csv(path, index=False, **kwargs)

    def write_json(self, df: pd.DataFrame, path: str, **kwargs) -> None:
        df.to_json(path, **kwargs)

    def write_parquet(self, df: pd.DataFrame, path: str, **kwargs) -> None:
        df.to_parquet(path, index=False, **kwargs)

    def write_excel(self, df: pd.DataFrame, path: str, **kwargs) -> None:
        df.to_excel(path, index=False, **kwargs)

    # -------------------------
    # Data operations
    # -------------------------
    def select(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        return df.loc[:, columns]

    def filter(self, df: pd.DataFrame, condition: Any) -> pd.DataFrame:
        """
        Supports either:
        - a boolean mask / native pandas condition
        - a callable: lambda df: ...
        """
        if callable(condition):
            condition = condition(df)
        return df.loc[condition]

    def groupby(self, df: pd.DataFrame, by: str | list[str]) -> Any:
        return df.groupby(by)

    def agg(self, grouped_df: Any, aggregations: dict[str, Any]) -> pd.DataFrame:
        return grouped_df.agg(aggregations).reset_index()

    def join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str | list[str],
        how: str = "inner",
    ) -> pd.DataFrame:
        return left.merge(right, on=on, how=how)

    def union(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([df1, df2], ignore_index=True)

    def sort(
        self,
        df: pd.DataFrame,
        by: str | list[str],
        ascending: bool = True,
    ) -> pd.DataFrame:
        return df.sort_values(by=by, ascending=ascending)

    def head(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        return df.head(n)

    def collect(self, df: pd.DataFrame) -> list[dict]:
        return df.to_dict(orient="records")