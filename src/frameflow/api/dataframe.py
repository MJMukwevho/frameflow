from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from frameflow.adapters.base import EngineAdapter, JoinHow


class FrameflowDataFrame:
    """
    Engine-agnostic dataframe.

    Holds a reference to the underlying engine dataframe and the adapter
    responsible for executing operations. Every operation returns a new
    FrameflowDataFrame, so calls can be chained:

        session.read_csv("sales.csv").filter(...).select(["product"]).collect()
    """

    def __init__(self, data: Any, adapter: EngineAdapter) -> None:
        self._data = data
        self._adapter = adapter

    def _wrap(self, data: Any) -> FrameflowDataFrame:
        return FrameflowDataFrame(data, self._adapter)

    # -------------------------
    # Core operations
    # -------------------------
    def select(self, columns: Iterable[str]) -> FrameflowDataFrame:
        return self._wrap(self._adapter.select(self._data, columns))

    def filter(self, condition: Any) -> FrameflowDataFrame:
        """
        Keep the rows matching `condition`.

        For now `condition` is engine-specific: a callable that receives the
        engine dataframe (`lambda df: df["amount"] > 100`) or a native boolean
        mask. Phase 1 replaces this with engine-independent expressions.
        """
        return self._wrap(self._adapter.filter(self._data, condition))

    def group_by(self, by: str | list[str]) -> GroupedFrame:
        return GroupedFrame(self._adapter.groupby(self._data, by), self._adapter)

    def join(
        self,
        other: FrameflowDataFrame,
        on: str | list[str],
        how: JoinHow = "inner",
    ) -> FrameflowDataFrame:
        return self._wrap(self._adapter.join(self._data, other._data, on, how))

    def union(self, other: FrameflowDataFrame) -> FrameflowDataFrame:
        return self._wrap(self._adapter.union(self._data, other._data))

    def sort(
        self,
        by: str | list[str],
        descending: bool = False,
    ) -> FrameflowDataFrame:
        return self._wrap(self._adapter.sort(self._data, by, ascending=not descending))

    def head(self, n: int = 5) -> FrameflowDataFrame:
        return self._wrap(self._adapter.head(self._data, n))

    # -------------------------
    # Materialization
    # -------------------------
    def collect(self) -> list[dict]:
        return self._adapter.collect(self._data)

    # -------------------------
    # Write operations
    # -------------------------
    def write_csv(self, path: str, **kwargs: Any) -> None:
        self._adapter.write_csv(self._data, path, **kwargs)

    def write_json(self, path: str, **kwargs: Any) -> None:
        self._adapter.write_json(self._data, path, **kwargs)

    def write_parquet(self, path: str, **kwargs: Any) -> None:
        self._adapter.write_parquet(self._data, path, **kwargs)

    def write_excel(self, path: str, **kwargs: Any) -> None:
        self._adapter.write_excel(self._data, path, **kwargs)

    # -------------------------
    # Internal access
    # -------------------------
    @property
    def _engine_df(self) -> Any:
        """Access underlying engine dataframe (internal use)."""
        return self._data


class GroupedFrame:
    """
    Result of `FrameflowDataFrame.group_by()`.

    Wraps the engine's grouped object so it doesn't leak; call `agg()` to get
    a FrameflowDataFrame back.
    """

    def __init__(self, grouped: Any, adapter: EngineAdapter) -> None:
        self._grouped = grouped
        self._adapter = adapter

    def agg(self, aggregations: dict[str, Any]) -> FrameflowDataFrame:
        """
        Aggregate each group, for example `{"amount": "sum"}`.

        For now the aggregation names are passed to the engine as-is. Phase 1
        replaces them with engine-independent expressions.
        """
        return FrameflowDataFrame(self._adapter.agg(self._grouped, aggregations), self._adapter)
