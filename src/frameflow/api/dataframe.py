from __future__ import annotations

from typing import Any, Iterable

from frameflow.adapters.base import EngineAdapter


class FrameflowDataFrame:
    """
    Engine-agnostic dataframe wrapper.

    Holds a reference to the underlying engine dataframe and the
    adapter responsible for executing operations.
    """

    def __init__(self, data: Any, adapter: EngineAdapter) -> None:
        self._data = data
        self._adapter = adapter

    # -------------------------
    # Core operations
    # -------------------------
    def select(self, columns: Iterable[str]) -> "FrameflowDataFrame":
        result = self._adapter.select(self._data, columns)
        return FrameflowDataFrame(result, self._adapter)

    def filter(self, condition: Any) -> "FrameflowDataFrame":
        result = self._adapter.filter(self._data, condition)
        return FrameflowDataFrame(result, self._adapter)

    def groupby(self, by: str | list[str]):
        return self._adapter.groupby(self._data, by)

    def join(
        self,
        other: "FrameflowDataFrame",
        on: str | list[str],
        how: str = "inner",
    ) -> "FrameflowDataFrame":
        result = self._adapter.join(self._data, other._data, on, how)
        return FrameflowDataFrame(result, self._adapter)

    def union(self, other: "FrameflowDataFrame") -> "FrameflowDataFrame":
        result = self._adapter.union(self._data, other._data)
        return FrameflowDataFrame(result, self._adapter)

    def sort(
        self,
        by: str | list[str],
        ascending: bool = True,
    ) -> "FrameflowDataFrame":
        result = self._adapter.sort(self._data, by, ascending)
        return FrameflowDataFrame(result, self._adapter)

    # -------------------------
    # Materialization
    # -------------------------
    def head(self, n: int = 5) -> "FrameflowDataFrame":
        result = self._adapter.head(self._data, n)
        return FrameflowDataFrame(result, self._adapter)

    def collect(self) -> list[dict]:
        return self._adapter.collect(self._data)

    # -------------------------
    # Internal access
    # -------------------------
    @property
    def _engine_df(self) -> Any:
        """Access underlying engine dataframe (internal use)."""
        return self._data