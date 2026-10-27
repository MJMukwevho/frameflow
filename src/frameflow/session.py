from __future__ import annotations

from typing import Any

from frameflow.api.dataframe import FrameflowDataFrame
from frameflow.execution.selector import get_engine_adapter


class Session:
    """
    Main entrypoint for interacting with Frameflow.

    A Session selects an execution engine and reads data into
    FrameflowDataFrame objects. Everything else (filtering, joining,
    writing, ...) is a method on the returned dataframe, so the public
    API stays engine-agnostic.
    """

    def __init__(self, engine: str = "pandas") -> None:
        self.engine = engine
        self.adapter = get_engine_adapter(engine)

    def _wrap(self, df: Any) -> FrameflowDataFrame:
        return FrameflowDataFrame(df, self.adapter)

    def read_csv(self, path: str, **kwargs: Any) -> FrameflowDataFrame:
        return self._wrap(self.adapter.read_csv(path, **kwargs))

    def read_json(self, path: str, **kwargs: Any) -> FrameflowDataFrame:
        return self._wrap(self.adapter.read_json(path, **kwargs))

    def read_parquet(self, path: str, **kwargs: Any) -> FrameflowDataFrame:
        return self._wrap(self.adapter.read_parquet(path, **kwargs))

    def read_excel(self, path: str, **kwargs: Any) -> FrameflowDataFrame:
        return self._wrap(self.adapter.read_excel(path, **kwargs))
