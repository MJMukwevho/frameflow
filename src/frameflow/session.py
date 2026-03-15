from __future__ import annotations

from frameflow.api.dataframe import FrameflowDataFrame
from frameflow.execution.selector import get_engine_adapter


class Session:
    """
    Main entrypoint for interacting with Frameflow.

    A Session selects an execution engine and returns FrameflowDataFrame
    objects so the public API stays engine-agnostic.
    """

    def __init__(self, engine: str = "pandas") -> None:
        self.engine = engine
        self.adapter = get_engine_adapter(engine)

    def _wrap(self, df) -> FrameflowDataFrame:
        return FrameflowDataFrame(df, self.adapter)

    def _unwrap(self, df):
        if isinstance(df, FrameflowDataFrame):
            return df._engine_df
        return df

    # Read operations
    def read_csv(self, path: str, **kwargs) -> FrameflowDataFrame:
        df = self.adapter.read_csv(path, **kwargs)
        return self._wrap(df)

    def read_json(self, path: str, **kwargs) -> FrameflowDataFrame:
        df = self.adapter.read_json(path, **kwargs)
        return self._wrap(df)

    def read_parquet(self, path: str, **kwargs) -> FrameflowDataFrame:
        df = self.adapter.read_parquet(path, **kwargs)
        return self._wrap(df)

    def read_excel(self, path: str, **kwargs) -> FrameflowDataFrame:
        df = self.adapter.read_excel(path, **kwargs)
        return self._wrap(df)

    # Write operations
    def write_csv(self, df, path: str, **kwargs) -> None:
        self.adapter.write_csv(self._unwrap(df), path, **kwargs)

    def write_json(self, df, path: str, **kwargs) -> None:
        self.adapter.write_json(self._unwrap(df), path, **kwargs)

    def write_parquet(self, df, path: str, **kwargs) -> None:
        self.adapter.write_parquet(self._unwrap(df), path, **kwargs)

    def write_excel(self, df, path: str, **kwargs) -> None:
        self.adapter.write_excel(self._unwrap(df), path, **kwargs)

    # Data operations
    def select(self, df, columns) -> FrameflowDataFrame:
        result = self.adapter.select(self._unwrap(df), columns)
        return self._wrap(result)

    def filter(self, df, condition) -> FrameflowDataFrame:
        result = self.adapter.filter(self._unwrap(df), condition)
        return self._wrap(result)

    def groupby(self, df, by):
        return self.adapter.groupby(self._unwrap(df), by)

    def agg(self, grouped_df, aggregations) -> FrameflowDataFrame:
        result = self.adapter.agg(grouped_df, aggregations)
        return self._wrap(result)

    def join(self, left, right, on, how: str = "inner") -> FrameflowDataFrame:
        result = self.adapter.join(
            self._unwrap(left),
            self._unwrap(right),
            on=on,
            how=how,
        )
        return self._wrap(result)

    def union(self, df1, df2) -> FrameflowDataFrame:
        result = self.adapter.union(self._unwrap(df1), self._unwrap(df2))
        return self._wrap(result)

    def sort(self, df, by, ascending: bool = True) -> FrameflowDataFrame:
        result = self.adapter.sort(self._unwrap(df), by=by, ascending=ascending)
        return self._wrap(result)

    def head(self, df, n: int = 5) -> FrameflowDataFrame:
        result = self.adapter.head(self._unwrap(df), n=n)
        return self._wrap(result)

    def collect(self, df):
        return self.adapter.collect(self._unwrap(df))