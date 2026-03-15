from __future__ import annotations

from frameflow.execution.selector import get_engine_adapter


class Session:
    """
    Main entrypoint for interacting with Frameflow.

    A Session selects an execution engine and delegates dataframe
    operations to the corresponding adapter.
    """

    def __init__(self, engine: str = "pandas") -> None:
        self.engine = engine
        self.adapter = get_engine_adapter(engine)

    # Read operations
    def read_csv(self, path: str, **kwargs):
        return self.adapter.read_csv(path, **kwargs)

    def read_json(self, path: str, **kwargs):
        return self.adapter.read_json(path, **kwargs)

    def read_parquet(self, path: str, **kwargs):
        return self.adapter.read_parquet(path, **kwargs)

    def read_excel(self, path: str, **kwargs):
        return self.adapter.read_excel(path, **kwargs)

    # Write operations
    def write_csv(self, df, path: str, **kwargs) -> None:
        self.adapter.write_csv(df, path, **kwargs)

    def write_json(self, df, path: str, **kwargs) -> None:
        self.adapter.write_json(df, path, **kwargs)

    def write_parquet(self, df, path: str, **kwargs) -> None:
        self.adapter.write_parquet(df, path, **kwargs)

    def write_excel(self, df, path: str, **kwargs) -> None:
        self.adapter.write_excel(df, path, **kwargs)

    # Data operations
    def select(self, df, columns):
        return self.adapter.select(df, columns)

    def filter(self, df, condition):
        return self.adapter.filter(df, condition)

    def groupby(self, df, by):
        return self.adapter.groupby(df, by)

    def agg(self, grouped_df, aggregations):
        return self.adapter.agg(grouped_df, aggregations)

    def join(self, left, right, on, how: str = "inner"):
        return self.adapter.join(left, right, on=on, how=how)

    def union(self, df1, df2):
        return self.adapter.union(df1, df2)

    def sort(self, df, by, ascending: bool = True):
        return self.adapter.sort(df, by=by, ascending=ascending)

    def head(self, df, n: int = 5):
        return self.adapter.head(df, n=n)

    def collect(self, df):
        return self.adapter.collect(df)