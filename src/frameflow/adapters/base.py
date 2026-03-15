from abc import ABC, abstractmethod
from typing import Any, Iterable


class DataReader(ABC):
    """Read data from external sources."""

    @abstractmethod
    def read_csv(self, path: str, **kwargs) -> Any:
        ...

    @abstractmethod
    def read_json(self, path: str, **kwargs) -> Any:
        ...

    @abstractmethod
    def read_parquet(self, path: str, **kwargs) -> Any:
        ...

    @abstractmethod
    def read_excel(self, path: str, **kwargs) -> Any:
        ...


class DataWriter(ABC):
    """Write data to external sinks."""

    @abstractmethod
    def write_csv(self, df: Any, path: str, **kwargs) -> None:
        ...

    @abstractmethod
    def write_json(self, df: Any, path: str, **kwargs) -> None:
        ...

    @abstractmethod
    def write_parquet(self, df: Any, path: str, **kwargs) -> None:
        ...

    @abstractmethod
    def write_excel(self, df: Any, path: str, **kwargs) -> None:
        ...


class DataOps(ABC):
    """Core dataframe operations."""

    @abstractmethod
    def select(self, df: Any, columns: Iterable[str]) -> Any:
        ...

    @abstractmethod
    def filter(self, df: Any, condition: Any) -> Any:
        ...

    @abstractmethod
    def groupby(self, df: Any, by: str | list[str]) -> Any:
        ...

    @abstractmethod
    def agg(self, grouped_df: Any, aggregations: dict[str, Any]) -> Any:
        ...

    @abstractmethod
    def join(
        self,
        left: Any,
        right: Any,
        on: str | list[str],
        how: str = "inner",
    ) -> Any:
        ...

    @abstractmethod
    def union(self, df1: Any, df2: Any) -> Any:
        ...

    @abstractmethod
    def sort(
        self,
        df: Any,
        by: str | list[str],
        ascending: bool = True,
    ) -> Any:
        ...

    @abstractmethod
    def head(self, df: Any, n: int = 5) -> Any:
        ...

    @abstractmethod
    def collect(self, df: Any) -> Any:
        ...


class EngineAdapter(DataReader, DataWriter, DataOps):
    """
    Unified contract that all execution engines must implement.

    Examples:
        - PandasAdapter
        - PolarsAdapter
        - SparkAdapter
    """
    pass