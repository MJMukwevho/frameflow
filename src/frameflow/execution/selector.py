from __future__ import annotations

from frameflow.adapters.base import EngineAdapter
from frameflow.adapters.pandas.adapter import PandasAdapter
from frameflow.exceptions import UnsupportedEngineError

SUPPORTED_ENGINES = ("pandas",)
# On the roadmap, but no adapter yet
PLANNED_ENGINES = ("polars", "spark")


def get_engine_adapter(engine: str) -> EngineAdapter:
    if engine == "pandas":
        return PandasAdapter()

    if engine in PLANNED_ENGINES:
        raise UnsupportedEngineError(
            f"The {engine} engine is planned but not implemented yet. "
            f"Supported engines: {', '.join(SUPPORTED_ENGINES)}"
        )

    raise UnsupportedEngineError(
        f"Unknown engine: {engine!r}. Supported engines: {', '.join(SUPPORTED_ENGINES)}"
    )
