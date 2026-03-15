from frameflow.adapters.pandas.adapter import PandasAdapter


def get_engine_adapter(engine: str):
    if engine == "pandas":
        return PandasAdapter()

    raise ValueError(f"Unsupported engine: {engine}")