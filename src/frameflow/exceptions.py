from __future__ import annotations


class FrameflowError(Exception):
    """Base class for all Frameflow errors."""


class UnsupportedEngineError(FrameflowError, ValueError):
    """The requested engine doesn't exist or isn't implemented yet."""


class UnsupportedOperationError(FrameflowError):
    """The selected engine can't run the requested operation."""


class MissingDependencyError(FrameflowError, ImportError):
    """An optional dependency needed for this engine or file format isn't installed."""

    def __init__(self, package: str, extra: str) -> None:
        self.package = package
        self.extra = extra
        super().__init__(
            f"{package} is required for this feature. "
            f"Install it with: pip install 'frameflow[{extra}]'"
        )
