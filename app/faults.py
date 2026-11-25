"""Fault injection used by tests, the simulator and the failure-scenario demos."""
from __future__ import annotations

from typing import Callable, Optional


class SimulatedCrash(BaseException):
    """Raised at an injection point to emulate a worker process dying.

    It derives from BaseException so that ordinary ``except Exception``
    handlers cannot swallow it, which is what a real crash looks like. Any
    open transaction is rolled back, and in-memory state is lost.
    """


CrashHook = Callable[[str, str], None]


def crash_at(point: str, *, times: int = 1) -> CrashHook:
    """Return a hook that raises SimulatedCrash the first ``times`` it reaches ``point``."""
    remaining = {"n": times}

    def hook(where: str, ref: str) -> None:
        if where == point and remaining["n"] > 0:
            remaining["n"] -= 1
            raise SimulatedCrash(f"simulated crash at {where} ({ref})")

    return hook


def maybe(hook: Optional[CrashHook], where: str, ref: str) -> None:
    if hook is not None:
        hook(where, ref)
