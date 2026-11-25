"""Provider abstraction.

The dialer only ever talks to ``TelecomProvider``. Provider-specific details
(latency, retries, webhook formats, credentials) stay behind this interface.
Events come back asynchronously through an ``EventSink`` as normalised
``ProviderEvent`` objects.

Error semantics matter for safety:

* ``ProviderRejected``: the provider definitely did NOT place the call. It is
  safe to release the agent and borrower immediately.
* ``ProviderTimeout``: the outcome is *ambiguous*. The call may or may not be
  ringing. The caller must keep the reservations and reconcile later.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Optional

from app.models.provider_event import ProviderEvent


class ProviderError(Exception):
    pass


class ProviderRejected(ProviderError):
    """Definitive failure: the call was not placed."""


class ProviderTimeout(ProviderError):
    """Ambiguous failure: the call may have been placed."""


@dataclass(frozen=True)
class CallRequest:
    client_ref: str        # our call id; echoed back on every event (idempotency key)
    to_number: str
    campaign_id: str


@dataclass(frozen=True)
class InitiateResult:
    provider_call_id: str
    latency_ms: float


@dataclass(frozen=True)
class ProviderCallStatus:
    provider_call_id: str
    client_ref: str
    state: str                       # INITIATED | RINGING | ANSWERED | COMPLETED | FAILED
    sequence: int
    answered: bool
    talk_duration_s: Optional[float]


EventSink = Callable[[ProviderEvent], None]


class TelecomProvider(ABC):
    name: str

    def __init__(self) -> None:
        self._sink: Optional[EventSink] = None

    def set_event_sink(self, sink: EventSink) -> None:
        self._sink = sink

    def _emit(self, event: ProviderEvent) -> None:
        if self._sink is not None:
            self._sink(event)

    @abstractmethod
    def initiate_call(self, request: CallRequest) -> InitiateResult:
        """Ask the provider to dial. Raises ProviderRejected or ProviderTimeout."""

    @abstractmethod
    def hangup(self, provider_call_id: str) -> None:
        """Best-effort hangup. If the borrower already answered, this ends the conversation."""

    @abstractmethod
    def get_call_status(self, client_ref: str) -> Optional[ProviderCallStatus]:
        """Reconciliation lookup by our reference. ``None`` = provider never saw it.
        Raises ProviderTimeout when the provider is unreachable."""
