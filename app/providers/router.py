"""Provider selection: weighted choice among providers whose circuit allows traffic."""
from __future__ import annotations

import random
from typing import Optional

from app.providers.base_provider import TelecomProvider
from app.providers.circuit_breaker import CircuitBreaker


class ProviderRouter:
    def __init__(self, providers: list[TelecomProvider], breaker: CircuitBreaker,
                 weights: Optional[dict[str, float]] = None, seed: int = 11) -> None:
        self.providers = {p.name: p for p in providers}
        self.breaker = breaker
        self.weights = weights or {p.name: 1.0 for p in providers}
        self.rng = random.Random(seed)
        for name in self.providers:
            breaker.ensure(name)

    def get(self, name: str) -> Optional[TelecomProvider]:
        return self.providers.get(name)

    def names(self) -> list[str]:
        return list(self.providers)

    def routable_names(self) -> list[str]:
        """Providers that can actually receive traffic (weight > 0)."""
        return [n for n in self.providers if self.weights.get(n, 0) > 0]

    def select(self) -> Optional[TelecomProvider]:
        """Pick a provider. ``allow_request`` may consume a HALF_OPEN probe slot,
        so we only ask the breaker about the provider we actually choose."""
        candidates = [n for n in self.providers if self.weights.get(n, 0) > 0]
        while candidates:
            total = sum(self.weights[n] for n in candidates)
            pick = self.rng.uniform(0, total)
            acc = 0.0
            chosen = candidates[-1]
            for n in candidates:
                acc += self.weights[n]
                if pick <= acc:
                    chosen = n
                    break
            if self.breaker.allow_request(chosen):
                return self.providers[chosen]
            candidates.remove(chosen)
        return None

    def any_available(self) -> bool:
        """Cheap check used for capacity planning (does not consume probe slots)."""
        for name in self.providers:
            row = self.breaker.get(name)
            if row is None or row["state"] != "OPEN":
                return True
            if row["retry_at"] is not None and row["retry_at"] <= self.breaker.clock.now():
                return True
        return False
