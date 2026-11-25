"""Safety Controller: the only component that can authorise outbound calls.

Design
------
* Pacing engines (progressive or predictive) send a ``PacingRecommendation``.
  A recommendation is a *wish* and carries no authority.
* The controller re-derives capacity **from the database, using its own
  queries** (it does not use the engine's numbers or ``MetricsReader``).
  It then approves, reduces, rejects, or falls back to progressive (agent-bound)
  dialing.
* An approval is materialised as a ``dial_permits`` row. The Call Allocator
  places calls only by atomically consuming permit slots, and the DB constraint
  ``consumed <= approved`` makes over-spending impossible.
* Capacity computation and permit issuance happen inside one ``BEGIN IMMEDIATE``
  transaction. Two workers evaluating at the same moment are serialised, and
  the second one sees the first one's outstanding permit slots as committed
  capacity. This prevents the classic check-then-act double approval.
* ``SafetyConfig`` is frozen and private to this object. Nothing in the pacing
  layer holds a reference that could switch the safety mode off.

Safety invariant (STRICT mode)
------------------------------
Every unanswered *pooled* (UNBOUND) call must be backed by an agent who will
be free when it is answered::

    U + S  <=  A                (existing commitments covered by idle agents now)
    new    <=  A - (U + S) + W  (look-ahead agents only back *new* calls)

U = unbound pre-answer calls, S = outstanding permit slots, A = AVAILABLE
agents, W = agents whose *system-enforced* wrap-up ends before the earliest
possible answer of a call dialled now (``guaranteed_min_answer_delay_s``).
A call dialled now cannot be answered before W agents are free, and older
calls are covered by A, so by induction every answer finds an agent. This
holds *regardless of the answer-rate estimate*, which is why STRICT is
deterministic. The only thing that can break it is an agent disappearing
involuntarily. ``find_capacity_deficit`` detects that and cancels the excess
ringing calls.

BOUNDED_RISK mode (operator opt-in, off by default)
---------------------------------------------------
Allows over-dial up to the largest D with P(Binomial(D, p_hi) > capacity) <= epsilon,
where p_hi is a pessimistic upper-confidence answer rate. Even then it falls
back to progressive whenever the estimate is not trustworthy.
"""
from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass
from typing import Optional

from app.clock import Clock
from app.config import PacingConfig, SafetyConfig
from app.database.connection import Database
from app.logging_setup import get_logger, log_event
from app.models.call import Binding
from app.models.safety import (
    CapacitySnapshot,
    Decision,
    DialPermit,
    PacingRecommendation,
    ProviderHealthLevel,
    SafetyDecision,
    SafetyMode,
)
from app.providers.circuit_breaker import CircuitBreaker

log = get_logger("safety_controller")

_ACTIVE = "('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED')"
_PRE_ANSWER = "('QUEUED','RESERVED','INITIATED','RINGING')"


def binomial_tail_above(n: int, p: float, c: int) -> float:
    """P(X > c) for X ~ Binomial(n, p), computed in log space for stability."""
    if c >= n:
        return 0.0
    if c < 0:
        return 1.0
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    lp, lq = math.log(p), math.log1p(-p)
    lgn = math.lgamma(n + 1)
    cdf = 0.0
    for k in range(0, c + 1):
        cdf += math.exp(lgn - math.lgamma(k + 1) - math.lgamma(n - k + 1) + k * lp + (n - k) * lq)
    return max(0.0, 1.0 - cdf)


def max_dials_within_risk(capacity: int, p: float, epsilon: float, hard_cap: int) -> int:
    """Largest D <= hard_cap with P(Binomial(D, p) > capacity) <= epsilon (binary search).

    epsilon == 0 degenerates to D == capacity, i.e. the deterministic bound.
    """
    capacity = max(0, capacity)
    if epsilon <= 0.0 or p >= 1.0:
        return min(capacity, hard_cap)
    lo, hi = min(capacity, hard_cap), hard_cap
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if binomial_tail_above(mid, p, capacity) <= epsilon:
            lo = mid
        else:
            hi = mid - 1
    return lo


@dataclass(frozen=True)
class _Room:
    strict_unbound: int
    bound: int
    bounded_risk_unbound: int
    deficit_bound: int


class UnknownSafetyMode(ValueError):
    pass


class SafetyController:
    def __init__(self, db: Database, clock: Clock, config: SafetyConfig, breaker: CircuitBreaker,
                 provider_names: list[str], pacing_config: PacingConfig) -> None:
        try:
            self._mode = SafetyMode(config.mode)
        except ValueError as exc:
            raise UnknownSafetyMode(f"unknown safety mode {config.mode!r}") from exc
        self._db = db
        self._clock = clock
        self._cfg = config
        self._breaker = breaker
        self._providers = list(provider_names)
        self._pacing_cfg = pacing_config

    @property
    def safety_mode(self) -> SafetyMode:
        return self._mode

    # ================================================================ evaluate
    def evaluate(self, rec: PacingRecommendation) -> SafetyDecision:
        now = self._clock.now()
        requested = max(0, int(rec.requested))
        with self._db.transaction() as conn:
            campaign = conn.execute("SELECT * FROM campaigns WHERE id=?", (rec.campaign_id,)).fetchone()
            snap, room = self._capacity(conn, rec.campaign_id, now)
            binding = rec.binding
            fallback_reason: Optional[str] = None
            limits: dict[str, int] = {}

            if campaign is None or campaign["status"] != "RUNNING":
                decision, approved, reason = Decision.REJECTED, 0, "campaign is not RUNNING"
            elif requested == 0:
                decision, approved, reason = Decision.REJECTED, 0, "nothing requested"
            elif snap.provider_health == ProviderHealthLevel.DOWN.value:
                decision, approved, reason = (Decision.REJECTED, 0,
                                              "no telecom provider is accepting calls (all circuits OPEN)")
            elif snap.unbound_in_flight > room.deficit_bound:
                decision, approved, reason = (Decision.REJECTED, 0,
                                              f"capacity deficit: {snap.unbound_in_flight} pooled calls in flight "
                                              f"but only {room.deficit_bound} agents can take them")
            else:
                if binding == Binding.UNBOUND:
                    fallback_reason = self._fallback_reason(conn, rec.campaign_id, snap, now)
                    if fallback_reason:
                        binding = Binding.BOUND
                if binding == Binding.BOUND:
                    limits["agent_capacity"] = room.bound
                elif self._mode == SafetyMode.STRICT:
                    limits["agent_capacity"] = room.strict_unbound
                else:
                    limits["risk_bounded_capacity"] = room.bounded_risk_unbound
                limits["global_outstanding_limit"] = self._cfg.max_outstanding_calls - snap.active_calls
                limits["campaign_concurrency_limit"] = campaign["max_concurrent_calls"] - snap.campaign_active_calls
                limits["eligible_borrowers"] = snap.eligible_borrowers
                if not self._any_closed_provider():
                    limits["half_open_probe_limit"] = self._breaker.cfg.half_open_max_probes
                allowed = max(0, min(limits.values()))
                approved = min(requested, allowed)
                limiter = min(limits, key=lambda k: limits[k])
                if fallback_reason:
                    decision = Decision.FALLBACK_TO_PROGRESSIVE if approved > 0 else Decision.REJECTED
                    reason = (f"fallback to progressive: {fallback_reason}; agent-bound capacity "
                              f"allows {approved}")
                elif approved == requested:
                    decision, reason = Decision.APPROVED, f"within safe capacity (tightest limit: {limiter}={limits[limiter]})"
                elif approved > 0:
                    decision, reason = Decision.REDUCED, f"reduced to {approved}: limited by {limiter}={limits[limiter]}"
                else:
                    decision, reason = Decision.REJECTED, f"no safe capacity: {limiter}={limits[limiter]}"

            rejected = requested - approved
            cur = conn.execute(
                """INSERT INTO safety_decisions (created_at, campaign_id, engine, safety_mode, binding, requested,
                       approved, rejected, decision, reason, capacity_json, explanation_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (now, rec.campaign_id, rec.engine, self._mode.value, binding.value, requested, approved,
                 rejected, decision.value, reason, json.dumps({**snap.as_dict(), "limits": limits}),
                 json.dumps(list(rec.explanation))),
            )
            decision_id = cur.lastrowid
            permit = None
            if approved > 0:
                permit = DialPermit(uuid.uuid4().hex, rec.campaign_id, approved, binding, now,
                                    now + self._cfg.permit_ttl_s)
                conn.execute(
                    """INSERT INTO dial_permits (id, campaign_id, decision_id, binding, approved, status,
                           issued_at, expires_at) VALUES (?,?,?,?,?, 'ACTIVE', ?,?)""",
                    (permit.permit_id, rec.campaign_id, decision_id, binding.value, approved, now,
                     permit.expires_at),
                )

        result = SafetyDecision(decision_id, rec.campaign_id, rec.engine, requested, approved, rejected,
                                decision, reason, self._mode, binding, snap, permit)
        log_event(log, "safety_decision", at=round(now, 3), campaign_id=rec.campaign_id, engine=rec.engine,
                  requested=requested, approved=approved, rejected=rejected, decision=decision.value,
                  reason=reason, safety_mode=self._mode.value, binding=binding.value,
                  available=snap.available_agents, unbound_in_flight=snap.unbound_in_flight,
                  limits=limits)
        return result

    # ============================================================== capacity
    def _capacity(self, conn, campaign_id: str, now: float) -> tuple[CapacitySnapshot, _Room]:
        def q(sql: str, params=()) -> int:
            return conn.execute(sql, params).fetchone()[0] or 0

        lookahead = self._cfg.guaranteed_min_answer_delay_s if self._cfg.enable_wrapup_lookahead else -1e18
        avail = q("SELECT COUNT(*) FROM agents WHERE status='AVAILABLE' AND pause_requested=0")
        avail_pause = q("SELECT COUNT(*) FROM agents WHERE status='AVAILABLE' AND pause_requested=1")
        wrap_look = q("SELECT COUNT(*) FROM agents WHERE status='WRAP_UP' AND pause_requested=0 "
                      "AND wrap_up_until <= ?", (now + lookahead,))
        # Pause-pending agents finishing wrap-up stay claimable by pooled calls (their
        # pause is applied only when headroom allows), but never back *new* calls.
        wrap_look_pause = q("SELECT COUNT(*) FROM agents WHERE status='WRAP_UP' AND pause_requested=1 "
                            "AND wrap_up_until <= ?", (now + lookahead,))
        reserved = q("SELECT COUNT(*) FROM agents WHERE status='RESERVED'")
        dialing = q("SELECT COUNT(*) FROM agents WHERE status='DIALING'")
        connected = q("SELECT COUNT(*) FROM agents WHERE status='CONNECTED'")
        logged_in = q("SELECT COUNT(*) FROM agents WHERE status NOT IN ('OFFLINE','PAUSED')")
        unbound = q(f"SELECT COUNT(*) FROM calls WHERE binding='UNBOUND' AND status IN {_PRE_ANSWER}")
        slots = {r[0]: r[1] for r in conn.execute(
            "SELECT binding, COALESCE(SUM(approved-consumed),0) FROM dial_permits "
            "WHERE status='ACTIVE' AND expires_at > ? GROUP BY binding", (now,)).fetchall()}
        s_unbound, s_bound = slots.get("UNBOUND", 0), slots.get("BOUND", 0)
        camp_slots = q("SELECT COALESCE(SUM(approved-consumed),0) FROM dial_permits "
                       "WHERE status='ACTIVE' AND expires_at > ? AND campaign_id=?", (now, campaign_id))
        # Granted-but-unspent permit slots are future calls: they count against every
        # outstanding-call cap, exactly like calls that already exist. Otherwise two
        # workers evaluating back to back both see the full headroom.
        active = q(f"SELECT COUNT(*) FROM calls WHERE status IN {_ACTIVE}") + s_unbound + s_bound
        camp_active = q(f"SELECT COUNT(*) FROM calls WHERE campaign_id=? AND status IN {_ACTIVE}",
                        (campaign_id,)) + camp_slots
        max_attempts = q("SELECT max_attempts FROM campaigns WHERE id=?", (campaign_id,)) or 1
        eligible = q("SELECT COUNT(*) FROM borrowers WHERE campaign_id=? AND status IN ('PENDING','RETRY_WAIT') "
                     "AND next_eligible_at <= ? AND attempt_count < ?", (campaign_id, now, max_attempts))
        eligible = max(0, eligible - camp_slots)   # each unspent slot will take one of them
        stats = conn.execute("SELECT answer_rate_ewma, answer_rate_fast, answer_samples FROM pacing_stats "
                             "WHERE campaign_id=?", (campaign_id,)).fetchone()
        p_slow = stats[0] if stats else self._pacing_cfg.initial_answer_rate
        p_fast = stats[1] if stats else p_slow
        samples = stats[2] if stats else 0
        p_hi = self._answer_rate_upper(max(p_slow, p_fast), samples)
        health = self._breaker.overall_health(self._providers)

        committed = unbound + s_unbound + s_bound
        # Look-ahead agents may back new calls only while existing commitments are
        # fully covered by agents that are idle right now (see module docstring).
        strict_room = (avail - committed + wrap_look) if committed <= avail else 0
        strict_room = max(0, strict_room - self._cfg.capacity_reserve_agents)
        bound_room = max(0, min(avail - s_bound, strict_room))
        claimable = avail + avail_pause + wrap_look + wrap_look_pause
        pool_for_risk = max(0, avail + wrap_look - s_bound - self._cfg.capacity_reserve_agents)
        hard_cap = max(0, self._cfg.max_outstanding_calls)
        if self._mode == SafetyMode.BOUNDED_RISK:
            max_unbound = max_dials_within_risk(pool_for_risk, p_hi, self._cfg.abandon_risk_epsilon, hard_cap)
            risk_room = max(0, max_unbound - unbound - s_unbound)
            # Hysteresis: the risk budget is spent at *admission* (pessimistic p_hi,
            # epsilon). Each answer consumes one agent but lowers the allowed
            # in-flight count by about 1/p, so re-applying the admission bound to
            # calls already ringing would cancel them constantly. Ringing calls are
            # only cancelled when *expected* answers exceed claimable agents
            # (median bound). That happens after real agent loss.
            p_point = min(1.0, max(p_slow, 0.01))
            deficit_bound = max_dials_within_risk(claimable, p_point, 0.5, hard_cap)
        else:
            max_unbound = unbound + s_unbound + strict_room
            risk_room = strict_room
            deficit_bound = claimable

        snap = CapacitySnapshot(
            available_agents=avail, pause_pending_agents=avail_pause + wrap_look_pause, wrapup_lookahead_agents=wrap_look,
            claimable_capacity=claimable, reserved_agents=reserved, dialing_agents=dialing,
            connected_agents=connected, logged_in_agents=logged_in, unbound_in_flight=unbound,
            outstanding_permit_slots=s_unbound + s_bound, active_calls=active,
            campaign_active_calls=camp_active, eligible_borrowers=eligible, provider_health=health.value,
            answer_rate_upper=round(p_hi, 4), answer_samples=samples, max_unbound_in_flight=max_unbound,
        )
        return snap, _Room(strict_room, bound_room, risk_room, deficit_bound)

    def _answer_rate_upper(self, p: float, samples: int) -> float:
        """Pessimistic (upper) confidence bound on the answer rate.

        Effective sample size of an EWMA is about (2 - alpha) / alpha, so old data
        never makes us over-confident."""
        alpha = self._pacing_cfg.ewma_alpha
        n_eff = max(1.0, min(float(samples), (2.0 - alpha) / alpha))
        p = min(max(p, 0.0), 1.0)
        return min(1.0, p + self._cfg.answer_rate_confidence_z * math.sqrt(max(p * (1 - p), 1e-6) / n_eff))

    def _fallback_reason(self, conn, campaign_id: str, snap: CapacitySnapshot, now: float) -> Optional[str]:
        """Conditions under which pooled dialing is not justified -> agent-bound dialing."""
        if snap.provider_health == ProviderHealthLevel.DEGRADED.value:
            return "provider health DEGRADED (ambiguous outcomes more likely)"
        drop = self._availability_drop(conn, now)
        if drop is not None:
            return drop
        if self._mode == SafetyMode.BOUNDED_RISK:
            if snap.answer_samples < self._cfg.min_samples_for_overdial:
                return (f"only {snap.answer_samples} answer-rate samples "
                        f"(< {self._cfg.min_samples_for_overdial}); estimate not trustworthy")
            stats = conn.execute("SELECT answer_rate_ewma, answer_rate_fast FROM pacing_stats "
                                 "WHERE campaign_id=?", (campaign_id,)).fetchone()
            if stats is not None:
                slow, fast = stats[0], stats[1]
                if abs(fast - slow) / max(slow, 0.05) > 0.5:
                    return f"answer rate unstable (slow EWMA {slow:.2f} vs fast EWMA {fast:.2f})"
        return None

    def _availability_drop(self, conn, now: float) -> Optional[str]:
        window = self._cfg.availability_drop_window_s
        peak = conn.execute("SELECT MAX(logged_in) FROM agent_availability_samples WHERE at >= ?",
                            (now - window,)).fetchone()[0]
        current = conn.execute("SELECT COUNT(*) FROM agents WHERE status NOT IN ('OFFLINE','PAUSED')").fetchone()[0]
        if peak and peak > 0 and (peak - current) / peak >= self._cfg.availability_drop_threshold:
            return f"agent availability dropped {peak} -> {current} within {window:.0f}s"
        return None

    def _any_closed_provider(self) -> bool:
        return any(self._breaker.is_closed(p) for p in self._providers)

    # ====================================================== agent observation
    def observe_agents(self) -> int:
        """Record the logged-in agent count. Used to detect sudden drops."""
        now = self._clock.now()
        with self._db.transaction() as conn:
            logged_in = conn.execute(
                "SELECT COUNT(*) FROM agents WHERE status NOT IN ('OFFLINE','PAUSED')").fetchone()[0]
            conn.execute("INSERT INTO agent_availability_samples (at, logged_in) VALUES (?,?)", (now, logged_in))
            conn.execute("DELETE FROM agent_availability_samples WHERE at < ?",
                         (now - 10 * self._cfg.availability_drop_window_s,))
        return logged_in

    # ===================================================== deficit enforcement
    def find_capacity_deficit(self) -> list[str]:
        """If agents vanished, pooled in-flight calls may exceed claimable agents.

        Marks the excess calls (newest, least advanced first) as cancel-requested
        and revokes outstanding permits. Returns the call ids the allocator must
        hang up. Cancel-requested calls stay counted until the provider confirms
        they ended unanswered. If one is answered first, it is still handled
        normally.
        """
        now = self._clock.now()
        with self._db.transaction() as conn:
            campaign_id = self._reference_campaign(conn)
            if campaign_id is None:
                return []
            snap, room = self._capacity(conn, campaign_id, now)
            cancelling = conn.execute(
                f"SELECT COUNT(*) FROM calls WHERE binding='UNBOUND' AND status IN {_PRE_ANSWER} "
                "AND cancel_requested_at IS NOT NULL").fetchone()[0]
            excess = snap.unbound_in_flight - room.deficit_bound - cancelling
            if excess <= 0:
                return []
            victims = [r[0] for r in conn.execute(
                f"""SELECT id FROM calls WHERE binding='UNBOUND' AND status IN {_PRE_ANSWER}
                       AND cancel_requested_at IS NULL
                    ORDER BY CASE status WHEN 'QUEUED' THEN 0 WHEN 'RESERVED' THEN 1
                                         WHEN 'INITIATED' THEN 2 ELSE 3 END, created_at DESC
                    LIMIT ?""", (excess,)).fetchall()]
            for call_id in victims:
                conn.execute("UPDATE calls SET cancel_requested_at=?, cancel_reason='CANCELLED_CAPACITY', "
                             "updated_at=? WHERE id=? AND cancel_requested_at IS NULL", (now, now, call_id))
            revoked = conn.execute("UPDATE dial_permits SET status='REVOKED' WHERE status='ACTIVE'").rowcount
        log_event(log, "capacity_deficit", at=round(now, 3), unbound_in_flight=snap.unbound_in_flight,
                  claimable=room.deficit_bound, cancelling=len(victims), permits_revoked=revoked)
        return victims

    def pooled_headroom(self) -> int:
        """How many AVAILABLE agents could leave voluntarily without breaking a commitment."""
        now = self._clock.now()
        with self._db.transaction() as conn:
            campaign_id = self._reference_campaign(conn)
            if campaign_id is None:
                return 10 ** 9
            snap, _ = self._capacity(conn, campaign_id, now)
        committed = snap.unbound_in_flight + snap.outstanding_permit_slots
        return snap.claimable_capacity - snap.pause_pending_agents - committed

    @staticmethod
    def _reference_campaign(conn) -> Optional[str]:
        """Agents form one shared pool. Pool-level checks use the running campaign's
        answer-rate statistics (prototype assumption: one active campaign per pool)."""
        row = conn.execute("SELECT id FROM campaigns ORDER BY (status='RUNNING') DESC, created_at LIMIT 1").fetchone()
        return row[0] if row else None
