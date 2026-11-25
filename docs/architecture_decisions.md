# Architecture decision records

Each ADR records the context, the decision, the alternatives that were rejected, and the consequences.

---

## ADR-001: Modular monolith with SQLite, no message broker

**Context.** This is a prototype that has to be runnable with `pip install` and must still show real
concurrency safety. The hard problems (double allocation, idempotency, crash recovery) are about
*state*, not transport.

**Decision.** One codebase with layered modules. SQLite in WAL mode is the single source of truth.
Workers coordinate only through atomic SQL statements and constraints.

**Alternatives.** Kafka/Redis plus microservices. Rejected: they add operational weight, and they would
hide the correctness argument behind infrastructure. A broker still needs an idempotent consumer
and an atomic reservation store, which is exactly what is built here.

**Consequences.** + Trivial to run, deterministic tests, the correctness argument is readable SQL.
− SQLite has a single writer, which limits write throughput (measured in `results/load_test_results.md`).
Migration path: PostgreSQL (ADR-007).

---

## ADR-002: The Safety Controller is the only authority; pacing is advisory

**Context.** The assignment requires that an aggressive predictive engine can never cause unsafe
dialing.

**Decision.** Engines return a `PacingRecommendation` (a number plus an explanation). The
`SafetyController` recomputes capacity from the DB **with its own queries** inside one
`BEGIN IMMEDIATE` transaction and writes a `dial_permits` row. The `CallAllocator` dials only by
consuming permit slots with a conditional UPDATE. `CHECK(consumed <= approved)` makes overspending
impossible even for buggy code.

**Alternatives.** (a) The engine calls the allocator directly with a "max" guard. Rejected: the guard
lives in the same code path as the risk. (b) A capability object held only in memory. Rejected: it
can be forged or reused, and it does not survive across processes.

**Consequences.** + The no-bypass property is testable (`tests/test_safety.py::TestNoBypass`).
+ Every decision is auditable (`safety_decisions` stores the snapshot, the limits and the explanation).
− One extra DB round trip per cycle (sub-millisecond, see the load test).

---

## ADR-003: Two safety modes, STRICT by default

**Context.** "Zero abandoned calls" and "higher utilisation than progressive" pull in opposite
directions. Over-dialing beats progressive only by betting on the answer rate.

**Decision.**
* `STRICT` (default): pooled in-flight calls plus outstanding permits ≤ idle agents. Look-ahead agents
  (system-enforced wrap-up ends within the 3s minimum answer delay) may back *new* calls only. This
  is deterministic and independent of the answer-rate estimate.
* `BOUNDED_RISK` (opt-in, env `SMARTDIALER_SAFETY_MODE`): admit the largest D with
  `P(Binomial(D, p_hi) > capacity) ≤ ε`, where `p_hi` is an upper confidence bound (z = 2, EWMA
  effective sample size). It falls back to progressive with fewer than 200 samples, an unstable rate,
  a degraded provider, or a sudden drop in agents.

**Alternatives.** The classic Erlang-style predictive dialer with a target abandon rate and no hard
bound. Rejected as the default because it gives no guarantee.

**Consequences.** The simulation shows the trade-off honestly: STRICT predictive ≈ progressive
utilisation with 0 abandons, while BOUNDED_RISK gains utilisation at a measured, bounded risk (see the README).

---

## ADR-004: BOUND vs UNBOUND calls

**Decision.** Progressive and fallback calls are BOUND: the agent is reserved *before* dialing.
Predictive calls are UNBOUND: the agent is claimed atomically when ANSWERED arrives
(`AgentRepository.claim_for_answered_call`). If no agent can be claimed, the call is ABANDONED and
counted, never hidden.

**Consequences.** + Pooling is what makes predictive dialing useful at all. − It needs deficit
enforcement when agents disappear (`find_capacity_deficit`).

---

## ADR-005: Idempotent event inbox with rank-based ordering

**Decision.** Every webhook is first inserted into `provider_events` (`UNIQUE(provider, event_id)`)
**in the same transaction** as its state change. A duplicate returns `DUPLICATE` (and increments
`duplicate_count`). Transitions are accepted only if their rank goes up. Terminal states are protected.
A crash rolls back both the inbox row and the state change, so redelivery is applied exactly once.

**Alternatives.** Dedup in an in-memory LRU. Rejected: it is lost on restart and not shared across workers.

---

## ADR-006: Persisted circuit breaker with jittered exponential backoff

**Decision.** Breaker state lives in `provider_health`, so all workers agree. CLOSED→OPEN on 5
consecutive failures, or an EWMA failure rate (α = 0.1) ≥ 50% once at least 20 requests have been seen. The cooldown is `base·2^n` capped at
300s with ±20% jitter. HALF_OPEN grants exactly one probe through a CAS on the probe slot. An OPEN
circuit past `retry_at` reports DEGRADED so that a probe actually happens. Health maps to safety:
DOWN ⇒ REJECTED, DEGRADED ⇒ fallback to BOUND dialing.

---

## ADR-007: SQLite now, PostgreSQL in production

| Concern | SQLite (prototype) | PostgreSQL (production) |
|---|---|---|
| Atomic claim | conditional UPDATE + `RETURNING` | same, plus `FOR UPDATE SKIP LOCKED` for queues |
| Serialising `evaluate` | `BEGIN IMMEDIATE` (database-wide write lock) | `pg_advisory_xact_lock(pool_id)`: per-pool, not global |
| Writers | one at a time | many; row-level locks |
| Partial unique indexes | supported | supported |
| HA | none | streaming replica + failover |

The repository layer contains all SQL, so the port changes `app/database/*` only.

---

## ADR-008: Virtual clock and seeded randomness everywhere

**Decision.** Every component takes a `Clock`. The providers, the simulator, the breaker jitter and
borrower retry jitter all use seeded `random.Random` instances.

**Consequences.** + Bit-for-bit reproducible simulations
(`tests/test_simulation.py::test_same_seed_gives_identical_results`), and an 80-minute scenario runs in
under a minute. − Real wall-clock behaviour (GC pauses, network jitter) is modelled, not observed.

---

## ADR-009: Never release an agent on an ambiguous outcome

**Decision.** If `initiate_call` times out, or the worker crashes after the HTTP call, the agent and
borrower stay reserved until `RecoveryManager.reconcile_call` gets a definitive answer from the
provider (`get_call_status(client_ref)`). If the provider is unreachable, they stay reserved and
recovery retries.

**Trade-off.** An agent may sit idle for one setup lease (10s). The alternative would be dialing a
borrower twice, or connecting a live borrower to nobody.

---

## ADR-010: Predictive algorithm: transparent statistics, not ML

**Selected.** Two EWMAs of the answer rate (slow α = 0.05 for stability, fast α = 0.10 for change
detection). With fewer than 30 samples the engine assumes a **high** answer rate (60%), because a high
assumed rate means *fewer* dials. Expected free agents = idle + wrap-ups ending within the
dial-to-answer horizon + connected × min(1, H / talk). Target in-flight = expected_free / p,
capped at 4×. The result is scaled by provider health (1 / 0.5 / 0) and by ×0.7 when the rate is
unstable, minus calls already ringing and a safety margin of 1 (`predictive_engine.py`).

**Why.** Every number in the output is explainable to an operator (`PacingRecommendation.explain()`),
and the engine is cheap enough to run every second per campaign.

**Alternatives.** Erlang-A / queueing models (need stable arrival assumptions); ML forecasting
(needs data we do not have and is opaque). Either could replace this engine later without
touching safety, because the engine is only advisory.

**Limitation.** The memoryless talk-time assumption is crude. Real talk-time distributions are heavy-tailed.

---

## ADR-011: Provider abstraction

**Selected.** An abstract `TelecomProvider` with `initiate_call`, `hangup`, `get_call_status` and
an event sink (`base_provider.py`). Errors are split into `ProviderRejected` (definitive: safe to
release) and `ProviderTimeout` (ambiguous: must reconcile). Two mocks share one engine
(`mock_provider.py`) with different `ProviderProfile`s: A is fast and reliable, B is slow, flaky,
duplicating and reordering.

**Why.** The definitive-vs-ambiguous split is what lets recovery be correct. `client_ref` (our
call id) is passed to the provider so that reconciliation can look a call up even when we never
learned its `provider_call_id`.

**Limitation.** Real carriers have rate limits, regional routing and per-number reputation. These are not modelled.

---

## ADR-012: Retry strategy

| What | Strategy | Where |
|---|---|---|
| Borrower re-attempt after no answer/busy | exponential `300s·2^(n-1)` ±jitter, capped 1h, max 3 attempts | `borrower_manager.retry_delay`, `CallLifecycle.finish` |
| Setup failures / not placed | not counted as an attempt, retry after 30s | `CallLifecycle.finish` (`NOT_AN_ATTEMPT`) |
| Abandoned (answered, no agent) | retry after 60s, counted | `CallLifecycle.finish` |
| Provider after failures | circuit breaker backoff `10s·2^n` ±20%, cap 300s | `circuit_breaker.py` |
| Hangup that failed during outage | re-attempted by recovery each cycle | `RecoveryManager.run` |
| Initiate timeout | **no blind retry**: reconcile first (ADR-009) | `RecoveryManager.reconcile_call` |

**Why no automatic retry of `initiate_call`?** A timed-out initiate may have rung the borrower.
Retrying would risk two calls to one person. The partial unique index on active calls per borrower
would also reject the second insert.

---

## ADR-013: Scalability path

**Selected for the prototype.** Stateless workers plus one DB. Every coordination point is an atomic
statement, so adding workers is safe. The load test measures the single-writer ceiling.

**Path to 100k agents.**
1. PostgreSQL with row-level locking; `evaluate` serialised **per agent pool** with an advisory lock
   instead of the database-wide lock.
2. **Partition by agent pool / campaign group**: a pool's agents, calls and permits live in one
   shard, and each worker owns a set of pools (lease-based ownership table). Safety needs a
   consistent view only *within* a pool.
3. Webhook ingestion: a stateless HTTP tier writes to the inbox table (or a durable queue partitioned by
   `provider_call_id`), so events for one call are processed in order by one consumer.
4. Provider rate limits: a token bucket per provider and account in the router, fed to SafetyController as an
   additional limit.
5. Connection limits: PgBouncer in transaction mode. Transactions are short by design.

**Limitation.** Cross-pool agent sharing (one agent in two pools) would need a global
reservation service. Avoid it by assigning each agent to exactly one pool at login.
