"""Guided CLI demo of the SmartDialer (no server needed).

    python scripts/demo.py              # everything
    python scripts/demo.py pacing       # one section: pacing | safety | failures
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.logging_setup import configure_logging  # noqa: E402
from app.models.call import Binding  # noqa: E402
from app.models.campaign import DialMode  # noqa: E402
from app.models.safety import PacingRecommendation  # noqa: E402
from app.providers.mock_provider import StaticOutcomeModel  # noqa: E402
from app.runtime import build_runtime  # noqa: E402
from app.simulation.failure_scenarios import FAILURE_SCENARIOS  # noqa: E402


def banner(title: str) -> None:
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def pacing() -> None:
    banner("1. Progressive vs predictive pacing (50 agents, 30% answer rate, 10 simulated minutes)")
    for mode in (DialMode.PROGRESSIVE, DialMode.PREDICTIVE):
        rt = build_runtime(Settings(), db_path=":memory:", fast_db=True,
                           outcome_model=StaticOutcomeModel(0.30, 120))
        rt.campaigns.create("demo", "demo", mode)
        rt.agents.create_many(50)
        rt.borrowers.create_many("demo", 5000)
        rt.campaigns.start("demo")
        rt.advance(600)
        # Recommendation and decision from the same (last) pacing cycle.
        print(f"\n--- {mode.value} (last pacing cycle at t={rt.clock.now():.0f}s) ---")
        for r in rt.last_cycle:
            print(r.recommendation.explain())
            if r.decision is None:
                print("Safety Controller: not consulted (nothing requested)")
            else:
                d = r.decision
                print(f"Safety Controller: {d.decision.value} {d.approved}/{d.requested} - {d.reason}")
        print("agents:", rt.metrics.agent_counts())
        print("outcomes:", rt.lifecycle.calls.counts_by_outcome())


def safety() -> None:
    banner("2. The predictive engine asks for 100 calls; the Safety Controller decides")
    rt = build_runtime(Settings(), db_path=":memory:", fast_db=True)
    rt.campaigns.create("demo", "demo", DialMode.PREDICTIVE)
    rt.agents.create_many(10)
    rt.borrowers.create_many("demo", 1000)
    rt.campaigns.start("demo")
    rec = PacingRecommendation("demo", "PREDICTIVE", 100, Binding.UNBOUND, ("deliberately excessive request",))
    d = rt.safety.evaluate(rec)
    print(json.dumps({k: v for k, v in d.as_dict().items() if k != "capacity"}, indent=1))
    print("capacity seen by the controller:", json.dumps(d.capacity.as_dict()))
    res = rt.allocator.execute(d.permit)
    print(f"allocator used {len(res.call_ids)} of {d.approved} permit slots: {res.placed} placed, "
          f"{res.setup_failed} setup failed, {res.ambiguous} ambiguous (provider timeout)")
    print("a second execute of the same permit -> ", end="")
    try:
        rt.allocator.execute(d.permit)
    except PermissionError as exc:
        print(f"refused: {exc}")


def failures() -> None:
    for name, fn in FAILURE_SCENARIOS.items():
        banner(f"3. Failure scenario: {name}")
        report = fn()
        if "timeline" in report and len(report["timeline"]) > 14:
            report["timeline"] = report["timeline"][::4]
        print(json.dumps(report, indent=1, default=str))


def main() -> None:
    configure_logging("WARNING")
    sections = {"pacing": pacing, "safety": safety, "failures": failures}
    chosen = sys.argv[1:] or list(sections)
    for name in chosen:
        sections[name]()


if __name__ == "__main__":
    main()
