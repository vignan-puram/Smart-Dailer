"""Matplotlib charts comparing dialing modes (from real simulation results)."""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from app.simulation.metrics import SimulationResult  # noqa: E402

MODE_COLORS = {"PROGRESSIVE": "#4C72B0", "PREDICTIVE/STRICT": "#55A868", "PREDICTIVE/BOUNDED_RISK": "#C44E52"}


def label(r: SimulationResult) -> str:
    return "PROGRESSIVE" if r.mode == "PROGRESSIVE" else f"PREDICTIVE/{r.safety_mode}"


def _grouped_bars(results: list[SimulationResult], metric: str, ylabel: str, title: str, path: Path) -> None:
    scenarios = sorted({r.scenario for r in results})
    labels = [m for m in MODE_COLORS if any(label(r) == m for r in results)]
    width = 0.8 / max(1, len(labels))
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for i, lab in enumerate(labels):
        vals = []
        for s in scenarios:
            match = [r for r in results if r.scenario == s and label(r) == lab]
            vals.append(getattr(match[0], metric) if match else 0)
        xs = [x + i * width for x in range(len(scenarios))]
        bars = ax.bar(xs, vals, width, label=lab, color=MODE_COLORS[lab])
        for b, v in zip(bars, vals):
            ax.annotate(f"{v:g}", (b.get_x() + b.get_width() / 2, b.get_height()), ha="center", va="bottom",
                        fontsize=8)
    ax.set_xticks([x + width * (len(labels) - 1) / 2 for x in range(len(scenarios))])
    ax.set_xticklabels([f"Scenario {s}" for s in scenarios])
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(frameon=False, fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _decisions(results: list[SimulationResult], path: Path) -> None:
    kinds = ["APPROVED", "REDUCED", "FALLBACK_TO_PROGRESSIVE", "REJECTED"]
    colors = ["#55A868", "#DD8452", "#8172B3", "#C44E52"]
    rows = sorted(results, key=lambda r: (r.scenario, list(MODE_COLORS).index(label(r))))
    names = [f"{r.scenario}:{label(r).replace('PREDICTIVE/', 'P/').replace('PROGRESSIVE', 'PROG')}" for r in rows]
    fig, ax = plt.subplots(figsize=(11, 4.5))
    bottom = [0] * len(rows)
    for kind, color in zip(kinds, colors):
        vals = [r.safety_decisions.get(kind, 0) for r in rows]
        ax.bar(names, vals, bottom=bottom, label=kind, color=color)
        bottom = [b + v for b, v in zip(bottom, vals)]
    ax.set_ylabel("Safety Controller decisions")
    ax.set_title("Safety Controller decisions per run (one evaluation per pacing tick with demand)")
    ax.legend(frameon=False, fontsize=8, ncol=4)
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _timeline(results: list[SimulationResult], scenario: str, path: Path) -> None:
    runs = [r for r in results if r.scenario == scenario and r.timeline]
    if not runs:
        return
    fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
    ref = runs[0].timeline
    t = [p.t / 60 for p in ref]
    axes[0].plot(t, [p.true_answer_rate for p in ref], "k--", label="true answer rate (world)")
    for r in runs:
        if r.mode == "PREDICTIVE":
            axes[0].plot([p.t / 60 for p in r.timeline], [p.answer_rate_ewma for p in r.timeline],
                         label=f"EWMA estimate ({label(r)})", color=MODE_COLORS[label(r)])
    axes[0].set_ylabel("answer rate")
    axes[0].legend(frameon=False, fontsize=8)
    for r in runs:
        axes[1].plot([p.t / 60 for p in r.timeline], [100 * p.utilisation_window for p in r.timeline],
                     label=label(r), color=MODE_COLORS[label(r)])
    axes[1].set_ylabel("utilisation % (per minute)")
    axes[1].legend(frameon=False, fontsize=8)
    for r in runs:
        if r.mode == "PREDICTIVE":
            axes[2].plot([p.t / 60 for p in r.timeline], [p.approved for p in r.timeline],
                         label=f"approved/min ({label(r)})", color=MODE_COLORS[label(r)])
    axes[2].plot(t, [p.logged_in for p in ref], ":", color="grey", label="logged-in agents")
    axes[2].set_ylabel("calls approved / min")
    axes[2].set_xlabel("simulated minutes")
    axes[2].legend(frameon=False, fontsize=8)
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(f"Scenario {scenario}: pacing under changing conditions")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def generate_charts(results: list[SimulationResult], out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / "utilisation_by_mode.png", out_dir / "connects_per_agent_hour.png",
             out_dir / "safety_decisions.png", out_dir / "scenario_D_timeline.png"]
    _grouped_bars(results, "agent_utilisation_pct", "agent utilisation (%)",
                  "Agent utilisation: time CONNECTED / logged-in working time", paths[0])
    _grouped_bars(results, "connects_per_agent_hour", "connected calls per agent-hour",
                  "Throughput: borrowers reached per agent-hour", paths[1])
    _decisions(results, paths[2])
    _timeline(results, "D", paths[3])
    return paths
