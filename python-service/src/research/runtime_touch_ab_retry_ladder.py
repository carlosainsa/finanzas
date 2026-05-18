import argparse
import json
import shlex
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.research.execution_probe_universe_selection import (
    ExecutionProbeUniverseConfig,
    create_execution_probe_universe_selection,
)


REPORT_VERSION = "runtime_touch_ab_retry_ladder_v1"


@dataclass(frozen=True)
class RuntimeTouchAbRetryAttempt:
    label: str
    universe_limit: int
    min_runtime_signalable_density: float
    min_runtime_signalable_snapshots: int
    runtime_touch_hybrid_backfill: bool = False


@dataclass(frozen=True)
class RuntimeTouchAbRetryLadderConfig:
    profile_a: str = "execution_probe_v11"
    profile_b: str = "execution_probe_v12"
    min_assets: int = 2
    observation_seconds: int = 3600
    fresh_capture_seconds: int = 1800
    runtime_touch_lookback_ms: int = 900_000
    min_runtime_touch_change_rate: float = 0.01
    min_runtime_touch_snapshots: int = 10
    min_runtime_active_minutes: int = 2
    runtime_signal_min_spread: float = 0.03
    runtime_signal_min_depth: float = 1.5
    runtime_recent_signalable_window_ms: int = 180_000
    runtime_touch_freshness_ordering: str = "freshest_first"
    min_avg_opportunity_spread: float | None = 0.000625
    max_avg_opportunity_spread: float | None = None
    attempts: tuple[RuntimeTouchAbRetryAttempt, ...] = (
        RuntimeTouchAbRetryAttempt(
            label="strict_signalable",
            universe_limit=10,
            min_runtime_signalable_density=0.05,
            min_runtime_signalable_snapshots=3,
        ),
        RuntimeTouchAbRetryAttempt(
            label="wider_universe",
            universe_limit=20,
            min_runtime_signalable_density=0.05,
            min_runtime_signalable_snapshots=3,
        ),
        RuntimeTouchAbRetryAttempt(
            label="lower_signalable_density",
            universe_limit=20,
            min_runtime_signalable_density=0.02,
            min_runtime_signalable_snapshots=3,
        ),
        RuntimeTouchAbRetryAttempt(
            label="lower_signalable_snapshots",
            universe_limit=20,
            min_runtime_signalable_density=0.02,
            min_runtime_signalable_snapshots=1,
        ),
        RuntimeTouchAbRetryAttempt(
            label="runtime_hybrid_backfill",
            universe_limit=20,
            min_runtime_signalable_density=0.02,
            min_runtime_signalable_snapshots=3,
            runtime_touch_hybrid_backfill=True,
        ),
    )
    runtime_hybrid_min_signalable_snapshots: int = 1
    runtime_hybrid_min_signalable_density: float = 0.0
    runtime_hybrid_min_liquidity: float = 0.0

    def __post_init__(self) -> None:
        if self.profile_a not in {"execution_probe_v11", "execution_probe_v12"}:
            raise ValueError(
                "profile_a must be execution_probe_v11 or execution_probe_v12"
            )
        if self.profile_b not in {"execution_probe_v11", "execution_probe_v12"}:
            raise ValueError(
                "profile_b must be execution_probe_v11 or execution_probe_v12"
            )
        if self.profile_a == self.profile_b:
            raise ValueError("profile_a and profile_b must be different")
        if self.min_assets < 2:
            raise ValueError("min_assets must be >= 2 for A/B observations")
        if self.observation_seconds < 1800 or self.observation_seconds > 5400:
            raise ValueError("observation_seconds must be between 1800 and 5400")
        if self.fresh_capture_seconds < 1800 or self.fresh_capture_seconds > 5400:
            raise ValueError("fresh_capture_seconds must be between 1800 and 5400")
        if self.min_runtime_touch_snapshots <= 0:
            raise ValueError("min_runtime_touch_snapshots must be positive")
        if self.min_runtime_active_minutes <= 0:
            raise ValueError("min_runtime_active_minutes must be positive")
        if self.runtime_recent_signalable_window_ms <= 0:
            raise ValueError("runtime_recent_signalable_window_ms must be positive")
        if self.runtime_touch_freshness_ordering not in {
            "score_first",
            "freshest_first",
        }:
            raise ValueError(
                "runtime_touch_freshness_ordering must be score_first or freshest_first"
            )
        if not self.attempts:
            raise ValueError("attempts cannot be empty")
        for attempt in self.attempts:
            if attempt.universe_limit < self.min_assets:
                raise ValueError("attempt universe_limit must be >= min_assets")
            if not 0 <= attempt.min_runtime_signalable_density <= 1:
                raise ValueError(
                    "attempt min_runtime_signalable_density must be between 0 and 1"
                )
            if attempt.min_runtime_signalable_snapshots < 0:
                raise ValueError(
                    "attempt min_runtime_signalable_snapshots must be non-negative"
                )
        if self.runtime_hybrid_min_signalable_snapshots < 0:
            raise ValueError(
                "runtime_hybrid_min_signalable_snapshots must be non-negative"
            )
        if not 0 <= self.runtime_hybrid_min_signalable_density <= 1:
            raise ValueError(
                "runtime_hybrid_min_signalable_density must be between 0 and 1"
            )
        if self.runtime_hybrid_min_liquidity < 0:
            raise ValueError("runtime_hybrid_min_liquidity must be non-negative")


def create_runtime_touch_ab_retry_ladder(
    fresh_duckdb: Path,
    output_dir: Path,
    fresh_report_root: Path | None = None,
    config: RuntimeTouchAbRetryLadderConfig = RuntimeTouchAbRetryLadderConfig(),
) -> dict[str, Any]:
    if not fresh_duckdb.exists():
        raise FileNotFoundError(f"fresh DuckDB does not exist: {fresh_duckdb}")
    output_dir.mkdir(parents=True, exist_ok=True)
    attempts: list[dict[str, Any]] = []
    selected_attempt: dict[str, Any] | None = None
    for attempt in config.attempts:
        attempt_dir = output_dir / "attempts" / attempt.label
        selection = create_execution_probe_universe_selection(
            fresh_duckdb,
            attempt_dir,
            ExecutionProbeUniverseConfig(
                profile=config.profile_a,
                limit=attempt.universe_limit,
                min_assets=config.min_assets,
                selection_source="runtime_touch",
                runtime_touch_lookback_ms=config.runtime_touch_lookback_ms,
                min_runtime_touch_change_rate=config.min_runtime_touch_change_rate,
                min_runtime_touch_snapshots=config.min_runtime_touch_snapshots,
                min_runtime_active_minutes=config.min_runtime_active_minutes,
                min_runtime_signalable_snapshots=attempt.min_runtime_signalable_snapshots,
                min_runtime_signalable_density=attempt.min_runtime_signalable_density,
                runtime_signal_min_spread=config.runtime_signal_min_spread,
                runtime_signal_min_depth=config.runtime_signal_min_depth,
                runtime_recent_signalable_window_ms=(
                    config.runtime_recent_signalable_window_ms
                ),
                runtime_touch_freshness_ordering=(
                    config.runtime_touch_freshness_ordering
                ),
                runtime_touch_hybrid_backfill=attempt.runtime_touch_hybrid_backfill,
                runtime_hybrid_min_signalable_snapshots=(
                    config.runtime_hybrid_min_signalable_snapshots
                ),
                runtime_hybrid_min_signalable_density=(
                    config.runtime_hybrid_min_signalable_density
                ),
                runtime_hybrid_min_liquidity=config.runtime_hybrid_min_liquidity,
                min_avg_opportunity_spread=config.min_avg_opportunity_spread,
                max_avg_opportunity_spread=config.max_avg_opportunity_spread,
            ),
        )
        attempt_payload = {
            "label": attempt.label,
            "status": selection["status"],
            "config": asdict(attempt),
            "market_asset_ids_count": selection["market_asset_ids_count"],
            "market_asset_ids_sha256": selection["market_asset_ids_sha256"],
            "market_asset_ids": selection["market_asset_ids"],
            "selection_reason": selection["selection_reason"],
            "universe_selection_path": str(
                attempt_dir / "execution_probe_universe_selection.json"
            ),
        }
        attempts.append(attempt_payload)
        if selected_attempt is None and selection["status"] == "ready":
            selected_attempt = attempt_payload

    recommendation = (
        "RUN_RUNTIME_TOUCH_AB_WITH_SELECTED_ATTEMPT"
        if selected_attempt is not None
        else "COLLECT_FRESH_RUNTIME_SAMPLE_OR_CHANGE_MARKET_TIMING"
    )
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "decision_policy": "offline_runtime_touch_ab_retry_ladder_only",
        "fresh_duckdb": str(fresh_duckdb),
        "fresh_report_root": str(fresh_report_root) if fresh_report_root else None,
        "config": _config_payload(config),
        "attempts": attempts,
        "selected_attempt": selected_attempt,
        "recommendation": recommendation,
        "next_command": build_next_command(
            fresh_duckdb,
            fresh_report_root,
            selected_attempt,
            config,
        )
        if selected_attempt is not None
        else None,
        "next_run": build_next_run(
            fresh_duckdb,
            fresh_report_root,
            selected_attempt,
            config,
        )
        if selected_attempt is not None
        else None,
        "outputs": {
            "report": str(output_dir / "runtime_touch_ab_retry_ladder.json"),
            "attempts_dir": str(output_dir / "attempts"),
        },
    }
    (output_dir / "runtime_touch_ab_retry_ladder.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def build_next_command(
    fresh_duckdb: Path,
    fresh_report_root: Path | None,
    selected_attempt: dict[str, Any],
    config: RuntimeTouchAbRetryLadderConfig,
) -> str:
    attempt_config = selected_attempt["config"]
    env = {
        "EXECUTION_PROBE_UNIVERSE_LIMIT": attempt_config["universe_limit"],
        "EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_DENSITY": attempt_config[
            "min_runtime_signalable_density"
        ],
        "EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_SNAPSHOTS": attempt_config[
            "min_runtime_signalable_snapshots"
        ],
        "EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_SPREAD": config.runtime_signal_min_spread,
        "EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_DEPTH": config.runtime_signal_min_depth,
        "EXECUTION_PROBE_RECENT_SIGNALABLE_WINDOW_MS": (
            config.runtime_recent_signalable_window_ms
        ),
        "EXECUTION_PROBE_RUNTIME_TOUCH_FRESHNESS_ORDERING": (
            config.runtime_touch_freshness_ordering
        ),
    }
    if attempt_config["runtime_touch_hybrid_backfill"]:
        env.update(
            {
                "EXECUTION_PROBE_RUNTIME_TOUCH_HYBRID_BACKFILL": "1",
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_SIGNALABLE_SNAPSHOTS": (
                    config.runtime_hybrid_min_signalable_snapshots
                ),
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_SIGNALABLE_DENSITY": (
                    config.runtime_hybrid_min_signalable_density
                ),
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_LIQUIDITY": (
                    config.runtime_hybrid_min_liquidity
                ),
            }
        )
    command = [
        "scripts/run_runtime_touch_ab_cycle.sh",
        "--skip-fresh-capture",
        "--fresh-duckdb",
        str(fresh_duckdb),
        "--duration-seconds",
        str(config.observation_seconds),
        "--profile-a",
        config.profile_a,
        "--profile-b",
        config.profile_b,
        "--universe-limit",
        str(attempt_config["universe_limit"]),
        "--min-assets",
        str(config.min_assets),
        "--freshness-ordering",
        config.runtime_touch_freshness_ordering,
    ]
    if fresh_report_root is not None:
        command.extend(["--fresh-report-root", str(fresh_report_root)])
    if attempt_config["runtime_touch_hybrid_backfill"]:
        command.append("--runtime-touch-hybrid-backfill")
    env_prefix = " ".join(
        f"{key}={shlex.quote(str(value))}" for key, value in env.items()
    )
    return f"{env_prefix} {' '.join(shlex.quote(part) for part in command)}"


def build_next_run(
    fresh_duckdb: Path,
    fresh_report_root: Path | None,
    selected_attempt: dict[str, Any],
    config: RuntimeTouchAbRetryLadderConfig,
) -> dict[str, Any]:
    attempt_config = selected_attempt["config"]
    env = {
        "EXECUTION_PROBE_UNIVERSE_LIMIT": attempt_config["universe_limit"],
        "EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_DENSITY": attempt_config[
            "min_runtime_signalable_density"
        ],
        "EXECUTION_PROBE_MIN_RUNTIME_SIGNALABLE_SNAPSHOTS": attempt_config[
            "min_runtime_signalable_snapshots"
        ],
        "EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_SPREAD": config.runtime_signal_min_spread,
        "EXECUTION_PROBE_RUNTIME_SIGNAL_MIN_DEPTH": config.runtime_signal_min_depth,
        "EXECUTION_PROBE_RECENT_SIGNALABLE_WINDOW_MS": (
            config.runtime_recent_signalable_window_ms
        ),
        "EXECUTION_PROBE_RUNTIME_TOUCH_FRESHNESS_ORDERING": (
            config.runtime_touch_freshness_ordering
        ),
    }
    if attempt_config["runtime_touch_hybrid_backfill"]:
        env.update(
            {
                "EXECUTION_PROBE_RUNTIME_TOUCH_HYBRID_BACKFILL": "1",
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_SIGNALABLE_SNAPSHOTS": (
                    config.runtime_hybrid_min_signalable_snapshots
                ),
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_SIGNALABLE_DENSITY": (
                    config.runtime_hybrid_min_signalable_density
                ),
                "EXECUTION_PROBE_RUNTIME_HYBRID_MIN_LIQUIDITY": (
                    config.runtime_hybrid_min_liquidity
                ),
            }
        )
    return {
        "script": "scripts/run_runtime_touch_ab_cycle.sh",
        "env": env,
        "args": [
            "--skip-fresh-capture",
            "--fresh-duckdb",
            str(fresh_duckdb),
            *(
                ["--fresh-report-root", str(fresh_report_root)]
                if fresh_report_root is not None
                else []
            ),
            "--duration-seconds",
            str(config.observation_seconds),
            "--profile-a",
            config.profile_a,
            "--profile-b",
            config.profile_b,
            "--universe-limit",
            str(attempt_config["universe_limit"]),
            "--min-assets",
            str(config.min_assets),
            "--freshness-ordering",
            config.runtime_touch_freshness_ordering,
            *(
                ["--runtime-touch-hybrid-backfill"]
                if attempt_config["runtime_touch_hybrid_backfill"]
                else []
            ),
        ],
        "selected_attempt_label": selected_attempt["label"],
        "can_execute_trades": False,
    }


def _config_payload(config: RuntimeTouchAbRetryLadderConfig) -> dict[str, Any]:
    payload = asdict(config)
    payload["attempts"] = [asdict(attempt) for attempt in config.attempts]
    return payload


def plan_payload(
    output_dir: Path,
    fresh_duckdb: Path | None,
    fresh_report_root: Path | None,
    config: RuntimeTouchAbRetryLadderConfig,
) -> dict[str, Any]:
    return {
        "script": "scripts/run_runtime_touch_ab_retry_ladder.sh",
        "report_version": REPORT_VERSION,
        "can_execute_trades": False,
        "execution_mode": "dry_run",
        "decision_policy": "offline_runtime_touch_ab_retry_ladder_only",
        "fresh_duckdb": str(fresh_duckdb) if fresh_duckdb else None,
        "fresh_report_root": str(fresh_report_root) if fresh_report_root else None,
        "config": _config_payload(config),
        "delegates_to": [
            "src.research.execution_probe_universe_selection",
            "scripts/run_runtime_touch_ab_cycle.sh",
        ],
        "outputs": {
            "report": str(output_dir / "runtime_touch_ab_retry_ladder.json"),
            "attempts_dir": str(output_dir / "attempts"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh-duckdb")
    parser.add_argument("--fresh-report-root")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--fresh-capture-seconds", type=int, default=1800)
    parser.add_argument("--min-assets", type=int, default=2)
    parser.add_argument("--profile-a", default="execution_probe_v11")
    parser.add_argument("--profile-b", default="execution_probe_v12")
    parser.add_argument("--min-runtime-touch-snapshots", type=int, default=10)
    parser.add_argument(
        "--recent-signalable-window-ms",
        type=int,
        default=RuntimeTouchAbRetryLadderConfig.runtime_recent_signalable_window_ms,
    )
    parser.add_argument(
        "--freshness-ordering",
        choices=("score_first", "freshest_first"),
        default=RuntimeTouchAbRetryLadderConfig.runtime_touch_freshness_ordering,
    )
    parser.add_argument("--print-plan", action="store_true")
    args = parser.parse_args()
    config = RuntimeTouchAbRetryLadderConfig(
        profile_a=args.profile_a,
        profile_b=args.profile_b,
        min_assets=args.min_assets,
        observation_seconds=args.duration_seconds,
        fresh_capture_seconds=args.fresh_capture_seconds,
        min_runtime_touch_snapshots=args.min_runtime_touch_snapshots,
        runtime_recent_signalable_window_ms=args.recent_signalable_window_ms,
        runtime_touch_freshness_ordering=args.freshness_ordering,
    )
    fresh_duckdb = Path(args.fresh_duckdb) if args.fresh_duckdb else None
    fresh_report_root = Path(args.fresh_report_root) if args.fresh_report_root else None
    output_dir = Path(args.output_dir)
    if args.print_plan:
        print(
            json.dumps(
                plan_payload(output_dir, fresh_duckdb, fresh_report_root, config),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if fresh_duckdb is None:
        raise ValueError("--fresh-duckdb is required unless --print-plan is used")
    report = create_runtime_touch_ab_retry_ladder(
        fresh_duckdb,
        output_dir,
        fresh_report_root,
        config,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
