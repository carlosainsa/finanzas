import argparse
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from src.research.runtime_touch_ranking import normalize_records


REPORT_VERSION = "market_family_memory_v1"


@dataclass(frozen=True)
class MarketFamilyMemoryConfig:
    min_observations: int = 1

    def __post_init__(self) -> None:
        if self.min_observations <= 0:
            raise ValueError("min_observations must be positive")


def create_market_family_memory_report(
    output_dir: Path,
    discovery_batches_path: Path | None = None,
    batch_comparison_path: Path | None = None,
    fillability_score_path: Path | None = None,
    config: MarketFamilyMemoryConfig = MarketFamilyMemoryConfig(),
) -> dict[str, Any]:
    rows = build_family_rows(
        discovery_batches_path=discovery_batches_path,
        batch_comparison_path=batch_comparison_path,
        fillability_score_path=fillability_score_path,
    )
    family_rows = aggregate_family_rows(rows, config)
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(family_rows).to_parquet(
        output_dir / "market_family_memory.parquet",
        index=False,
    )
    payload: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "can_execute_trades": False,
        "can_promote_live": False,
        "decision_policy": "offline_market_family_memory_only",
        "selection_source": "market_family_memory",
        "risk_contract": {
            "execution_mode": "dry_run",
            "does_not_modify_quote_policy": True,
            "does_not_modify_risk_limits": True,
            "does_not_publish_signals": True,
        },
        "config": asdict(config),
        "inputs": {
            "discovery_batches": str(discovery_batches_path)
            if discovery_batches_path
            else None,
            "batch_comparison": str(batch_comparison_path)
            if batch_comparison_path
            else None,
            "fillability_score": str(fillability_score_path)
            if fillability_score_path
            else None,
        },
        "counts": {
            "observations": len(rows),
            "families": len(family_rows),
            "exploitable_families": sum(
                1 for row in family_rows if row["family_memory_decision"] == "EXPLOIT"
            ),
        },
        "families": normalize_records(family_rows),
        "outputs": [
            "market_family_memory.parquet",
            "market_family_memory.json",
        ],
    }
    (output_dir / "market_family_memory.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def build_family_rows(
    discovery_batches_path: Path | None,
    batch_comparison_path: Path | None,
    fillability_score_path: Path | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    batch_status = load_batch_status(batch_comparison_path)
    if discovery_batches_path and discovery_batches_path.exists():
        discovery = read_json(discovery_batches_path)
        for batch in discovery.get("batches", []):
            if not isinstance(batch, dict):
                continue
            status = batch_status.get(str(batch.get("batch_id") or ""), {})
            for market in batch.get("markets", []):
                if not isinstance(market, dict):
                    continue
                family_key, family_source = market_family(market)
                rows.append(
                    {
                        "family_key": family_key,
                        "family_source": family_source,
                        "market_id": str(market.get("market_id") or ""),
                        "batch_id": str(batch.get("batch_id") or ""),
                        "batch_decision": status.get("batch_decision", "UNKNOWN"),
                        "route_next_action": status.get(
                            "route_next_action", "UNKNOWN"
                        ),
                        "selected_assets": int_value(status.get("selected_assets")),
                        "eligible_windows": int_value(status.get("eligible_windows")),
                        "market_fillability_score": 0.0,
                        "observation_source": "discovery_batch",
                    }
                )
    if fillability_score_path and fillability_score_path.exists():
        fillability = read_json(fillability_score_path)
        for row in fillability.get("selected", []):
            if not isinstance(row, dict):
                continue
            family_key, family_source = market_family(row)
            rows.append(
                {
                    "family_key": family_key,
                    "family_source": family_source,
                    "market_id": str(row.get("market_id") or ""),
                    "batch_id": "",
                    "batch_decision": "READY"
                    if str(row.get("recommendation")) == "PROMOTE_TO_DISCOVERY_BATCH"
                    else "REJECT",
                    "route_next_action": "FILLABILITY_EVIDENCE",
                    "selected_assets": 1,
                    "eligible_windows": 0,
                    "market_fillability_score": float(
                        row.get("market_fillability_score") or 0.0
                    ),
                    "observation_source": "market_fillability_score",
                }
            )
    return rows


def aggregate_family_rows(
    rows: list[dict[str, Any]],
    config: MarketFamilyMemoryConfig,
) -> list[dict[str, Any]]:
    families: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        families.setdefault(str(row["family_key"]), []).append(row)
    output: list[dict[str, Any]] = []
    for family_key, family_rows in families.items():
        observations = len(family_rows)
        ready = sum(1 for row in family_rows if row["batch_decision"] == "READY")
        blocked = sum(
            1 for row in family_rows if row["route_next_action"] == "BLOCK_LIVE"
        )
        no_windows = sum(
            1
            for row in family_rows
            if row.get("route_next_action") == "EXPAND_MARKET_DISCOVERY"
            or row.get("eligible_windows") == 0
        )
        avg_selected_assets = sum(
            int_value(row.get("selected_assets")) for row in family_rows
        ) / observations
        avg_eligible_windows = sum(
            int_value(row.get("eligible_windows")) for row in family_rows
        ) / observations
        avg_fillability_score = sum(
            float(row.get("market_fillability_score") or 0.0) for row in family_rows
        ) / observations
        ready_rate = ready / observations
        penalty_rate = (blocked + no_windows) / observations
        family_memory_score = (
            ready_rate * 100
            + avg_selected_assets * 10
            + avg_eligible_windows * 5
            + avg_fillability_score
            - penalty_rate * 25
        )
        decision = (
            "EXPLOIT"
            if observations >= config.min_observations and family_memory_score > 0
            else "EXPLORE"
        )
        first = family_rows[0]
        output.append(
            {
                "family_key": family_key,
                "family_source": first.get("family_source", "unknown"),
                "observations": observations,
                "ready_batches": ready,
                "blocked_batches": blocked,
                "no_window_observations": no_windows,
                "ready_rate": ready_rate,
                "avg_selected_assets": avg_selected_assets,
                "avg_eligible_windows": avg_eligible_windows,
                "avg_fillability_score": avg_fillability_score,
                "family_memory_score": family_memory_score,
                "family_memory_decision": decision,
            }
        )
    return sorted(
        output,
        key=lambda row: (
            row["family_memory_score"],
            row["ready_rate"],
            row["observations"],
            row["family_key"],
        ),
        reverse=True,
    )


def load_family_memory_map(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    rows = payload.get("families")
    if not isinstance(rows, list):
        return {}
    return {
        str(row["family_key"]): row
        for row in rows
        if isinstance(row, dict) and row.get("family_key")
    }


def load_batch_status(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    rows = payload.get("batches")
    if not isinstance(rows, list):
        return {}
    return {
        str(row["batch_id"]): row
        for row in rows
        if isinstance(row, dict) and row.get("batch_id")
    }


def market_family(row: dict[str, Any]) -> tuple[str, str]:
    tags = row.get("tags")
    if isinstance(tags, list) and tags:
        return f"tag:{normalize_family_token(str(tags[0]))}", "tag"
    slug = str(row.get("slug") or "").strip()
    if slug:
        parts = [part for part in re.split(r"[^a-zA-Z0-9]+", slug.lower()) if part]
        if parts:
            return f"slug:{'-'.join(parts[:3])}", "slug"
    question = str(row.get("question") or "").strip()
    tokens = [
        token
        for token in re.split(r"[^a-zA-Z0-9]+", question.lower())
        if len(token) >= 3
    ]
    if tokens:
        return f"question:{'-'.join(tokens[:3])}", "question"
    return "unknown:market", "unknown"


def normalize_family_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return token or "unknown"


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--discovery-batches")
    parser.add_argument("--batch-comparison")
    parser.add_argument("--fillability-score")
    parser.add_argument("--min-observations", type=int, default=1)
    args = parser.parse_args()
    report = create_market_family_memory_report(
        Path(args.output_dir),
        discovery_batches_path=Path(args.discovery_batches)
        if args.discovery_batches
        else None,
        batch_comparison_path=Path(args.batch_comparison)
        if args.batch_comparison
        else None,
        fillability_score_path=Path(args.fillability_score)
        if args.fillability_score
        else None,
        config=MarketFamilyMemoryConfig(min_observations=args.min_observations),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
