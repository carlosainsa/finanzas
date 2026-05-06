import json
from dataclasses import dataclass
from pathlib import Path


SELECTION_VERSION = "execution_probe_fraction_selection_v1"


@dataclass(frozen=True)
class ExecutionProbeFractionSelection:
    version: str
    profile: str
    near_touch_max_spread_fraction: float
    decision_policy: str
    can_execute_trades: bool
    source_report: str | None = None


def load_execution_probe_v5_fraction_selection(
    path: str | None,
    *,
    default_fraction: float,
) -> ExecutionProbeFractionSelection:
    if path is None:
        return ExecutionProbeFractionSelection(
            version=SELECTION_VERSION,
            profile="execution_probe_v5",
            near_touch_max_spread_fraction=default_fraction,
            decision_policy="default_fraction_until_offline_selection_available",
            can_execute_trades=False,
            source_report=None,
        )
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("execution probe v5 fraction selection must be a JSON object")
    selected_fraction = payload.get("near_touch_max_spread_fraction")
    if not isinstance(selected_fraction, (int, float)):
        raise ValueError("execution probe v5 selected fraction must be numeric")
    selection = ExecutionProbeFractionSelection(
        version=str(payload.get("version") or ""),
        profile=str(payload.get("profile") or ""),
        near_touch_max_spread_fraction=float(selected_fraction),
        decision_policy=str(payload.get("decision_policy") or ""),
        can_execute_trades=bool(payload.get("can_execute_trades")),
        source_report=(
            str(payload["source_report"]) if payload.get("source_report") else None
        ),
    )
    validate_execution_probe_v5_fraction_selection(selection)
    return selection


def load_execution_probe_v6_fraction_selection(
    path: str | None,
    *,
    default_fraction: float,
) -> ExecutionProbeFractionSelection:
    if path is None:
        return ExecutionProbeFractionSelection(
            version=SELECTION_VERSION,
            profile="execution_probe_v6",
            near_touch_max_spread_fraction=default_fraction,
            decision_policy="default_fraction_until_offline_selection_available",
            can_execute_trades=False,
            source_report=None,
        )
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("execution probe v6 fraction selection must be a JSON object")
    selected_fraction = payload.get("near_touch_max_spread_fraction")
    if not isinstance(selected_fraction, (int, float)):
        raise ValueError("execution probe v6 selected fraction must be numeric")
    selection = ExecutionProbeFractionSelection(
        version=str(payload.get("version") or ""),
        profile=str(payload.get("profile") or ""),
        near_touch_max_spread_fraction=float(selected_fraction),
        decision_policy=str(payload.get("decision_policy") or ""),
        can_execute_trades=bool(payload.get("can_execute_trades")),
        source_report=(
            str(payload["source_report"]) if payload.get("source_report") else None
        ),
    )
    validate_execution_probe_v6_fraction_selection(selection)
    return selection


def validate_execution_probe_v5_fraction_selection(
    selection: ExecutionProbeFractionSelection,
) -> None:
    if selection.version != SELECTION_VERSION:
        raise ValueError("unsupported execution probe v5 fraction selection version")
    if selection.profile != "execution_probe_v5":
        raise ValueError("execution probe v5 fraction selection profile mismatch")
    if not 0 <= selection.near_touch_max_spread_fraction <= 1:
        raise ValueError("execution probe v5 selected fraction must be between 0 and 1")
    if selection.can_execute_trades:
        raise ValueError("execution probe v5 fraction selection cannot enable trades")


def validate_execution_probe_v6_fraction_selection(
    selection: ExecutionProbeFractionSelection,
) -> None:
    if selection.version != SELECTION_VERSION:
        raise ValueError("unsupported execution probe v6 fraction selection version")
    if selection.profile != "execution_probe_v6":
        raise ValueError("execution probe v6 fraction selection profile mismatch")
    if not 0 <= selection.near_touch_max_spread_fraction <= 1:
        raise ValueError("execution probe v6 selected fraction must be between 0 and 1")
    if selection.can_execute_trades:
        raise ValueError("execution probe v6 fraction selection cannot enable trades")
