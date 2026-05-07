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
    return load_execution_probe_fraction_selection(
        path,
        default_fraction=default_fraction,
        profile="execution_probe_v5",
    )


def load_execution_probe_v6_fraction_selection(
    path: str | None,
    *,
    default_fraction: float,
) -> ExecutionProbeFractionSelection:
    return load_execution_probe_fraction_selection(
        path,
        default_fraction=default_fraction,
        profile="execution_probe_v6",
    )


def load_execution_probe_v7_fraction_selection(
    path: str | None,
    *,
    default_fraction: float,
) -> ExecutionProbeFractionSelection:
    return load_execution_probe_fraction_selection(
        path,
        default_fraction=default_fraction,
        profile="execution_probe_v7",
    )


def load_execution_probe_fraction_selection(
    path: str | None,
    *,
    default_fraction: float,
    profile: str,
) -> ExecutionProbeFractionSelection:
    if path is None:
        return ExecutionProbeFractionSelection(
            version=SELECTION_VERSION,
            profile=profile,
            near_touch_max_spread_fraction=default_fraction,
            decision_policy="default_fraction_until_offline_selection_available",
            can_execute_trades=False,
            source_report=None,
        )
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{profile} fraction selection must be a JSON object")
    selected_fraction = payload.get("near_touch_max_spread_fraction")
    if not isinstance(selected_fraction, (int, float)):
        raise ValueError(f"{profile} selected fraction must be numeric")
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
    validate_execution_probe_fraction_selection(selection, profile=profile)
    return selection


def validate_execution_probe_v5_fraction_selection(
    selection: ExecutionProbeFractionSelection,
) -> None:
    validate_execution_probe_fraction_selection(selection, profile="execution_probe_v5")


def validate_execution_probe_v6_fraction_selection(
    selection: ExecutionProbeFractionSelection,
) -> None:
    validate_execution_probe_fraction_selection(selection, profile="execution_probe_v6")


def validate_execution_probe_v7_fraction_selection(
    selection: ExecutionProbeFractionSelection,
) -> None:
    validate_execution_probe_fraction_selection(selection, profile="execution_probe_v7")


def validate_execution_probe_fraction_selection(
    selection: ExecutionProbeFractionSelection,
    *,
    profile: str,
) -> None:
    profile_label = profile.replace("_", " ")
    if selection.version != SELECTION_VERSION:
        raise ValueError(f"unsupported {profile_label} fraction selection version")
    if selection.profile != profile:
        raise ValueError(f"{profile_label} fraction selection profile mismatch")
    if not 0 <= selection.near_touch_max_spread_fraction <= 1:
        raise ValueError(f"{profile_label} selected fraction must be between 0 and 1")
    if selection.can_execute_trades:
        raise ValueError(f"{profile_label} fraction selection cannot enable trades")
