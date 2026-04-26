import json
from decimal import Decimal
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from src.research.backtest import duckdb_literal
from src.research.pre_live_promotion import create_promotion_views


ADVISORY_REPORT_VERSION = "agent_advisory_offline_v1"
ADVISORY_MODEL_VERSION = "offline_agent_advisory_v1"
ADVISORY_DATA_VERSION = "pre_live_promotion_metrics_v1"
ADVISORY_FEATURE_VERSION = "advisory_evaluator_suite_v1"


@dataclass(frozen=True)
class AdvisoryConfig:
    min_realized_edge: float = 0.0
    min_fill_rate: float = 0.10
    max_adverse_selection_rate: float = 0.50
    max_brier_score: float = 0.25
    max_stale_data_rate: float = 0.05
    max_reconciliation_divergence_rate: float = 0.01


def create_agent_advisory_views(
    db_path: Path, config: AdvisoryConfig = AdvisoryConfig()
) -> None:
    create_promotion_views(db_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            create or replace view agent_advisory_evaluations as
            select
                *,
                cast('{ADVISORY_MODEL_VERSION}' as varchar) as advisory_model_version,
                cast('{ADVISORY_DATA_VERSION}' as varchar) as advisory_data_version,
                cast('{ADVISORY_FEATURE_VERSION}' as varchar) as advisory_feature_version
            from (
                select
                    '{ADVISORY_REPORT_VERSION}' as report_version,
                    'edge_reviewer' as evaluator_id,
                    'edge_reviewer_v1' as evaluator_version,
                    'realized_edge' as metric_name,
                    realized_edge as metric_value,
                    {config.min_realized_edge} as threshold,
                    case
                        when realized_edge is null then 'WARN'
                        when realized_edge > {config.min_realized_edge} then 'PASS'
                        else 'FAIL'
                    end as status,
                    'Checks realized edge after slippage before any live promotion.' as message
                from pre_live_promotion_metrics
                union all
                select
                    '{ADVISORY_REPORT_VERSION}',
                    'execution_quality_reviewer',
                    'execution_quality_reviewer_v1',
                    'fill_rate',
                    fill_rate,
                    {config.min_fill_rate},
                    case
                        when fill_rate >= {config.min_fill_rate} then 'PASS'
                        else 'FAIL'
                    end,
                    'Checks whether theoretical edge is executable through observed fills.'
                from pre_live_promotion_metrics
                union all
                select
                    '{ADVISORY_REPORT_VERSION}',
                    'adverse_selection_reviewer',
                    'adverse_selection_reviewer_v1',
                    'adverse_selection_rate',
                    adverse_selection_rate,
                    {config.max_adverse_selection_rate},
                    case
                        when adverse_selection_rate is null then 'WARN'
                        when adverse_selection_rate <= {config.max_adverse_selection_rate} then 'PASS'
                        else 'FAIL'
                    end,
                    'Checks whether passive fills are followed by unfavorable short-horizon marks.'
                from pre_live_promotion_metrics
                union all
                select
                    '{ADVISORY_REPORT_VERSION}',
                    'calibration_reviewer',
                    'calibration_reviewer_v1',
                    'test_brier_score',
                    test_brier_score,
                    {config.max_brier_score},
                    case
                        when test_brier_score is null then 'WARN'
                        when test_brier_score <= {config.max_brier_score} then 'PASS'
                        else 'FAIL'
                    end,
                    'Checks walk-forward calibration quality on the test split.'
                from pre_live_promotion_metrics
                union all
                select
                    '{ADVISORY_REPORT_VERSION}',
                    'data_quality_reviewer',
                    'data_quality_reviewer_v1',
                    'stale_data_rate',
                    stale_data_rate,
                    {config.max_stale_data_rate},
                    case
                        when stale_data_rate <= {config.max_stale_data_rate} then 'PASS'
                        else 'FAIL'
                    end,
                    'Checks stale orderbook snapshot gaps before trusting research outputs.'
                from pre_live_promotion_metrics
                union all
                select
                    '{ADVISORY_REPORT_VERSION}',
                    'reconciliation_reviewer',
                    'reconciliation_reviewer_v1',
                    'reconciliation_divergence_rate',
                    reconciliation_divergence_rate,
                    {config.max_reconciliation_divergence_rate},
                    case
                        when reconciliation_divergence_rate <= {config.max_reconciliation_divergence_rate} then 'PASS'
                        else 'FAIL'
                    end,
                    'Checks whether local state diverges from execution or reconciliation reports.'
                from pre_live_promotion_metrics
            )
            """
        )
        conn.execute(
            """
            create or replace view agent_advisory_summary as
            select
                report_version,
                count(*) as evaluations,
                sum(case when status = 'PASS' then 1 else 0 end) as passed,
                sum(case when status = 'WARN' then 1 else 0 end) as warned,
                sum(case when status = 'FAIL' then 1 else 0 end) as failed,
                bool_and(status != 'FAIL') as advisory_acceptable,
                false as can_execute_trades
            from agent_advisory_evaluations
            group by report_version
            """
        )


def create_agent_advisory_report(
    db_path: Path, config: AdvisoryConfig = AdvisoryConfig()
) -> dict[str, object]:
    create_agent_advisory_views(db_path, config)
    with duckdb.connect(str(db_path)) as conn:
        evaluations = [
            {
                "report_version": row[0],
                "evaluator_id": row[1],
                "evaluator_version": row[2],
                "metric_name": row[3],
                "metric_value": json_value(row[4]),
                "threshold": json_value(row[5]),
                "status": row[6],
                "message": row[7],
                "advisory_model_version": row[8],
                "advisory_data_version": row[9],
                "advisory_feature_version": row[10],
            }
            for row in conn.execute(
                """
                select
                    report_version,
                    evaluator_id,
                    evaluator_version,
                    metric_name,
                    metric_value,
                    threshold,
                    status,
                    message,
                    advisory_model_version,
                    advisory_data_version,
                    advisory_feature_version
                from agent_advisory_evaluations
                order by evaluator_id, metric_name
                """
            ).fetchall()
        ]
        summary_row = conn.execute(
            """
            select evaluations, passed, warned, failed, advisory_acceptable, can_execute_trades
            from agent_advisory_summary
            """
        ).fetchone()
    summary = {
        "evaluations": int(summary_row[0]) if summary_row else 0,
        "passed": int(summary_row[1]) if summary_row else 0,
        "warned": int(summary_row[2]) if summary_row else 0,
        "failed": int(summary_row[3]) if summary_row else 0,
        "advisory_acceptable": bool(summary_row[4]) if summary_row else False,
        "can_execute_trades": bool(summary_row[5]) if summary_row else False,
    }
    return {
        "report_version": ADVISORY_REPORT_VERSION,
        "model_version": ADVISORY_MODEL_VERSION,
        "data_version": ADVISORY_DATA_VERSION,
        "feature_version": ADVISORY_FEATURE_VERSION,
        "config": asdict(config),
        "summary": summary,
        "evaluations": evaluations,
        "decision_policy": "offline_advisory_only",
    }


def export_agent_advisory_report(
    db_path: Path, output_dir: Path, config: AdvisoryConfig = AdvisoryConfig()
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report = create_agent_advisory_report(db_path, config)
    with duckdb.connect(str(db_path)) as conn:
        for view_name in ("agent_advisory_evaluations", "agent_advisory_summary"):
            target = output_dir / f"{view_name}.parquet"
            conn.execute(
                f"copy (select * from {view_name}) to '{duckdb_literal(target.as_posix())}' (format parquet)"
            )
    (output_dir / "agent_advisory.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="research-agent-advisory")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    report = export_agent_advisory_report(Path(args.duckdb), Path(args.output_dir))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    return value


if __name__ == "__main__":
    raise SystemExit(main())
