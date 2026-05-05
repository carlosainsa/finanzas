from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    redis_url: str = "redis://localhost:6379"
    polymarket_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    nvidia_nim_api_key: str | None = None
    nim_base_url: str = "https://integrate.api.nvidia.com/v1"
    nim_model: str = "deepseek-ai/deepseek-v3.2"
    enable_nim_advisory: bool = False
    nim_timeout_seconds: float = 30.0
    nim_max_evidence_per_run: int = 25
    nim_max_requests_per_run: int = 25
    nim_max_tokens_per_run: int = 0
    nim_max_latency_ms_per_run: float = 0.0
    nim_max_cost_per_run: float = 0.0
    nim_fail_on_budget_exceeded: bool = False
    nim_input_cost_per_million_tokens: float = 0.0
    nim_output_cost_per_million_tokens: float = 0.0
    nim_cost_currency: str = "USD"
    private_key: str | None = None
    app_env: str = "development"
    disable_market_ws: bool = False
    host: str = "0.0.0.0"
    port: int = 18000
    operator_api_url: str = "http://127.0.0.1:18000"
    operator_api_token: str | None = None
    operator_read_token: str | None = None
    operator_control_token: str | None = None
    require_postgres_state: bool = False
    database_url: str | None = None
    data_lake_root: str = "data_lake"
    data_lake_duckdb_path: str = "data_lake/research.duckdb"
    data_lake_export_count: int = 1000
    discovery_limit: int = 50
    discovery_min_liquidity: float = 100.0
    discovery_min_volume: float = 100.0
    orderbook_stream: str = "orderbook:stream"
    signals_stream: str = "signals:stream"
    execution_reports_stream: str = "execution:reports:stream"
    orderbook_deadletter_stream: str = "orderbook:deadletter"
    signals_deadletter_stream: str = "signals:deadletter"
    operator_commands_stream: str = "operator:commands:stream"
    operator_results_stream: str = "operator:results:stream"
    operator_kill_switch_key: str = "operator:kill_switch"
    orderbook_consumer_group: str = "python-predictor"
    orderbook_consumer_name: str = "predictor-1"
    executor_consumer_group: str = "rust-executor"
    execution_mode: str = "dry_run"
    max_order_size: float = 10.0
    min_confidence: float = 0.55
    signal_max_age_ms: int = 5000
    max_market_exposure: float = 100.0
    max_daily_loss: float = 50.0
    predictor_min_spread: float = 0.03
    predictor_order_size: float = 1.0
    predictor_min_confidence: float = 0.55
    predictor_strategy_profile: str = "baseline"
    predictor_quote_placement: str = "passive_bid"
    predictor_near_touch_research_only: bool = True
    predictor_near_touch_tick_size: float = 0.01
    predictor_near_touch_offset_ticks: int = 0
    predictor_near_touch_max_spread_fraction: float = 1.0
    predictor_conservative_min_confidence: float = 0.65
    predictor_conservative_near_touch_max_spread_fraction: float = 0.5
    predictor_conservative_min_depth: float = 2.0
    predictor_conservative_max_top_changes: int = 3
    predictor_conservative_top_change_window_ms: int = 60_000
    predictor_balanced_min_confidence: float = 0.60
    predictor_balanced_near_touch_max_spread_fraction: float = 0.75
    predictor_balanced_min_depth: float = 1.5
    predictor_balanced_max_top_changes: int = 6
    predictor_balanced_top_change_window_ms: int = 60_000
    predictor_execution_probe_min_confidence: float = 0.50
    predictor_execution_probe_near_touch_max_spread_fraction: float = 1.0
    predictor_execution_probe_min_depth: float = 0.25
    predictor_execution_probe_max_top_changes: int = 12
    predictor_execution_probe_top_change_window_ms: int = 60_000
    predictor_blocked_segments_path: str | None = None

settings = Settings()


def validate_production_settings() -> None:
    missing = []
    postgres_required = (
        settings.app_env.lower() == "production" or settings.require_postgres_state
    )
    if postgres_required and settings.database_url is None:
        missing.append("DATABASE_URL")
    if settings.app_env.lower() != "production":
        if missing:
            raise RuntimeError(
                "configuration missing required settings: " + ", ".join(missing)
            )
        return
    if settings.predictor_quote_placement.lower() != "passive_bid":
        missing.append("PREDICTOR_QUOTE_PLACEMENT=passive_bid")
    if settings.operator_read_token is None:
        missing.append("OPERATOR_READ_TOKEN")
    if settings.operator_control_token is None:
        missing.append("OPERATOR_CONTROL_TOKEN")
    if not settings.execution_mode:
        missing.append("EXECUTION_MODE")
    if missing:
        raise RuntimeError(
            "production configuration missing required settings: " + ", ".join(missing)
        )
