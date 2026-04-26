from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    redis_url: str = "redis://localhost:6379"
    polymarket_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    private_key: str | None = None
    app_env: str = "development"
    host: str = "0.0.0.0"
    port: int = 8000
    operator_api_url: str = "http://127.0.0.1:8000"
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
