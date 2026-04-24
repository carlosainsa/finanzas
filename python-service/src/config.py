from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    redis_url: str = "redis://localhost:6379"
    polymarket_api_url: str = "https://clob.polymarket.com"
    private_key: str | None = None
    host: str = "0.0.0.0"
    port: int = 8000
    operator_api_url: str = "http://127.0.0.1:8000"
    data_lake_root: str = "data_lake"
    data_lake_duckdb_path: str = "data_lake/research.duckdb"
    data_lake_export_count: int = 1000
    orderbook_stream: str = "orderbook:stream"
    signals_stream: str = "signals:stream"
    execution_reports_stream: str = "execution:reports:stream"
    orderbook_deadletter_stream: str = "orderbook:deadletter"
    signals_deadletter_stream: str = "signals:deadletter"
    operator_commands_stream: str = "operator:commands:stream"
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
