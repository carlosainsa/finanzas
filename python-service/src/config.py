from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    redis_url: str = "redis://localhost:6379"
    polymarket_api_url: str = "https://clob.polymarket.com"
    private_key: str | None = None
    host: str = "0.0.0.0"
    port: int = 8000
    orderbook_stream: str = "orderbook:stream"
    signals_stream: str = "signals:stream"
    execution_reports_stream: str = "execution:reports:stream"
    orderbook_deadletter_stream: str = "orderbook:deadletter"
    orderbook_consumer_group: str = "python-predictor"
    orderbook_consumer_name: str = "predictor-1"
    predictor_min_spread: float = 0.03
    predictor_order_size: float = 1.0
    predictor_min_confidence: float = 0.55

settings = Settings()
