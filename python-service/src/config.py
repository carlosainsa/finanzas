from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    polymarket_api_url: str = "https://clob.polymarket.com"
    private_key: str | None = None
    host: str = "0.0.0.0"
    port: int = 8000
    predictor_min_spread: float = 0.03
    predictor_order_size: float = 1.0
    predictor_min_confidence: float = 0.55

    class Config:
        env_file = ".env"

settings = Settings()
