from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./currency.db"
    nats_url: str = "nats://localhost:4222"
    task_interval_seconds: int = 60
    exchange_rates_api_url: str = "https://api.exchangerate-api.com/v4/latest/USD"
    # Тип API: "fiat" (обычные валюты), "crypto" (Binance криптовалюты), "mock" (случайные данные для демо)
    api_type: str = "crypto"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

