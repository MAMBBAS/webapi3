from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class CurrencyRateBase(BaseModel):
    base_currency: str = "USD"
    target_currency: str
    rate: float


class CurrencyRateCreate(CurrencyRateBase):
    pass


class CurrencyRateUpdate(BaseModel):
    rate: Optional[float] = None


class CurrencyRateResponse(CurrencyRateBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


