from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from app.db.database import Base


class CurrencyRate(Base):
    __tablename__ = "currency_rates"
    
    id = Column(Integer, primary_key=True, index=True)
    base_currency = Column(String, index=True, default="USD")
    target_currency = Column(String, index=True)
    rate = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


