from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional
from datetime import datetime, timezone
from app.models.currency import CurrencyRate
from app.schemas.currency import CurrencyRateCreate, CurrencyRateUpdate


class CurrencyService:
    @staticmethod
    async def get_all(session: AsyncSession) -> List[CurrencyRate]:
        result = await session.execute(select(CurrencyRate))
        return result.scalars().all()
    
    @staticmethod
    async def get_by_id(session: AsyncSession, currency_id: int) -> Optional[CurrencyRate]:
        result = await session.execute(
            select(CurrencyRate).where(CurrencyRate.id == currency_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_by_currency(
        session: AsyncSession, 
        base_currency: str, 
        target_currency: str
    ) -> Optional[CurrencyRate]:
        result = await session.execute(
            select(CurrencyRate).where(
                CurrencyRate.base_currency == base_currency,
                CurrencyRate.target_currency == target_currency
            )
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def create(session: AsyncSession, currency_data: CurrencyRateCreate) -> CurrencyRate:
        currency = CurrencyRate(**currency_data.model_dump())
        session.add(currency)
        await session.commit()
        await session.refresh(currency)
        return currency
    
    @staticmethod
    async def update(
        session: AsyncSession, 
        currency: CurrencyRate, 
        currency_data: CurrencyRateUpdate
    ) -> CurrencyRate:
        if currency_data.rate is not None:
            currency.rate = currency_data.rate
            # Обновляем updated_at при изменении курса
            currency.updated_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(currency)
        return currency
    
    @staticmethod
    async def delete(session: AsyncSession, currency: CurrencyRate) -> None:
        await session.delete(currency)
        await session.commit()

