from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from app.db.database import get_db
from app.services.currency_service import CurrencyService
from app.schemas.currency import (
    CurrencyRateCreate, 
    CurrencyRateUpdate, 
    CurrencyRateResponse
)
from app.tasks.background_task import background_task
from app.nats.client import nats_client
from app.ws.manager import ws_manager
from datetime import datetime


router = APIRouter()


@router.get("/items", response_model=List[CurrencyRateResponse])
async def get_items(session: AsyncSession = Depends(get_db)):
    items = await CurrencyService.get_all(session)
    return items


@router.get("/items/{item_id}", response_model=CurrencyRateResponse)
async def get_item(item_id: int, session: AsyncSession = Depends(get_db)):
    item = await CurrencyService.get_by_id(session, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@router.post("/items", response_model=CurrencyRateResponse, status_code=201)
async def create_item(
    item_data: CurrencyRateCreate, 
    session: AsyncSession = Depends(get_db)
):
    item = await CurrencyService.create(session, item_data)
    
    await nats_client.publish("items.updates", {
        "type": "item_created",
        "item_id": item.id,
        "data": {
            "base_currency": item.base_currency,
            "target_currency": item.target_currency,
            "rate": item.rate
        },
        "timestamp": datetime.now().isoformat()
    })
    
    await ws_manager.broadcast({
        "type": "item_created",
        "item": {
            "id": item.id,
            "base_currency": item.base_currency,
            "target_currency": item.target_currency,
            "rate": item.rate
        },
        "timestamp": datetime.now().isoformat()
    })
    
    return item


@router.patch("/items/{item_id}", response_model=CurrencyRateResponse)
async def update_item(
    item_id: int,
    item_data: CurrencyRateUpdate,
    session: AsyncSession = Depends(get_db)
):
    item = await CurrencyService.get_by_id(session, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    old_rate = item.rate
    item = await CurrencyService.update(session, item, item_data)
    
    await nats_client.publish("items.updates", {
        "type": "item_updated",
        "item_id": item.id,
        "data": {
            "base_currency": item.base_currency,
            "target_currency": item.target_currency,
            "old_rate": old_rate,
            "new_rate": item.rate
        },
        "timestamp": datetime.now().isoformat()
    })
    
    await ws_manager.broadcast({
        "type": "item_updated",
        "item": {
            "id": item.id,
            "base_currency": item.base_currency,
            "target_currency": item.target_currency,
            "old_rate": old_rate,
            "new_rate": item.rate
        },
        "timestamp": datetime.now().isoformat()
    })
    
    return item


@router.delete("/items/{item_id}", status_code=204)
async def delete_item(item_id: int, session: AsyncSession = Depends(get_db)):
    item = await CurrencyService.get_by_id(session, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    item_data = {
        "id": item.id,
        "base_currency": item.base_currency,
        "target_currency": item.target_currency,
        "rate": item.rate
    }
    
    await CurrencyService.delete(session, item)
    
    await nats_client.publish("items.updates", {
        "type": "item_deleted",
        "item_id": item_id,
        "data": item_data,
        "timestamp": datetime.now().isoformat()
    })
    
    await ws_manager.broadcast({
        "type": "item_deleted",
        "item": item_data,
        "timestamp": datetime.now().isoformat()
    })


@router.post("/tasks/run")
async def run_task():
    await background_task.run_task()
    return {
        "message": "Фоновая задача запущена",
        "timestamp": datetime.now().isoformat()
    }


