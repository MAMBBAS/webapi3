import asyncio
import httpx
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import AsyncSessionLocal
from app.services.currency_service import CurrencyService
from app.schemas.currency import CurrencyRateCreate, CurrencyRateUpdate
from app.nats.client import nats_client
from app.ws.manager import ws_manager
from config import settings


class BackgroundTask:
    def __init__(self):
        self.is_running = False
        self.task = None
    
    async def fetch_exchange_rates(self):
        if settings.api_type == "crypto":
            return await self._fetch_binance_rates()
        elif settings.api_type == "mock":
            return await self._fetch_mock_rates()
        else:
            return await self._fetch_fiat_rates()
    
    async def _fetch_fiat_rates(self):
        headers = {
            "User-Agent": "CurrencyRatesAPI/1.0",
            "Accept": "application/json"
        }
        
        try:
            print(f"Запрос к API: {settings.exchange_rates_api_url}")
            async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
                response = await client.get(settings.exchange_rates_api_url)
                print(f"Статус ответа: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"Ошибка HTTP: {response.status_code}")
                    print(f"Ответ: {response.text[:200]}")
                    return await self._fetch_alternative_api()
                
                response.raise_for_status()
                data = response.json()
                print(f"Получены данные: base={data.get('base')}, rates_count={len(data.get('rates', {}))}")
                
                base_currency = data.get("base", "USD")
                rates = data.get("rates", {})
                
                if not rates:
                    print("Получен пустой словарь курсов, пробуем альтернативный API")
                    return await self._fetch_alternative_api()
                
                return base_currency, rates
        except httpx.TimeoutException as e:
            print(f"Таймаут при запросе к API: {e}")
            return await self._fetch_alternative_api()
        except httpx.HTTPStatusError as e:
            print(f"HTTP ошибка: {e.response.status_code}")
            print(f"URL: {e.request.url}")
            print(f"Ответ: {e.response.text[:200]}")
            return await self._fetch_alternative_api()
        except Exception as e:
            print(f"Ошибка при получении курсов валют: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return await self._fetch_alternative_api()
    
    async def _fetch_binance_rates(self):
        try:
            symbols = [
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
                "XRPUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "MATICUSDT",
                "LINKUSDT", "UNIUSDT", "LTCUSDT", "ATOMUSDT", "ETCUSDT"
            ]
            
            print(f"Запрос к Binance API для {len(symbols)} криптовалютных пар")
            url = "https://api.binance.com/api/v3/ticker/price"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                print(f"Статус ответа: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"Ошибка HTTP: {response.status_code}")
                    return None, {}
                
                all_prices = response.json()
                
                base_currency = "USDT"
                rates = {}
                
                for price_data in all_prices:
                    symbol = price_data.get("symbol", "")
                    if symbol in symbols:
                        target_currency = symbol.replace("USDT", "")
                        price = float(price_data.get("price", 0))
                        if price > 0:
                            rates[target_currency] = price
                
                print(f"Получено {len(rates)} криптовалютных курсов")
                return base_currency, rates
                
        except Exception as e:
            print(f"Ошибка при получении курсов с Binance: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None, {}
    
    async def _fetch_mock_rates(self):
        import random
        
        print("Генерация случайных курсов для демонстрации")
        
        base_rates = {
            "EUR": 0.85, "GBP": 0.73, "JPY": 110.0, "CNY": 7.2,
            "RUB": 75.0, "INR": 83.0, "KRW": 1300.0, "BRL": 5.0,
            "CAD": 1.35, "AUD": 1.50, "CHF": 0.92, "SGD": 1.35,
            "HKD": 7.8, "NZD": 1.65, "MXN": 20.0, "ZAR": 18.0
        }
        
        base_currency = "USD"
        rates = {}
        
        for currency, base_rate in base_rates.items():
            change_percent = random.uniform(-0.05, 0.05)  # ±5%
            new_rate = base_rate * (1 + change_percent)
            rates[currency] = round(new_rate, 4)
        
        print(f"Сгенерировано {len(rates)} случайных курсов")
        return base_currency, rates
    
    async def _fetch_alternative_api(self):
        """Альтернативный API (fixer.io через exchangerate.host)"""
        try:
            alt_url = "https://api.exchangerate.host/latest?base=USD"
            print(f"Пробуем альтернативный API: {alt_url}")
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(alt_url)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success", False) and data.get("rates"):
                        base_currency = data.get("base", "USD")
                        rates = data.get("rates", {})
                        print(f"Альтернативный API успешен: {len(rates)} курсов")
                        return base_currency, rates
        except Exception as e:
            print(f"Альтернативный API также не сработал: {e}")
        return None, {}
    
    async def save_rates_to_db(self, base_currency: str, rates: dict):
        try:
            async with AsyncSessionLocal() as session:
                updated_count = 0
                created_count = 0
                unchanged_count = 0
                
                for target_currency, rate in rates.items():
                    if target_currency == base_currency:
                        continue
                    
                    try:
                        existing = await CurrencyService.get_by_currency(
                            session, base_currency, target_currency
                        )
                        
                        if existing:
                            if rate is not None and existing.rate != rate:
                                old_rate = existing.rate
                                existing.rate = rate
                                existing.updated_at = datetime.now(timezone.utc)
                                updated_count += 1
                                if updated_count <= 5:
                                    print(f" {base_currency}/{target_currency}: {old_rate} → {rate}")
                            else:
                                unchanged_count += 1
                        else:
                            from app.models.currency import CurrencyRate
                            new_currency = CurrencyRate(
                                base_currency=base_currency,
                                target_currency=target_currency,
                                rate=rate
                            )
                            session.add(new_currency)
                            created_count += 1
                            if created_count <= 5:
                                print(f"  ➕ Создан {base_currency}/{target_currency}: {rate}")
                    except Exception as e:
                        print(f"Ошибка при обработке {base_currency}/{target_currency}: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
                
                # Один commit для всех изменений
                try:
                    await session.commit()
                    print(f"Сохранено в БД: создано {created_count}, обновлено {updated_count}, без изменений {unchanged_count}")
                except Exception as e:
                    await session.rollback()
                    print(f"Ошибка при commit в БД: {e}")
                    raise
        except Exception as e:
            print(f"Критическая ошибка при сохранении в БД: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def run_task(self):
        """Выполнение фоновой задачи"""
        print(f"Запуск фоновой задачи: {datetime.now()}")
        
        try:
            base_currency, rates = await self.fetch_exchange_rates()
            
            if base_currency and rates:
                print(f"Получено курсов: {len(rates)} (базовая валюта: {base_currency})")
                await self.save_rates_to_db(base_currency, rates)
                
                # Публикация события в NATS
                try:
                    await nats_client.publish("items.updates", {
                        "type": "background_task_completed",
                        "base_currency": base_currency,
                        "rates_count": len(rates),
                        "timestamp": datetime.now().isoformat()
                    })
                except Exception as e:
                    print(f"Ошибка публикации в NATS: {e}")
                
                # Отправка уведомления через WebSocket
                try:
                    await ws_manager.broadcast({
                        "type": "background_task_completed",
                        "message": f"Обновлены курсы валют: {len(rates)} валют",
                        "base_currency": base_currency,
                        "timestamp": datetime.now().isoformat()
                    })
                except Exception as e:
                    print(f"Ошибка отправки WebSocket: {e}")
                
                print(f"Фоновая задача завершена: обновлено {len(rates)} курсов")
            else:
                print("Не удалось получить данные с внешнего API")
                if not base_currency:
                    print("   Причина: не получена базовая валюта")
                if not rates:
                    print("   Причина: не получены курсы валют")
        except Exception as e:
            print(f"Критическая ошибка в фоновой задаче: {e}")
            import traceback
            traceback.print_exc()
    
    async def start_periodic(self):
        """Запуск периодической фоновой задачи"""
        self.is_running = True
        print(f"Запуск периодической фоновой задачи (интервал: {settings.task_interval_seconds} сек)")
        
        while self.is_running:
            await self.run_task()
            await asyncio.sleep(settings.task_interval_seconds)
    
    async def stop(self):
        """Остановка фоновой задачи"""
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        print("Фоновая задача остановлена")


# Глобальный экземпляр задачи
background_task = BackgroundTask()

