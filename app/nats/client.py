import json
from typing import Optional, Callable
import nats
from nats.aio.client import Client as NATS
from config import settings


class NATSClient:
    def __init__(self):
        self.nc: Optional[NATS] = None
        self.subscription = None
        self.message_handler: Optional[Callable] = None
    
    async def connect(self):
        try:
            self.nc = await nats.connect(settings.nats_url)
            print(f"✅ Подключено к NATS: {settings.nats_url}")
        except Exception as e:
            print(f"⚠️ Не удалось подключиться к NATS: {e}")
            print("⚠️ Продолжаем работу без NATS")
            self.nc = None
    
    async def disconnect(self):
        if self.nc:
            await self.nc.close()
    
    async def publish(self, subject: str, data: dict):
        if not self.nc:
            return
        
        try:
            message = json.dumps(data).encode()
            await self.nc.publish(subject, message)
            print(f"📤 Опубликовано в NATS [{subject}]: {data}")
        except Exception as e:
            print(f"❌ Ошибка публикации в NATS: {e}")
    
    async def subscribe(self, subject: str, handler: Callable):
        if not self.nc:
            return
        
        self.message_handler = handler
        
        async def message_callback(msg):
            try:
                data = json.loads(msg.data.decode())
                print(f"📨 Получено сообщение из NATS [{msg.subject}]: {data}")
                if self.message_handler:
                    await self.message_handler(data)
            except Exception as e:
                print(f"❌ Ошибка обработки сообщения из NATS: {e}")
        
        self.subscription = await self.nc.subscribe(subject, cb=message_callback)
        print(f"📥 Подписка на NATS канал: {subject}")


# Глобальный экземпляр клиента
nats_client = NATSClient()

