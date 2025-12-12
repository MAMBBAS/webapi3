from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.ws.manager import ws_manager
from app.nats.client import nats_client
import json


router = APIRouter()


@router.websocket("/ws/items")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    
    try:
        await ws_manager.send_personal_message({
            "type": "connection",
            "message": "Подключено к WebSocket. Ожидайте обновления курсов валют."
        }, websocket)
        
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                print(f"Получено сообщение от клиента: {message}")
                
                # Эхо-ответ
                await ws_manager.send_personal_message({
                    "type": "echo",
                    "original_message": message
                }, websocket)
            except json.JSONDecodeError:
                await ws_manager.send_personal_message({
                    "type": "error",
                    "message": "Неверный формат JSON"
                }, websocket)
    
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


