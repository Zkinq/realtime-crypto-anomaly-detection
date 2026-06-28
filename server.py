import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import aio_pika

connected_clients = set()

# Arka plan dinleyici görevimiz
async def consume_rabbitmq():
    while True:
        try:
            print(" [*] Server RabbitMQ'ya bağlanıyor...", flush=True)
            
            # MÜDAHALE 1: ?heartbeat=0 ile RabbitMQ'nun fişi çekmesini yasakladık.
            # MÜDAHALE 2: connect_robust yerine connect kullandık. Koparsa arka planda zombi olmak yerine çöksün ve döngümüz onu yeniden diriltsin!
            connection = await aio_pika.connect("amqp://guest:guest@rabbitmq/?heartbeat=0")
            
            async with connection:
                print(" [+] Server RabbitMQ'ya BAĞLANDI ve Veri Bekliyor!", flush=True)
                channel = await connection.channel()
                queue = await channel.declare_queue("dashboard_queue")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            data = message.body.decode()
                            
                            # Eğer terminal çok kalabalık oluyorsa bu print'i silebilirsin
                            print(f" [->] Veri iletiliyor: {json.loads(data)['symbol']}", flush=True)
                            
                            for client in list(connected_clients):
                                try:
                                    await client.send_text(data)
                                except Exception:
                                    connected_clients.discard(client)
                                    
        except Exception as e:
            # Sistem zombi olmak yerine hatayı buraya düşürür ve 3 saniye sonra TERTEMİZ baştan başlar.
            print(f" [-] RabbitMQ bağlantısı koptu (Hata: {e}). 3 sn içinde onarılıyor...", flush=True)
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_rabbitmq())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f" [+] Tarayıcı bağlandı! Aktif izleyici: {len(connected_clients)}", flush=True)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # MÜDAHALE 3: Tarayıcı sayfayı yenilediğinde "unexpected EOF" fırlatmasını engelledik.
        connected_clients.discard(websocket)
        print(" [-] Tarayıcı ayrıldı veya sayfa yenilendi.", flush=True)
    except Exception as e:
        connected_clients.discard(websocket)
        print(f" [-] Tarayıcı ağ hatası.", flush=True)

if __name__ == "__main__":
    import uvicorn
    print(" [*] Web Server is starting... Port: 8000", flush=True)
    # MÜDAHALE 4: Tarayıcı ile olan WebSocket kopmalarını engellemek için ping süresini uzattık.
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_ping_interval=60, ws_ping_timeout=60)