import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import aio_pika

connected_clients = set()

# Background consumer task
async def consume_rabbitmq():
    while True:
        try:
            print(" [*] Server connecting to RabbitMQ...", flush=True)
            
            # INTERVENTION 1: Prevented RabbitMQ from dropping the connection by setting heartbeat=0.
            # INTERVENTION 2: Used connect instead of connect_robust. If it drops, it will fail loudly and let our loop restart it cleanly instead of becoming a background zombie!
            connection = await aio_pika.connect("amqp://guest:guest@rabbitmq/?heartbeat=0")
            
            async with connection:
                print(" [+] Server CONNECTED to RabbitMQ and waiting for data!", flush=True)
                channel = await connection.channel()
                queue = await channel.declare_queue("dashboard_queue")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            data = message.body.decode()
                            
                            # You can delete this print statement if the terminal becomes too cluttered
                            print(f" [->] Forwarding data: {json.loads(data)['symbol']}", flush=True)
                            
                            for client in list(connected_clients):
                                try:
                                    await client.send_text(data)
                                except Exception:
                                    connected_clients.discard(client)
                                    
        except Exception as e:
            # Instead of becoming a zombie, the system catches the exception here and restarts CLEANLY after 3 seconds.
            print(f" [-] RabbitMQ connection lost (Error: {e}). Reconnecting in 3 seconds...", flush=True)
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
    print(f" [+] Browser connected! Active clients: {len(connected_clients)}", flush=True)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # INTERVENTION 3: Prevented "unexpected EOF" errors when the browser refreshes the page.
        connected_clients.discard(websocket)
        print(" [-] Browser disconnected or page refreshed.", flush=True)
    except Exception:
        connected_clients.discard(websocket)
        print(" [-] Browser network error.", flush=True)

if __name__ == "__main__":
    import uvicorn
    print(" [*] Web Server is starting... Port: 8000", flush=True)
    # INTERVENTION 4: Extended ping interval to prevent WebSocket disconnections with the browser.
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_ping_interval=60, ws_ping_timeout=60)