import json
import asyncio
from fastapi import FastAPI, WebSocket
from contextlib import asynccontextmanager
import aio_pika

# Set to store connected browser clients
connected_clients = set()

# Background worker to consume RabbitMQ messages
async def consume_rabbitmq():
    try:
        # Connect to RabbitMQ container using the hostname 'rabbitmq'
        connection = await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("dashboard_queue")
            
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        data = message.body.decode()
                        
                        # Iterate over a copy of the set to prevent errors if a client drops
                        for client in list(connected_clients):
                            try:
                                await client.send_text(data)
                            except:
                                connected_clients.discard(client)
                                
    except asyncio.CancelledError:
        pass # Allows clean exit without printing huge errors

# Modern FastAPI lifespan manager for safe startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_rabbitmq())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# The WebSocket tunnel endpoint for browsers to connect
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except:
        connected_clients.discard(websocket)

if __name__ == "__main__":
    import uvicorn
    print(" [*] Web Server is starting... Port: 8000")
    # Host is 0.0.0.0 to allow external connections outside the Docker container
    uvicorn.run(app, host="0.0.0.0", port=8000)