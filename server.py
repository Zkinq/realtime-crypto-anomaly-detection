import json
import asyncio
import sqlite3
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import aio_pika
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from collections import deque

connected_clients = set()
recent_history = {}

# ========================================================
# 1. DATABASE SETUP
# ========================================================
# Initialize sqlite3 database connection and build schema if missing
conn = sqlite3.connect('anomalies.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        price REAL,
        timestamp TEXT
    )
''')
conn.commit()

async def consume_rabbitmq():
    while True:
        try:
            print(" [*] Server connecting to RabbitMQ...", flush=True)
            connection = await aio_pika.connect("amqp://guest:guest@rabbitmq/?heartbeat=0")
            
            async with connection:
                print(" [+] Server CONNECTED to RabbitMQ successfully!", flush=True)
                channel = await connection.channel()
                queue = await channel.declare_queue("dashboard_queue")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            data = message.body.decode()
                            data_dict = json.loads(data)
                            
                            # --- YENİ EKLENEN KISIM: Veriyi hafızaya al ---
                            sym = data_dict['symbol']
                            if sym not in recent_history:
                                # Create a queue the same size as the 40 limit on the frontend.
                                recent_history[sym] = deque(maxlen=40) 
                            # Add the raw JSON string (data) directly to the queue.
                            recent_history[sym].append(data)
  
                            # ========================================================
                            # 2. PERSIST DETECTED ANOMALIES TO DATABASE
                            # ========================================================
                            if data_dict.get("is_anomaly"):
                                # Capture the precise current datetime string for record logging
                                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                cursor.execute(
                                    "INSERT INTO anomalies (symbol, price, timestamp) VALUES (?, ?, ?)", 
                                    (data_dict['symbol'], data_dict['price'], now_str)
                                )
                                conn.commit()
                            
                            # Broadcast real-time stream via active WebSocket clients
                            for client in list(connected_clients):
                                try:
                                    await client.send_text(data)
                                except:
                                    connected_clients.discard(client)
                                    
        except Exception as e:
            print(f" [-] Connection dropped (Error: {e}). Retrying in 3 seconds...", flush=True)
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_rabbitmq())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================================================
# 3. REST API ENDPOINT: FETCH HISTORICAL ANOMALIES
# ========================================================
@app.get("/api/anomalies")
def get_historical_anomalies(symbol: str, date: str, start_time: str, end_time: str):
    # Parse query parameters into database timestamp boundaries
    start_datetime = f"{date} {start_time}:00"
    end_datetime = f"{date} {end_time}:59"
    
    # Execute SQL Query: Filter logs by selected asset symbol and target time slice
    cursor.execute('''
        SELECT symbol, price, timestamp 
        FROM anomalies 
        WHERE symbol=? AND timestamp >= ? AND timestamp <= ? 
        ORDER BY timestamp DESC
    ''', (symbol, start_datetime, end_datetime))
    
    rows = cursor.fetchall()
    
    # Serialize query records into a clean JSON array structure for front-end ingestion
    results = [{"symbol": r[0], "price": r[1], "timestamp": r[2]} for r in rows]
    return results

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    
    try:
        for history in recent_history.values():
            for cached_msg in history:
                await websocket.send_text(cached_msg)
    except Exception as e:
        print(f"Cache gönderme hatası: {e}")
        
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(websocket)

@app.get("/")
def serve_dashboard():
    return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, ws_ping_interval=60, ws_ping_timeout=60)