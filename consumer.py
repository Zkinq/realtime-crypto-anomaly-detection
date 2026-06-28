import pika
import json
import numpy as np
from collections import deque
from sklearn.ensemble import IsolationForest

TICKERS = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'USDT-USD', 'ADA-USD', 'SOL-USD']

# Dictionaries to hold independent histories and ML models for EACH coin
histories = {ticker: deque(maxlen=50) for ticker in TICKERS}
models = {ticker: IsolationForest(contamination=0.05, random_state=42) for ticker in TICKERS}

def callback(ch, method, properties, body):
    data = json.loads(body)
    symbol = data.get('symbol', 'BTC-USD')
    price = data['price']
    volume = data['volume']
    timestamp = data['timestamp'][11:19]
    
    is_anomaly = False
    
    if symbol not in histories:
        return # Ignore unknown symbols

    # Add price to the specific coin's history
    histories[symbol].append(price)
    
    if len(histories[symbol]) < 50:
        print(f" [O] {symbol} | {timestamp} -> Price: {price} (Gathering... {len(histories[symbol])}/50)")
    else:
        # Train model specifically for this coin
        X = np.array(histories[symbol]).reshape(-1, 1)
        models[symbol].fit(X)
        
        latest_data = np.array([[price]])
        prediction = models[symbol].predict(latest_data)
        
        if prediction[0] == -1:
            is_anomaly = True
            print(f" [!!!] ANOMALY on {symbol} ({timestamp}) -> Price: {price}")
        else:
            print(f" [✓] Normal {symbol} ({timestamp}) -> Price: {price}")

    result_data = {
        "timestamp": timestamp,
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "is_anomaly": is_anomaly
    }
    
    channel.basic_publish(
        exchange='',
        routing_key='dashboard_queue',
        body=json.dumps(result_data)
    )

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='financial_data')
channel.queue_declare(queue='dashboard_queue')

channel.basic_consume(
    queue='financial_data', 
    on_message_callback=callback, 
    auto_ack=True 
)

print(' [*] Consumer is running multi-stream ML Models... Press CTRL+C to exit')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    connection.close()