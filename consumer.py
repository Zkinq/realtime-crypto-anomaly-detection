import pika
import json
import numpy as np
from collections import deque
from sklearn.ensemble import IsolationForest

# 1. Sliding Window Preparation
price_history = deque(maxlen=50)

# 2. Define the Machine Learning Model
model = IsolationForest(contamination=0.05, random_state=42)

def callback(ch, method, properties, body):
    data = json.loads(body)
    price = data['price']
    volume = data['volume']
    timestamp = data['timestamp'][11:19]
    
    is_anomaly = False
    
    # Add the newly arrived price to the history
    price_history.append(price)
    
    # If not enough data, wait
    if len(price_history) < 50:
        print(f" [O] {timestamp} -> Price: {price} (Gathering data... {len(price_history)}/50)")
    else:
        # Train model with current window
        X = np.array(price_history).reshape(-1, 1)
        model.fit(X)
        
        # Predict the latest data point
        latest_data = np.array([[price]])
        prediction = model.predict(latest_data)
        
        if prediction[0] == -1:
            is_anomaly = True
            print(f" [!!!] ANOMALY DETECTED ({timestamp}) -> Price: {price} (Volume: {volume})")
        else:
            print(f" [✓] Normal ({timestamp}) -> Price: {price}")

    # Package results for the web dashboard
    result_data = {
        "timestamp": timestamp,
        "price": price,
        "volume": volume,
        "is_anomaly": is_anomaly
    }
    
    # Send result to the dashboard queue
    channel.basic_publish(
        exchange='',
        routing_key='dashboard_queue',
        body=json.dumps(result_data)
    )

# Connect to RabbitMQ container
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()

# Declare queues to ensure they exist
channel.queue_declare(queue='financial_data')
channel.queue_declare(queue='dashboard_queue')

channel.basic_consume(
    queue='financial_data', 
    on_message_callback=callback, 
    auto_ack=True 
)

print(' [*] Consumer is running. ML Model activated... Press CTRL+C to exit')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\n [*] Listening stopped.")
    connection.close()