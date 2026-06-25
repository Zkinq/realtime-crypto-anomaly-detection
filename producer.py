import pika
import json
import time
from datetime import datetime
import yfinance as yf

# Target stock symbol (Bitcoin)
TICKER_SYMBOL = 'BTC-USD'
stock = yf.Ticker(TICKER_SYMBOL)

# Connect to RabbitMQ container (Hostname is 'rabbitmq' as defined in docker-compose.yml)
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='financial_data')

print(f" [*] Fetching REAL-TIME data for {TICKER_SYMBOL}. Press CTRL+C to exit.")

try:
    while True:
        # Fetch the latest 1-minute interval data
        data = stock.history(period='1d', interval='1m')
        
        if not data.empty:
            # Using float() and int() explicitly to prevent Numpy JSON serialization errors
            current_price = float(data['Close'].iloc[-1])
            current_volume = int(data['Volume'].iloc[-1])
            
            payload = {
                "timestamp": datetime.now().isoformat(),
                "symbol": TICKER_SYMBOL,
                "price": round(current_price, 2),
                "volume": current_volume if current_volume > 0 else 100 
            }
            
            # Send to RabbitMQ
            channel.basic_publish(
                exchange='',
                routing_key='financial_data',
                body=json.dumps(payload)
            )
            
            print(f" [x] Sent: {payload}")
        
        # Wait 5 seconds to avoid rate limits
        time.sleep(5) 
        
except KeyboardInterrupt:
    print("\n [*] Data generation stopped.")
    connection.close()