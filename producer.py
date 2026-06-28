import pika
import json
import time
from datetime import datetime
import yfinance as yf

# Our new multi-coin target list
TICKERS = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'USDT-USD', 'ADA-USD', 'SOL-USD']

connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='financial_data')

print(f" [*] Fetching REAL-TIME data for {len(TICKERS)} coins. Press CTRL+C to exit.")

try:
    while True:
        for symbol in TICKERS:
            try:
                stock = yf.Ticker(symbol)
                data = stock.history(period='1d', interval='1m')
                
                if not data.empty:
                    current_price = float(data['Close'].iloc[-1])
                    current_volume = int(data['Volume'].iloc[-1])
                    
                    payload = {
                        "timestamp": datetime.now().isoformat(),
                        "symbol": symbol,
                        "price": round(current_price, 4), # 4 decimals for smaller coins like ADA/USDT
                        "volume": current_volume if current_volume > 0 else 100 
                    }
                    
                    channel.basic_publish(
                        exchange='',
                        routing_key='financial_data',
                        body=json.dumps(payload)
                    )
                    print(f" [x] Sent: {symbol} -> {payload['price']}")
            except Exception as e:
                print(f" [!] Error fetching {symbol}: {e}")
                
        # Wait 15 seconds to avoid Yahoo Finance API rate limits
        time.sleep(15) 
        
except KeyboardInterrupt:
    print("\n [*] Data generation stopped.")
    connection.close()