from confluent_kafka import Producer
import time
import requests
import json

# Kafka producer config
conf = {
    # 'kafka' is the service name from docker-compose
    'bootstrap.servers': 'kafka:29092', 
    'client.id': 'stock-price-producer'
}

producer = Producer(conf)
topic = 'conn-events'
ticker_symbol = 'BTC-USD'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...',
    'Content-Type': 'application/json'
}

def fetch_and_send_stock_price():
    print(f"Starting producer for {ticker_symbol}...")
    while True:
        try:
            url = f'https://query2.finance.yahoo.com/v8/finance/chart/{ticker_symbol}'
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
                
                producer.produce(topic, key=ticker_symbol, value=str(price))
                producer.flush()
                print(f"✅ Sent {ticker_symbol} price to Kafka: {price}")
            else:
                print(f"❌ Yahoo API Error {response.status_code}: might be blocking the request.")
                
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(5)

if __name__ == "__main__":
    fetch_and_send_stock_price()