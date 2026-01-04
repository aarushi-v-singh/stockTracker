from confluent_kafka import Consumer
import json
import os
from datetime import datetime

conf = {'bootstrap.servers': 'kafka:29092', 'group.id': 'storage-group', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['stock-topic'])

print("Data Consumer started...")

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    
    try:
        data = json.loads(msg.value().decode('utf-8'))
        ticker = data['ticker']
        price = data['price']
        timestamp = datetime.now().isoformat() # Fixed: datetime is now imported
        
        os.makedirs('/data', exist_ok=True)
        with open('/data/data.csv', 'a') as f:
            f.write(f"{ticker},{timestamp},{price}\n")
    except Exception as e:
        print(f"Consumer Error: {e}")
    