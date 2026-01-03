from confluent_kafka import Consumer, KafkaError
import csv
import os
from datetime import datetime

# IMPORTANT: We use /data/ because we will mount a shared folder here in Docker
csv_file = "/data/data.csv"

conf = {
    # 'kafka' is the service name in docker-compose
    'bootstrap.servers': 'kafka:29092', 
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'conn-events'
consumer.subscribe([topic])

print("Consumer started... waiting for messages.")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
           print('Reached end of partition')
        else:
            print(f'Error: {msg.error()}')
    else:
        price_val = msg.value().decode("utf-8")
        print(f'Received: {price_val}')
        
        # Format: [Time, Price]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_row = [timestamp, price_val]
        
        # Check if file exists to write header
        file_exists = os.path.isfile(csv_file)
        
        # Use 'a' for append mode
        with open(csv_file, mode="a", newline="") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["time", "price"]) # Header
            writer.writerow(data_row)