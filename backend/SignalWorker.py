import json
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from confluent_kafka import Consumer, Producer

# Config
CONF = {'bootstrap.servers': 'kafka:29092', 'group.id': 'signal-processor'}
INPUT_TOPIC = 'stock-topic'
OUTPUT_TOPIC = 'signals-topic'

consumer = Consumer(CONF)
consumer.subscribe([INPUT_TOPIC])
producer = Producer({'bootstrap.servers': 'kafka:29092'})

price_history = {}

def get_hurst_exponent(ts, max_lag=20):
    try:
        lags = range(2, max_lag)
        tau = [np.std(np.subtract(ts[lag:], ts[:-lag])) for lag in lags]
        # Added safety: if std dev is 0, log(tau) will fail
        if any(t == 0 for t in tau):
            return 0.5
        return np.polyfit(np.log(lags), np.log(tau), 1)[0]
    except:
        return 0.5 # Default to random walk if math fails

print("Signal Worker (Kafka-to-Kafka) started...")

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    
    try:
        data = json.loads(msg.value().decode('utf-8'))
        ticker = data['ticker']
        price = float(data['price'])
        
        if ticker not in price_history:
            price_history[ticker] = []
        price_history[ticker].append(price)

        # Trigger analysis once we have 50 data points
        if len(price_history[ticker]) >= 50:
            recent = price_history[ticker][-50:]
            
            # --- THE FIX: CHECK FOR VARIANCE ---
            # set(recent) returns unique values. If size is 1, all values are identical.
            if len(set(recent)) > 1:
                try:
                    hurst = get_hurst_exponent(recent)
                    p_value = adfuller(recent)[1]
                except Exception as e:
                    print(f"Stats Error for {ticker}: {e}")
                    hurst, p_value = 0.5, 1.0
            else:
                # If data is constant, p_value is 1.0 (not stationary) and hurst is 0.5
                hurst, p_value = 0.5, 1.0

            signal = "NEUTRAL"
            if hurst > 0.6 and p_value < 0.05: signal = "STRONG TREND"
            elif hurst < 0.4 and p_value < 0.05: signal = "MEAN REVERSION"

            payload = {
                "ticker": ticker,
                "price": price, # Added price so your frontend table gets it
                "hurst": round(hurst, 4),
                "p_value": round(p_value, 4),
                "signal": signal,
                "timestamp": pd.Timestamp.now().isoformat()
            }
            
            producer.produce(OUTPUT_TOPIC, key=ticker, value=json.dumps(payload))
            producer.flush()
            
            # Memory management: keep window small
            price_history[ticker] = price_history[ticker][-100:]
            
    except Exception as global_e:
        print(f"Global Worker Error: {global_e}")
        continue