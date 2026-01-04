from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import json
from contextlib import asynccontextmanager
from threading import Thread

# Synced list of 20 tickers
TICKERS = [
    "BTC-USD", "ETH-USD", "AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", 
    "META", "BRK-B", "UNH", "V", "JNJ", "WMT", "JPM", "PG", "MA", "LLY", "AVGO", "HD"
]

latest_signals = {}
DATA_PATH = "/data/data.csv"

def consume_signals():
    from confluent_kafka import Consumer
    conf = {'bootstrap.servers': 'kafka:29092', 'group.id': 'api-group', 'auto.offset.reset': 'earliest'}
    c = Consumer(conf)
    c.subscribe(['signals-topic'])
    while True:
        msg = c.poll(1.0)
        if msg:
            data = json.loads(msg.value().decode('utf-8'))
            latest_signals[data['ticker']] = data

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = Thread(target=consume_signals, daemon=True)
    thread.start()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/prices")
def get_prices(ticker: str = "BTC-USD", start: str = None):
    try:
        if not os.path.exists(DATA_PATH): return []
        df = pd.read_csv(DATA_PATH, names=["ticker", "timestamp", "price"], header=None)
        ticker_df = df[df["ticker"] == ticker].copy()
        ticker_df["timestamp"] = pd.to_datetime(ticker_df["timestamp"])
        
        if start:
            ticker_df = ticker_df[ticker_df["timestamp"] >= pd.to_datetime(start)]
            
        ticker_df["timestamp"] = ticker_df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return ticker_df.tail(100).to_dict(orient="records")
    except: return []

@app.get("/signal-table")
def get_signal_table():
    output = []
    for ticker in TICKERS:
        sig = latest_signals.get(ticker, {})
        # Explicitly sending 'price' ensures the sidebar isn't $0.00
        output.append({
            "ticker": ticker,
            "price": sig.get("price", 0.0),
            "hurst": sig.get("hurst", "---"),
            "p_value": sig.get("p_value", "---"),
            "signal": sig.get("signal", "NEUTRAL")
        })
    return output