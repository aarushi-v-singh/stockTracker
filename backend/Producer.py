import asyncio
import aiohttp
import json
from confluent_kafka import Producer

# Synchronized list of 20 tickers
TICKERS = [
    "BTC-USD", "ETH-USD", "AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", 
    "META", "BRK-B", "UNH", "V", "JNJ", "WMT", "JPM", "PG", "MA", "LLY", "AVGO", "HD"
]

conf = {'bootstrap.servers': 'kafka:29092'}
p = Producer(conf)

async def fetch_price(session, ticker):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
                
                payload = {"ticker": ticker, "price": price}
                # Using ticker as key ensures all data for one stock stays in order
                p.produce('stock-topic', key=ticker, value=json.dumps(payload))
                return True
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
    return False

async def main():
    async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
        while True:
            print(f"Fetching {len(TICKERS)} tickers...")
            tasks = [fetch_price(session, t) for t in TICKERS]
            await asyncio.gather(*tasks)
            p.flush()
            await asyncio.sleep(5) 

if __name__ == "__main__":
    asyncio.run(main())