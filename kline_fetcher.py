from binance import AsyncClient, BinanceSocketManager
import asyncio
import aiohttp

async def fetch_historical_klines(symbol, interval, limit=1000):
    """
    Fetch historical klines for a given symbol and interval up to the current moment.
    
    Args:
    - symbol: The symbol to fetch klines for (e.g., "BTCUSDT").
    - interval: The kline interval (e.g., "1m").
    - limit: The maximum number of data points to fetch. Defaults to 1000 or the maximum allowed by the API.
    
    Returns:
    - A list of kline data.
    """
    url = "https://api.binance.com/api/v3/klines"  # Adjust based on your data source
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print("Failed to fetch historical klines")
                return []

# Usage example (omitting start_str since we're fetching the maximum data allowed):
# historical_klines = await fetch_historical_klines("BTCUSDT", "1m", 1000)


async def fetch_klines(symbol, interval, queue):
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    async with bm.kline_socket(symbol=symbol, interval=interval) as stream:
        while True:
            res = await stream.recv()
            print(res)
            kline = res['k']
            if kline['x']:  # If the kline is closed
                await queue.put(kline)  # Emit the kline data to the queue

    await client.close_connection()
