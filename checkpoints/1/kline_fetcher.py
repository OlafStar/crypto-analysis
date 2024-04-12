from binance import AsyncClient, BinanceSocketManager
import asyncio

async def fetch_klines(symbol, interval, queue):
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    async with bm.kline_socket(symbol=symbol, interval=interval) as stream:
        while True:
            res = await stream.recv()
            kline = res['k']
            if kline['x']:  # If the kline is closed
                await queue.put(kline)  # Emit the kline data to the queue

    await client.close_connection()
