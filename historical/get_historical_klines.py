import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import os  

async def fetch_historical_klines_to_df(symbol, interval, start_str, end_str=None, limit=1000):
    df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])
    url = "https://api.binance.com/api/v3/klines"
    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': start_str,
                'limit': limit
            }
            if end_str:
                params['endTime'] = end_str
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if not data:
                        break
                    for kline in data:
                        new_row = {
                            'time': kline[0],  # Open time
                            'open': float(kline[1]),  # Open
                            'high': float(kline[2]),  # High
                            'low': float(kline[3]),  # Low
                            'close': float(kline[4]),  # Close
                            'volume': float(kline[5])  # Volume
                        }
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    start_str = data[-1][0] + 1
                else:
                    print("Failed to fetch historical klines")
                    break
    return df

async def save_df_to_csv(df, filename):
    directory = "historical/data"  # Define the directory path
    if not os.path.exists(directory):  # Check if the directory exists
        os.makedirs(directory)  # Create the directory if it does not exist
    filepath = os.path.join(directory, filename)  # Construct the filepath
    df.to_csv(filepath, index=False)  # Save the DataFrame to CSV in the specified path

async def main():
    symbol = "BTCUSDT"
    # intervals = ["1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m", "3m", "1m"] 
    intervals = ["5m", "3m", "1m"] 
    start_str = int(datetime(2017, 7, 17).timestamp() * 1000)  # Adjust start date as needed
    
    for interval in intervals:
        print(f"Fetching {interval} data for {symbol}")
        df = await fetch_historical_klines_to_df(symbol, interval, start_str)
        await save_df_to_csv(df, f"{symbol}_{interval}_klines.csv")  # The filename now includes the desired directory
        print(f"Saved {interval} data to CSV")

if __name__ == "__main__":
    asyncio.run(main())
