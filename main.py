import asyncio
import pandas as pd
from kline_fetcher import fetch_klines, fetch_historical_klines
from utils import datetime_to_milliseconds
import datetime
from positions.positions import calculate_positions
import os
from utils import add_date_column, calculate_5_ema, calculate_10_sma, calculate_rsi, calculate_macd, calculate_bollinger_bands, calculate_vwap, calculate_obv, calculate_stochastic_oscillator, calculate_atr, calculate_ad_line, calculate_pivot_points, calculate_cci, calculate_momentum, calculate_standard_deviation, calculate_fibonacci_retracement, calculate_roc

# DataFrame to store kline data
klines_df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])
klines_df = klines_df.astype({
    'time': 'float64',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'float64'
})

# async def fetch_and_process_historical_klines(symbol, interval, limit=1000):
#     global klines_df
#     historical_klines = await fetch_historical_klines(symbol, interval, limit)  # Fetch historical data
    
#     # Process each kline
#     for kline in historical_klines:
#         # Example structure: [Open time, Open, High, Low, Close, Volume, Close time, ...]
#         # Adjust the indices according to your API's response structure
#         new_row_df = pd.DataFrame([{
#             'time': kline[0],  # Open time
#             'open': float(kline[1]),  # Open
#             'high': float(kline[2]),  # High
#             'low': float(kline[3]),  # Low
#             'close': float(kline[4]),  # Close
#             'volume': float(kline[5])  # Volume
#         }])

#         klines_df = pd.concat([klines_df, new_row_df], ignore_index=True)

#         klines_df = add_date_column(klines_df)
#         klines_df = calculate_5_ema(klines_df)
#         klines_df = calculate_10_sma(klines_df)
#         klines_df = calculate_rsi(klines_df)
#         klines_df = calculate_macd(klines_df)
#         klines_df = calculate_bollinger_bands(klines_df)
#         klines_df = calculate_vwap(klines_df)
#         klines_df = calculate_obv(klines_df)
#         klines_df = calculate_stochastic_oscillator(klines_df)
#         klines_df = calculate_atr(klines_df)
#         klines_df = calculate_ad_line(klines_df)
#         klines_df = calculate_cci(klines_df)
#         klines_df = calculate_pivot_points(klines_df)
#         klines_df = calculate_momentum(klines_df)
#         klines_df = calculate_standard_deviation(klines_df)
#         klines_df = calculate_fibonacci_retracement(klines_df)
#         klines_df = calculate_roc(klines_df, period=14) 

#         # calculate_positions(klines_df=klines_df)

#     os.makedirs('./klines_data', exist_ok=True)
#     file_path = os.path.join('./klines_data', f'{symbol}_{interval}_klines.csv')
    
#     # Save DataFrame to CSV
#     klines_df.to_csv(file_path, index=False)
#     print(f'Saved DataFrame to {file_path}')
    

async def process_kline_data(queue):
    global klines_df, open_positions
    while True:
        kline = await queue.get()
        # Append the new kline data
        new_row_df = pd.DataFrame([{
            'time': kline['t'],
            'open': float(kline['o']),
            'high': float(kline['h']),
            'low': float(kline['l']),
            'close': float(kline['c']),
            'volume': float(kline['v'])
        }])
        
        klines_df = pd.concat([klines_df, new_row_df], ignore_index=True)
        
        # Calculate indicators
        klines_df = add_date_column(klines_df)
        klines_df = calculate_5_ema(klines_df)
        klines_df = calculate_10_sma(klines_df)
        klines_df = calculate_rsi(klines_df)
        klines_df = calculate_macd(klines_df)
        klines_df = calculate_bollinger_bands(klines_df)
        klines_df = calculate_vwap(klines_df)
        klines_df = calculate_obv(klines_df)
        klines_df = calculate_stochastic_oscillator(klines_df)
        klines_df = calculate_atr(klines_df)
        klines_df = calculate_ad_line(klines_df)
        klines_df = calculate_cci(klines_df)
        klines_df = calculate_pivot_points(klines_df)
        klines_df = calculate_momentum(klines_df)
        klines_df = calculate_standard_deviation(klines_df)
        klines_df = calculate_fibonacci_retracement(klines_df)
        klines_df = calculate_roc(klines_df, period=14) 

        # calculate_positions(klines_df=klines_df)
                
        queue.task_done()


async def main():
    symbol = 'BTCUSDT'
    interval = '15m'  # 1 minute interval
    
    
    queue = asyncio.Queue()
    producer = asyncio.create_task(fetch_klines(symbol, interval, queue))
    consumer = asyncio.create_task(process_kline_data(queue))
    await asyncio.gather(producer, consumer)

if __name__ == "__main__":
    asyncio.run(main())
