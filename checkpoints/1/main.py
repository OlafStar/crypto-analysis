import asyncio
import pandas as pd
from kline_fetcher import fetch_klines

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


def calculate_5_ema(df):
    """
    Calculate the 5-period Exponential Moving Average (EMA) of the close prices.
    """
    df['5_EMA'] = df['close'].ewm(span=5, adjust=False).mean()
    return df

def calculate_10_sma(df):
    """
    Calculate the 10-period Simple Moving Average (SMA) of the close prices.
    """
    df['10_SMA'] = df['close'].rolling(window=10).mean()
    return df

def calculate_rsi(df, period=14):
    """
    Calculate the Relative Strength Index (RSI) for the given period.
    """
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    return df

def calculate_macd(df, short_period=12, long_period=26, signal_period=9):
    """
    Calculate the Moving Average Convergence Divergence (MACD).
    """
    df['MACD'] = df['close'].ewm(span=short_period, adjust=False).mean() - df['close'].ewm(span=long_period, adjust=False).mean()
    df['MACD_Signal'] = df['MACD'].ewm(span=signal_period, adjust=False).mean()
    return df

def calculate_bollinger_bands(df, period=20):
    """
    Calculate Bollinger Bands.
    """
    sma = df['close'].rolling(window=period).mean()
    std = df['close'].rolling(window=period).std()
    
    df['Upper_BB'] = sma + (std * 2)
    df['Lower_BB'] = sma - (std * 2)
    return df


async def process_kline_data(queue):
    global klines_df
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
        klines_df = calculate_5_ema(klines_df)
        klines_df = calculate_10_sma(klines_df)
        klines_df = calculate_rsi(klines_df)
        klines_df = calculate_macd(klines_df)
        klines_df = calculate_bollinger_bands(klines_df)

        if len(klines_df) > 10:
            current = klines_df.iloc[-1]
            previous = klines_df.iloc[-2]

            # Print the latest values of the indicators for verification
            print(f"Latest Kline Close: {current['close']}")
            print(f"5-EMA: {current['5_EMA']}, 10-SMA: {current['10_SMA']}")
            print(f"RSI: {current['RSI']}")
            print(f"MACD: {current['MACD']}, MACD Signal: {current['MACD_Signal']}")
            print(f"Upper BB: {current['Upper_BB']}, Lower BB: {current['Lower_BB']}")
            print("--------------------------------------------")

            # Check for a buy signal based on trading logic
            ema_condition = current['5_EMA'] > current['10_SMA'] and previous['5_EMA'] <= previous['10_SMA']
            rsi_condition = current['RSI'] < 70
            macd_condition = current['MACD'] > current['MACD_Signal'] and previous['MACD'] <= previous['MACD_Signal']

            if ema_condition and rsi_condition and macd_condition:
                print(f"Buy Signal at {current['time']}. Conditions met for a long position.")
                
        queue.task_done()


async def main():
    symbol = 'BTCUSDT'
    interval = '1m'  # 1 minute interval
    queue = asyncio.Queue()
    
    # Start the kline fetcher and kline data processor
    producer = asyncio.create_task(fetch_klines(symbol, interval, queue))
    consumer = asyncio.create_task(process_kline_data(queue))
    
    # Wait for both tasks to complete
    await asyncio.gather(producer, consumer)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
