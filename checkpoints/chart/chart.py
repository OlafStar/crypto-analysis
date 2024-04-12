import asyncio
from matplotlib.animation import FuncAnimation
import pandas as pd
from kline_fetcher import fetch_klines, fetch_historical_klines
from utils import datetime_to_milliseconds
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import pandas as pd

class Position:
    def __init__(self, entry_time, entry_price, position_type):
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.position_type = position_type  # e.g., "long"

open_positions = []


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


def setup_plot():
    """
    Sets up the initial plotting environment.
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title('Live Kline Data')
    return fig, ax

def update_plot(frame, ax):
    """
    Updates the plot with the latest data, including a simple representation of candlesticks and indicators.
    """
    ax.clear()  # Clear previous drawings

    if not klines_df.empty:
        # Ensure 'date' column is in the right format
        klines_df['date'] = pd.to_datetime(klines_df['time'], unit='ms')
        
        # For candlesticks: high-low line
        ax.vlines(klines_df['date'], klines_df['low'], klines_df['high'], color='black', linewidth=1)
        # For candlesticks: open-close marker (you could use different colors for up/down)
        ax.plot(klines_df['date'], klines_df['close'], 'k.', markersize=5)  # Simulated with dots for simplicity

        # Plotting Indicators
        ax.plot(klines_df['date'], klines_df['5_EMA'], label='5-period EMA', color='blue', linewidth=1)
        ax.plot(klines_df['date'], klines_df['10_SMA'], label='10-period SMA', color='red', linewidth=1)
        # Add more indicators here as needed

        # Formatting the plot
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.xticks(rotation=45)
        ax.set_xlabel('Time')
        ax.set_ylabel('Price')
        ax.legend()
        ax.set_title('Live Kline Data with Indicators')

    plt.tight_layout()

def add_date_column(df):
    """
    Add a 'date' column to the DataFrame, derived from the 'time' column.
    The 'time' column is assumed to be UNIX timestamp in milliseconds.
    """
    df['date'] = pd.to_datetime(df['time'], unit='ms').dt.date
    return df

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

def calculate_vwap(df):
    """
    Calculate the Volume Weighted Average Price (VWAP), resetting daily.
    """
    # Calculate cumulative volume within each date
    df['cum_volume'] = df.groupby('date')['volume'].cumsum()
    
    # Directly calculate cum_price_volume without apply(), ensuring alignment
    df['cum_price_volume'] = (df['close'] * df['volume']).groupby(df['date']).cumsum()
    
    # Calculate VWAP with daily reset
    df['VWAP'] = df['cum_price_volume'] / df['cum_volume']
    return df



async def fetch_and_process_historical_klines(symbol, interval, limit=1000):
    global klines_df
    historical_klines = await fetch_historical_klines(symbol, interval, limit)  # Fetch historical data
    
    # Process each kline
    for kline in historical_klines:
        # Example structure: [Open time, Open, High, Low, Close, Volume, Close time, ...]
        # Adjust the indices according to your API's response structure
        new_row_df = pd.DataFrame([{
            'time': kline[0],  # Open time
            'open': float(kline[1]),  # Open
            'high': float(kline[2]),  # High
            'low': float(kline[3]),  # Low
            'close': float(kline[4]),  # Close
            'volume': float(kline[5])  # Volume
        }])

        klines_df = pd.concat([klines_df, new_row_df], ignore_index=True)

        klines_df = add_date_column(klines_df)
        klines_df = calculate_5_ema(klines_df)
        klines_df = calculate_10_sma(klines_df)
        klines_df = calculate_rsi(klines_df)
        klines_df = calculate_macd(klines_df)
        klines_df = calculate_bollinger_bands(klines_df)
        klines_df = calculate_vwap(klines_df)

        # calculate_positions()

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
        
        print(f"{new_row_df}")

        klines_df = pd.concat([klines_df, new_row_df], ignore_index=True)
        
        # Calculate indicators
        klines_df = add_date_column(klines_df)
        klines_df = calculate_5_ema(klines_df)
        klines_df = calculate_10_sma(klines_df)
        klines_df = calculate_rsi(klines_df)
        klines_df = calculate_macd(klines_df)
        klines_df = calculate_bollinger_bands(klines_df)
        klines_df = calculate_vwap(klines_df)

        # calculate_positions()
                
        queue.task_done()


async def main():
    symbol = 'BTCUSDT'
    interval = '1h'  # 1 minute interval

    # Setup the plot
    fig, ax = setup_plot()
    
    # Setup FuncAnimation
    ani = FuncAnimation(fig, update_plot, fargs=(ax,), interval=1000, cache_frame_data=False)
    
    await fetch_and_process_historical_klines(symbol, interval)
    
    queue = asyncio.Queue()
    producer = asyncio.create_task(fetch_klines(symbol, interval, queue))
    consumer = asyncio.create_task(process_kline_data(queue))
    
    # Start the plot in non-blocking mode
    plt.show(block=True)
    
    # Wait for both tasks to complete
    await asyncio.gather(producer, consumer)

if __name__ == "__main__":
    asyncio.run(main())