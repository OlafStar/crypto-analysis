import pandas as pd

# Function to convert a datetime object to milliseconds since epoch
def datetime_to_milliseconds(dt_obj):
    return int(dt_obj.timestamp() * 1000)

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

def calculate_obv(df):
    """
    Calculate On-Balance Volume (OBV) for the DataFrame, ensuring 'OBV' is treated as float.
    """
    # Initialize OBV column as float to ensure compatibility with volume data
    df['OBV'] = 0.0  # Ensures OBV is float from the start
    
    for i in range(1, len(df)):
        if df.loc[i, 'close'] > df.loc[i-1, 'close']:
            df.loc[i, 'OBV'] = df.loc[i-1, 'OBV'] + df.loc[i, 'volume']
        elif df.loc[i, 'close'] < df.loc[i-1, 'close']:
            df.loc[i, 'OBV'] = df.loc[i-1, 'OBV'] - df.loc[i, 'volume']
        else:
            df.loc[i, 'OBV'] = df.loc[i-1, 'OBV']
    
    # Ensure the 'OBV' column is explicitly treated as a float to avoid dtype issues
    df['OBV'] = df['OBV'].astype(float)
    
    return df

def calculate_stochastic_oscillator(df, k_period=14, d_period=3):
    """
    Calculate Stochastic Oscillator (%K and %D) for the DataFrame.
    """
    # Calculate %K
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()
    df['%K'] = ((df['close'] - low_min) / (high_max - low_min)) * 100

    # Calculate %D
    df['%D'] = df['%K'].rolling(window=d_period).mean()

    return df

def calculate_atr(df, period=14):
    """
    Calculate the Average True Range (ATR) for the given DataFrame.
    """
    # Calculate True Range (TR)
    df['high_low'] = df['high'] - df['low']
    df['high_close'] = (df['high'] - df['close'].shift()).abs()
    df['low_close'] = (df['low'] - df['close'].shift()).abs()
    
    df['TR'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
    
    # Calculate ATR
    df['ATR'] = df['TR'].rolling(window=period).mean()
    
    # Cleanup: Drop the intermediate calculation columns
    df.drop(['high_low', 'high_close', 'low_close', 'TR'], axis=1, inplace=True)
    
    return df

def calculate_ad_line(df):
    """
    Calculate the Accumulation/Distribution Line for the given DataFrame.
    """
    # Initialize A/D Line with 0s
    df['AD_Line'] = 0.0
    
    for i in range(1, len(df)):
        clv = ((df.loc[i, 'close'] - df.loc[i, 'low']) - (df.loc[i, 'high'] - df.loc[i, 'close'])) / (df.loc[i, 'high'] - df.loc[i, 'low']) if (df.loc[i, 'high'] - df.loc[i, 'low']) != 0 else 0
        df.loc[i, 'AD_Line'] = df.loc[i-1, 'AD_Line'] + (clv * df.loc[i, 'volume'])
    
    return df

def calculate_cci(df, period=20):
    """
    Calculate the Commodity Channel Index (CCI) for the given DataFrame.
    """
    # Calculate the Typical Price
    df['TP'] = (df['high'] + df['low'] + df['close']) / 3

    # Calculate the Simple Moving Average of the Typical Price
    df['TP_SMA'] = df['TP'].rolling(window=period).mean()

    # Calculate Mean Deviation without using lambda
    # The mean absolute deviation around the rolling mean (not around 0)
    df['MD'] = df['TP'].rolling(window=period).apply(lambda x: (x - x.mean()).abs().mean(), raw=False)

    # Calculate CCI
    df['CCI'] = (df['TP'] - df['TP_SMA']) / (0.015 * df['MD'])

    # Cleanup: Drop the intermediate calculation columns
    df.drop(['TP', 'TP_SMA', 'MD'], axis=1, inplace=True)
    
    return df

def calculate_pivot_points(df):
    """
    Calculate Pivot Points and the first and second levels of support and resistance.
    """
    # Shift the data to get 'prev' values for high, low, and close
    df['High_prev'] = df['high'].shift(1)
    df['Low_prev'] = df['low'].shift(1)
    df['Close_prev'] = df['close'].shift(1)

    # Calculate Pivot Point
    df['PP'] = (df['High_prev'] + df['Low_prev'] + df['Close_prev']) / 3

    # Calculate support and resistance levels
    df['R1'] = (2 * df['PP']) - df['Low_prev']
    df['S1'] = (2 * df['PP']) - df['High_prev']
    df['R2'] = df['PP'] + (df['High_prev'] - df['Low_prev'])
    df['S2'] = df['PP'] - (df['High_prev'] - df['Low_prev'])
    
    # Cleanup: Drop the intermediate calculation columns
    df.drop(['High_prev', 'Low_prev', 'Close_prev'], axis=1, inplace=True)
    
    return df

def calculate_momentum(df, period=14):
    """
    Calculate the Momentum Indicator for the given DataFrame.
    """
    # The momentum formula simply subtracts the closing price n periods ago from the current closing price
    df['Momentum'] = df['close'] - df['close'].shift(period)
    
    return df

def calculate_standard_deviation(df, period=20):
    """
    Calculate the Standard Deviation (Std Dev) of the close prices over the specified period.
    """
    df['Std_Dev'] = df['close'].rolling(window=period).std()
    
    return df

def calculate_fibonacci_retracement(df, period=14):
    """
    Calculate Fibonacci Retracement levels based on the highest and lowest prices
    over the last 'period' periods.
    """
    # Calculate the rolling highest high and lowest low
    highest_price = df['high'].rolling(window=period).max()
    lowest_price = df['low'].rolling(window=period).min()
    
    # Calculate the price range
    price_range = highest_price - lowest_price
    
    # Calculate Fibonacci Levels
    df['Fib_23_6'] = highest_price - (price_range * 0.236)
    df['Fib_38_2'] = highest_price - (price_range * 0.382)
    df['Fib_50'] = highest_price - (price_range * 0.5)
    df['Fib_61_8'] = highest_price - (price_range * 0.618)
    df['Fib_78_6'] = highest_price - (price_range * 0.786)  # Sometimes used
    
    return df

def calculate_roc(df, period=14):
    """
    Calculate the Price Rate of Change (ROC) for the given DataFrame.
    ROC is calculated by dividing the current price by the price 'period' days ago
    and then converting it to a percentage.
    """
    df['ROC'] = ((df['close'] - df['close'].shift(period)) / df['close'].shift(period)) * 100
    return df

import numpy as np

def calculate_support_resistance(df, window=14):
    """
    Calculate potential support and resistance levels.
    
    Args:
    - df (pd.DataFrame): The input DataFrame with 'high' and 'low' columns.
    - window (int): The rolling window size to identify local maxima and minima for resistance and support levels.
    
    Returns:
    - df (pd.DataFrame): The DataFrame with added 'Support' and 'Resistance' columns.
    """
    # Identify local maxima as potential resistance
    df['Resistance'] = df['high'].rolling(window=window, center=True).apply(lambda x: x.max() if x.argmax() == window // 2 else np.nan, raw=True)
    
    # Identify local minima as potential support
    df['Support'] = df['low'].rolling(window=window, center=True).apply(lambda x: x.min() if x.argmin() == window // 2 else np.nan, raw=True)
    
    # Optional: Fill NaN values with the nearest non-NaN values for visualization purposes
    df['Support'] = df['Support'].ffill()
    df['Resistance'] = df['Resistance'].ffill()
    
    return df

def calculate_vwma(df, period=20):
    """
    Calculate the Volume-Weighted Moving Average (VWMA).
    
    Parameters:
        df (pd.DataFrame): DataFrame with columns 'close' and 'volume'.
        period (int): The period over which to calculate the VWMA.
    
    Returns:
        pd.DataFrame: Original DataFrame with a new column 'vwma' for the specified period.
    """
    # Calculate the product of the closing price and volume
    df['price_volume'] = df['close'] * df['volume']
    # Calculate the sum of price_volume over the given period
    df['sum_price_volume'] = df['price_volume'].rolling(window=period).sum()
    # Calculate the sum of volume over the given period
    df['sum_volume'] = df['volume'].rolling(window=period).sum()
    # Calculate VWMA
    df['vwma'] = df['sum_price_volume'] / df['sum_volume']
    # Clean up by dropping the intermediate calculation columns
    df.drop(columns=['price_volume', 'sum_price_volume', 'sum_volume'], inplace=True)
    
    return df

def calculate_ppo(df, short_period=12, long_period=26, signal_period=9):
    """
    Calculate the Percentage Price Oscillator (PPO) and its signal line.
    
    Parameters:
        df (pd.DataFrame): DataFrame with a 'close' column.
        short_period (int): The period for the short-term EMA.
        long_period (int): The period for the long-term EMA.
        signal_period (int): The period for the signal line EMA.
    
    Returns:
        pd.DataFrame: Original DataFrame with new columns 'ppo', 'ppo_signal' for the PPO and its signal line.
    """
    # Calculate the short and long term EMAs of the closing prices
    ema_short = df['close'].ewm(span=short_period, adjust=False).mean()
    ema_long = df['close'].ewm(span=long_period, adjust=False).mean()

    # Calculate the PPO
    df['ppo'] = ((ema_short - ema_long) / ema_long) * 100
    
    # Calculate the PPO signal line
    df['ppo_signal'] = df['ppo'].ewm(span=signal_period, adjust=False).mean()

    return df

def calculate_ichimoku_cloud(df):
    """
    Calculate the Ichimoku Cloud components for a given DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame with 'high', 'low', and 'close' columns.
    
    Returns:
        pd.DataFrame: Original DataFrame with Ichimoku Cloud components added.
    """
    # Tenkan-sen (Conversion Line)
    df['tenkan_sen'] = (df['high'].rolling(window=9).max() + df['low'].rolling(window=9).min()) / 2

    # Kijun-sen (Base Line)
    df['kijun_sen'] = (df['high'].rolling(window=26).max() + df['low'].rolling(window=26).min()) / 2

    # Senkou Span A (Leading Span A)
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)

    # Senkou Span B (Leading Span B)
    df['senkou_span_b'] = ((df['high'].rolling(window=52).max() + df['low'].rolling(window=52).min()) / 2).shift(26)

    # Chikou Span (Lagging Span)
    df['chikou_span'] = df['close'].shift(-26)

    return df

def add_heikin_ashi_columns(df):
    """
    Add Heikin-Ashi candlestick columns to the given DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame with 'open', 'high', 'low', and 'close' columns.
    
    Returns:
        pd.DataFrame: Original DataFrame with added Heikin-Ashi 'ha_open', 'ha_high', 'ha_low', and 'ha_close'.
    """
    # Initialize Heikin-Ashi columns to avoid NaN for the first row
    df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    df['ha_open'] = ((df['open'] + df['close']) / 2).shift(1)
    df['ha_open'][0] = df['open'][0]  # Set the first Heikin-Ashi open to be the first open price
    
    # Calculate Heikin-Ashi high and low
    df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
    df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
    
    # Recalculate Heikin-Ashi open with a loop to use previous Heikin-Ashi open and close
    for i in range(1, len(df)):
        df.loc[i, 'ha_open'] = (df.loc[i-1, 'ha_open'] + df.loc[i-1, 'ha_close']) / 2
    
    return df

def calculate_elder_ray_index(df, period=13):
    """
    Calculate the Elder Ray Index components (Bull Power and Bear Power) and add them to the DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame with 'high', 'low', and 'close' columns.
        period (int): The period for the exponential moving average (EMA).
        
    Returns:
        pd.DataFrame: Original DataFrame with added 'bull_power' and 'bear_power' columns.
    """
    # Calculate the EMA of the closing prices
    ema_close = df['close'].ewm(span=period, adjust=False).mean()
    
    # Calculate Bull Power and Bear Power
    df['bull_power'] = df['high'] - ema_close
    df['bear_power'] = df['low'] - ema_close
    
    return df

def calculate_zig_zag(df, percentage=5):
    """
    Calculate the Zig Zag indicator by identifying significant price changes.
    
    Parameters:
        df (pd.DataFrame): DataFrame with a 'close' column representing closing prices.
        percentage (float): The percentage movement required to constitute a Zig Zag point.
        
    Returns:
        pd.Series: A Series with the same index as df, where non-Zig Zag points are NaN and Zig Zag points are the 'close' price.
    """
    # Initialize Zig Zag series with NaN
    zig_zag = pd.Series(index=df.index, dtype=float)
    
    # Set the first point
    last_zig_zag = df.iloc[0]['close']
    zig_zag.iloc[0] = last_zig_zag
    
    # Direction: 1 for up, -1 for down
    direction = 0
    
    for i in range(1, len(df)):
        # Percentage move since last zig zag point
        move_perc = ((df.iloc[i]['close'] - last_zig_zag) / last_zig_zag) * 100
        
        # Check for significant move
        if abs(move_perc) >= percentage:
            # Determine direction
            if move_perc > 0 and direction != 1:
                direction = 1  # Change direction to up
                zig_zag.iloc[i] = df.iloc[i]['close']
                last_zig_zag = df.iloc[i]['close']
            elif move_perc < 0 and direction != -1:
                direction = -1  # Change direction to down
                zig_zag.iloc[i] = df.iloc[i]['close']
                last_zig_zag = df.iloc[i]['close']
                
    return zig_zag