import pandas as pd
import os
from pathlib import Path
import sys

script_dir = Path(__file__).parent  # Directory of the script
parent_dir = script_dir.parent  # Parent directory
sys.path.append(str(parent_dir))

# Now you can import from utils.py
from utils import (add_date_column, calculate_5_ema, calculate_10_sma, calculate_rsi, calculate_macd, calculate_bollinger_bands, calculate_vwap, calculate_obv, calculate_stochastic_oscillator, calculate_atr, calculate_ad_line, calculate_cci, calculate_pivot_points, calculate_momentum, calculate_standard_deviation, calculate_fibonacci_retracement, calculate_roc, calculate_support_resistance, calculate_vwma, calculate_ppo, calculate_ichimoku_cloud, add_heikin_ashi_columns, calculate_elder_ray_index, calculate_zig_zag)

def read_and_process_files(source_dir, target_dir):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    
    for filename in os.listdir(source_dir):
        if filename.endswith(".csv"):
            print(f"Processing {filename}")
            klines_df = pd.read_csv(os.path.join(source_dir, filename))
            
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
            klines_df = calculate_support_resistance(klines_df)
            klines_df = calculate_vwma(klines_df, period=20)
            klines_df = calculate_ppo(klines_df)
            klines_df = calculate_ichimoku_cloud(klines_df)
            klines_df = add_heikin_ashi_columns(klines_df)
            klines_df = calculate_elder_ray_index(klines_df)
            klines_df['zig_zag'] = calculate_zig_zag(klines_df, percentage=5)
            klines_df['zig_zag'] = klines_df['zig_zag'].ffill()

            klines_df.to_csv(os.path.join(target_dir, filename), index=False)
            print(f"Saved processed data for {filename}")

source_directory = "historical/data"  # Original files directory
target_directory = "historical/data_with_indicators"  # New directory for processed files

read_and_process_files(source_directory, target_directory)
