import pandas as pd
import datetime

# Load the CSV file
df = pd.read_csv('./historical/data_with_indicators/BTCUSDT_15m_klines.csv')

# Drop rows with null values
df.dropna(inplace=True)

# Simulation parameters
initial_balance = 10000
balance = initial_balance
leverage = 100
win_lose_value = 3
risk_per_trade = 0.01
open_positions = []
monthly_results = []
total_trades = 0
profitable_trades = 0
lost_trades = 0
id_positions = 0

def create_monthly_result(position, isWin):
    date = position['date']

    position_date = datetime.datetime.strptime(date, "%Y-%m-%d")

    result_obj = {
       'date': date,
       'total_trades': 1,
       'win_trades': 0,
       'lost_trades': 0,
    }

    if isWin == True:
        result_obj['win_trades'] += 1
    else:
        result_obj['lost_trades'] += 1

    found = False

    for mres in monthly_results:
        result_date = datetime.datetime.strptime(mres['date'], "%Y-%m-%d")
        if result_date.month == position_date.month and result_date.year == position_date.year:
            mres['total_trades'] += 1
            if isWin:
                mres['win_trades'] += 1
            else:
                mres['lost_trades'] += 1
            found = True
            break 

    if not found:
        monthly_results.append(result_obj)

def print_monthly_results(monthly_results):
    print("Monthly Trading Results:")
    print("-" * 60)  # Print a divider for clarity
    print(f"{'Date':<12} | {'Total Trades':<12} | {'Win Trades':<10} | {'Lost Trades':<10}")
    print("-" * 60)  # Print a divider for clarity
    total = 0
    total_win = 0
    total_lost = 0
    
    for result in monthly_results:
        # Format the date to only show month and year
        result_date = datetime.datetime.strptime(result['date'], "%Y-%m-%d")
        formatted_date = result_date.strftime("%Y-%m")

        total += result['total_trades']
        total_win += result['win_trades']
        total_lost += result['lost_trades']
        
        print(f"{formatted_date:<12} | {result['total_trades']:<12} | {result['win_trades']:<10} | {result['lost_trades']:<10}")

    print(f"Total trades: {total}")
    print(f"Total trades: {total_win}")
    print(f"Total trades: {total_lost}")


weights = {
    'RSI_Condition': 0.25,  # Adjusted to consider overbought conditions more cautiously
    'MACD_Momentum': 0.25,  # Momentum via MACD
    'EMA_SMA_Trend': 0.25,  # Trend confirmation via EMA/SMA crossover
    'Volatility_Risk': 0.25,  # Risk management via ATR
}

def calculate_signal_strength(row, previous_row):
    signal_strength = 0

    # Tightening bullish conditions
    if 30 <= row['RSI'] < 70:  # RSI in a neutral zone, avoiding overbought conditions
        signal_strength += weights['RSI_Condition']
    if row['MACD'] > row['MACD_Signal'] and previous_row['MACD'] <= previous_row['MACD_Signal']:
        signal_strength += weights['MACD_Momentum']
    if row['5_EMA'] > row['10_SMA'] and previous_row['5_EMA'] <= previous_row['10_SMA']:
        signal_strength += weights['EMA_SMA_Trend']

    # Incorporating risk management via volatility
    if row['ATR'] / row['close'] < 0.01:  # Assuming a low ATR compared to price as lower risk
        signal_strength += weights['Volatility_Risk']
    else:
        signal_strength -= weights['Volatility_Risk']  # High volatility decreases signal strength

    return signal_strength

def check_buy_signal(df):
    global open_positions
    row = df.iloc[index]
    previous_row = df.iloc[index - 1]
    previous2_row = df.iloc[index - 2]
    previous3_row = df.iloc[index - 3]

    signal_strength = calculate_signal_strength(row, previous_row)

    buy_signal = signal_strength > 0.5
    
    if buy_signal:
      if open_positions.__len__() > 0:
        for position in open_positions:
          if position['entry_price'] % row['open'] < 100:
              return False
          
          return True
      
      return True
      

def check_current_positions(row):
    global lost_trades, profitable_trades, balance

    open_price = row['open']
    close_price = row['close']

    for position in open_positions:
        stop_loss = position['stop_loss']
        take_profit = position['take_profit']

        if open_price <= stop_loss <= close_price or close_price <= stop_loss <= open_price:
            print(f"Position {position['id']} hits stop loss -{usdt}$")
            lost_trades += 1
            open_positions.remove(position)
            create_monthly_result(position, False)
            continue
        
        if open_price <= take_profit <= close_price or close_price <= take_profit <= open_price:
            print(f"Position {position['id']} hits take profit {usdt * 4}$")
            profitable_trades += 1
            balance = balance + (position['usdt'] * 4)
            open_positions.remove(position)
            create_monthly_result(position, True)
            continue

for index in range(1, len(df)):
    current_row = df.iloc[index]
    previous_row = df.iloc[index - 1]

    current_price = current_row['close']

    if balance < initial_balance - balance * 0.5 and len(open_positions) == 0:
       break

    # Entry Signal
    if check_buy_signal(df) and balance > initial_balance - balance * 0.5:
        usdt = balance * risk_per_trade
        position_value = usdt * leverage  
        balance -= usdt
        entry_price = current_price
        stop_loss = entry_price * 0.99
        take_profit = entry_price * 1.03
        id_positions += 1
        current_position = {
            'id': id_positions,
            'date': current_row['date'],
            'usdt': usdt,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'position_value': position_value
        }
        open_positions.append(current_position)
        total_trades += 1
        print(f"Opened Position Id: {id_positions}: Entry price = {entry_price}, Stop Loss = {stop_loss}, Take Profit = {take_profit}, Position Value = {position_value}")

    check_current_positions(current_row)
    

# Ensure balance does not go below zero
balance = max(0, balance)

# Final Balance Calculation and Output
percentage_gain_loss = ((balance - initial_balance) / initial_balance) * 100 if initial_balance > 0 else 0

print(f'Final Balance: ${balance:.2f}')
print(f'Total Trades: {total_trades}')
print(f'Profitable Trades: {profitable_trades}')
print(f'Lost Trades: {lost_trades}')
print(f'Win Ratio: {(profitable_trades/total_trades) * 100}%')
print(f'Percentage Gain/Loss: {percentage_gain_loss:.2f}%')
print_monthly_results(monthly_results)

