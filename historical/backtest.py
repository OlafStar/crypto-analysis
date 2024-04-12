import pandas as pd
import datetime

# Load the CSV file
df = pd.read_csv('./historical/data_with_indicators/BTCUSDT_15m_klines.csv')

# Drop rows with null values
df.dropna(inplace=True)

# Simulation parameters
initial_balance = 10000
balance = initial_balance
leverage = 50
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

# if row['close'] > row['5_EMA'] and 50 < row['RSI'] < 70 and balance > 0:
#         # In futures, the position size is the value you control, not just the margin
#         position_value = balance * risk_per_trade * leverage  # This is the amount at risk, adjusted for leverage
#         entry_price = current_price
#         stop_loss = entry_price * 0.99  # Assuming a 1% stop loss
#         take_profit = entry_price * 1.03  # Assuming a 3% take profit
#         open_positions.append({
#             'entry_price': entry_price,
#             'stop_loss': stop_loss,
#             'take_profit': take_profit,
#             'position_value': position_value
#         })
#         total_trades += 1
#         print(f"Opened Position: Entry Price = {entry_price}, Stop Loss = {stop_loss}, Take Profit = {take_profit}, Position Value = {position_value}")

#     # Check and Manage Open Positions
#     for position in open_positions[:]:
#         position_closed = False
#         if current_price <= position['stop_loss']:
#             # Calculate PnL based on position value and price movement
#             pnl_percentage = (position['stop_loss'] - position['entry_price']) / position['entry_price']
#             pnl = pnl_percentage * position['position_value']
#             lost_trades += 1
#             position_closed = True
#         elif current_price >= position['take_profit']:
#             pnl_percentage = (position['take_profit'] - position['entry_price']) / position['entry_price']
#             pnl = pnl_percentage * position['position_value']
#             profitable_trades += 1
#             position_closed = True
        
#         if position_closed:
#             # Update balance
#             balance += pnl
#             # Remove position from open_positions
#             open_positions.remove(position)
#             print(f"Closed Position: Entry Price = {position['entry_price']}, Closed Price = {current_price}, PnL = {pnl:.2f}")


# def check_buy_signal(row, previous):
#     # Trend confirmation
#     trend_confirmation = row['5_EMA'] > row['10_SMA']
    
#     # Momentum confirmation
#     momentum_confirmation = (row['Momentum'] > 0) and (row['MACD'] > row['MACD_Signal'])
    
#     # Volume confirmation
#     avg_volume = row['cum_volume'] / 14  # Assuming a 14-day period for average calculation
#     volume_confirmation = row['volume'] > avg_volume
    
#     # Bollinger Band bounce
#     bollinger_bounce = row['close'] > row['Lower_BB']
    
#     # RSI and Stochastic confirmation
#     rsi_stochastic_confirmation = (row['RSI'] < 70) and (row['%K'] > row['%D'])
    
#     # ATR and CCI confirmation
#     volatility_confirmation = row['ATR'] > 0  # Simplified check, can be enhanced based on strategy
#     cci_confirmation = -100 < row['CCI'] < 100
    
#     # Rate of Change confirmation
#     roc_confirmation = row['ROC'] > 0
    
#     # All conditions must be met for a buy signal
#     buy_signal = (trend_confirmation and momentum_confirmation and volume_confirmation and 
#                   bollinger_bounce and rsi_stochastic_confirmation and volatility_confirmation and 
#                   cci_confirmation and roc_confirmation)
    
#     return buy_signal
def check_buy_signal(df):
    global open_positions
    row = df.iloc[index]
    previous_row = df.iloc[index - 1]
    previous2_row = df.iloc[index - 2]
    previous3_row = df.iloc[index - 3]


    # Trend confirmation
    trend_confirmation = row['5_EMA'] > row['10_SMA']
    trend_confirmation2 = row['5_EMA'] > row['10_SMA'] and previous_row['5_EMA'] <= previous_row['10_SMA']

    # RSI confirmation
    rsi_confirmation = row['RSI'] > 30 and row['RSI'] < 50

    # Bull power
    bull_power = previous3_row['bull_power'] < previous2_row['bull_power'] < previous_row['bull_power']

    # Bear power
    bear_power = previous3_row['bear_power'] > previous2_row['bear_power'] > previous_row['bear_power']
    
    # Momentum confirmation
    momentum_confirmation = (row['Momentum'] > 0) and (row['MACD'] > row['MACD_Signal'])
    momentum_confirmation2 = row['RSI'] > 50 and row['MACD'] > row['MACD_Signal']

    # PPO check
    ppo_confirmation = previous2_row['ppo'] > previous_row['ppo']
    
    # Volume confirmation
    avg_volume = row['cum_volume'] / 7  # Assuming a 14-day period for average calculation
    volume_confirmation = row['volume'] > avg_volume
    volume_confirmation2 = row['close'] > row['VWAP'] and row['OBV'] > previous_row['OBV']

    
    # Bollinger Band bounce
    bollinger_bounce = row['close'] > row['Lower_BB']
    
    # RSI and Stochastic confirmation
    rsi_stochastic_confirmation = (row['RSI'] < 70) and (row['%K'] > row['%D'])
    
    # ATR and CCI confirmation
    volatility_confirmation = row['ATR'] > 0  # Simplified check, can be enhanced based on strategy
    cci_confirmation = -100 < row['CCI'] < 100

    # Volatility Conditions
    volatility_confirmation = row['close'] > row['Lower_BB'] and row['close'] < row['Upper_BB']

    
    # Rate of Change confirmation
    roc_confirmation = row['ROC'] > 0

    roc_ppo_conf = row['ROC'] > row['ppo']

    bullish_engulfing = row['open'] < previous_row['close'] and row['close'] > previous_row['open']
    
    # All conditions must be met for a buy signal
    # buy_signal = (trend_confirmation and momentum_confirmation and volume_confirmation and 
    #               bollinger_bounce and rsi_stochastic_confirmation and volatility_confirmation and 
    #               cci_confirmation and roc_confirmation)

    #47%
    # buy_signal = (trend_confirmation and momentum_confirmation and 
    #               bull_power and bear_power and ppo_confirmation)

    buy_signal = (bullish_engulfing and trend_confirmation2 and momentum_confirmation2 and volatility_confirmation and volume_confirmation2)
    
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
        stop_loss = entry_price * 0.95
        take_profit = entry_price * 1.15
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

