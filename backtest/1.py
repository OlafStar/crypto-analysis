import pandas as pd

# Assuming klines_df is already loaded into your DataFrame
klines_df = pd.read_csv('./klines_data/BTCUSDT_1m_klines.csv')

open_positions = []
closed_positions = []

account_balance = 10000
risk_per_trade = 0.01
leverage = 10

# Variables to count and sum profitable and lost trades
profitable_trades_count = 0
lost_trades_count = 0
total_earned = 0
total_lost = 0

for index, row in klines_df.iterrows():
    buy_signal = row['5_EMA'] > row['10_SMA'] and row['RSI'] < 30
    sell_signal = row['5_EMA'] < row['10_SMA'] or row['RSI'] > 70

    trade_size = account_balance * risk_per_trade * leverage
    # Check for a buy signal to open a new long position
    if buy_signal:
        new_position = {
            'entry_time': row['time'], 
            'entry_price': row['close'], 
            'trade_size': trade_size
        }
        open_positions.append(new_position)
        print(f"Opened new position at time {new_position['entry_time']} with entry price {new_position['entry_price']}, trade size: {trade_size}")

    # Check existing open positions for a sell signal
    for position in open_positions[:]:
        if sell_signal:
            # Close position
            position['exit_time'] = row['time']
            position['exit_price'] = row['close']
            closed_positions.append(position)
            open_positions.remove(position)
            
            profit_loss = (position['exit_price'] - position['entry_price']) * (position['trade_size'] / position['entry_price'])
            account_balance += profit_loss

            print(f"Closed position opened at time {position['entry_time']} with entry price {position['entry_price']}, "
                  f"closed at {position['exit_time']} with exit price {position['exit_price']}. "
                  f"Profit/Loss: {profit_loss}, New Balance: {account_balance}")

            # Count and sum up earnings or losses from the closed position
            if profit_loss > 0:
                profitable_trades_count += 1
                total_earned += profit_loss
            else:
                lost_trades_count += 1
                total_lost += profit_loss

# Calculate total profit/loss
total_profit_loss = total_earned + total_lost

print(f"\nTotal Profit/Loss: {total_profit_loss}")
print(f"Number of Profitable Trades: {profitable_trades_count}")
print(f"Number of Lost Trades: {lost_trades_count}")
print(f"Win rate: {(profitable_trades_count/(profitable_trades_count + lost_trades_count)) * 100}%")
print(f"Total Earned from Profitable Trades: {total_earned}")
print(f"Total Lost from Lost Trades: {total_lost}")
print(f"\nFinal Account Balance: {account_balance}")
