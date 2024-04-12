class Position:
    def __init__(self, entry_time, entry_price, position_type):
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.position_type = position_type  # e.g., "long"

open_positions = []

def calculate_positions(klines_df):
    open_positions  

    required_columns = ['close', '5_EMA', '10_SMA', 'RSI', 'MACD', 'MACD_Signal', 'Upper_BB', 'Lower_BB', 'VWAP', 'time']
    if not all(column in klines_df.columns for column in required_columns):
        return

    if klines_df[required_columns].tail(2).isnull().values.any():
        return
    
    current = klines_df.iloc[-1]
    previous = klines_df.iloc[-2]

    # conditions 
    ema_condition = current['5_EMA'] > current['10_SMA'] and previous['5_EMA'] <= previous['10_SMA']
    macd_condition = current['MACD'] > current['MACD_Signal'] and previous['MACD'] <= previous['MACD_Signal']
    rsi_condition = current['RSI'] < 70

    if ema_condition and rsi_condition and macd_condition:
        # Open a new long position
        new_position = Position(entry_time=current['time'], entry_price=current['close'], position_type="long")
        open_positions.append(new_position)

    for position in list(open_positions):
        stop_loss_percentage = 0.99
        take_profit_percentage = 1.01
        stop_loss_price = position.entry_price * stop_loss_percentage
        take_profit_price = position.entry_price * take_profit_percentage

        ema_sell_condition = current['5_EMA'] < current['10_SMA'] and previous['5_EMA'] >= previous['10_SMA']
        macd_sell_condition = current['MACD'] < current['MACD_Signal'] and previous['MACD'] >= previous['MACD_Signal']
        rsi_sell_condition = current['RSI'] > 70  # Overbought condition for selling

        if (ema_sell_condition or macd_sell_condition or rsi_sell_condition) or current['close'] <= stop_loss_price or current['close'] >= take_profit_price:
            # Close the position
            print(f"Closing position: Entry Price: {position.entry_price}, Close Price: {current['close']}, Count: {current['close'] - position.entry_price}, %: {(current['close'] - position.entry_price)/position.entry_price * 100}%")
            open_positions.remove(position)