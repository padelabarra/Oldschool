import yfinance as yf
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

# Get historical data for Tesla
tesla = yf.Ticker("TSLA").history(period="max")

# Create a DataFrame with the historical data
tesla_df = pd.DataFrame(tesla)

# Calculate the moving average and standard deviation
tesla_df['MA'] = tesla_df['Close'].rolling(window=20).mean()
tesla_df['STD'] = tesla_df['Close'].rolling(window=20).std()

# Calculate the upper and lower Bollinger Bands
tesla_df['Upper'] = tesla_df['MA'] + (tesla_df['STD'] * 2)
tesla_df['Lower'] = tesla_df['MA'] - (tesla_df['STD'] * 2)

# Bollinger Band width
tesla_df['BB_Width'] = (tesla_df['Upper'] - tesla_df['Lower']) / tesla_df['MA']

# Plot the Bollinger Bands
plt.figure(figsize=(15,5))
plt.plot(tesla_df['Close'], label='TSLA')
plt.plot(tesla_df['Upper'], label='Upper Band')
plt.plot(tesla_df['Lower'], label='Lower Band')
plt.legend(loc='upper left')
plt.show()

# Initialize the short and long windows
short_window = 5
long_window = 20

# Create short and long simple moving averages (SMA)
tesla_df['short_ma'] = tesla_df['Close'].rolling(window=short_window).mean()
tesla_df['long_ma'] = tesla_df['Close'].rolling(window=long_window).mean()

# Create signals
tesla_df['signal'] = np.where(tesla_df['short_ma'] > tesla_df['long_ma'], 1, 0)
tesla_df['positions'] = tesla_df['signal'].diff()

# Create a DataFrame of the position and the returns of each trade
tesla_df['returns'] = np.log(tesla_df['Close'] / tesla_df['Close'].shift(1))
tesla_df['strategy'] = tesla_df['returns'] * tesla_df['positions']

# Plot the returns of the strategy
plt.figure(figsize=(15,5))
plt.plot(tesla_df['returns'], label='Returns')
plt.plot(tesla_df['strategy'], label='Strategy')
plt.legend(loc='upper left')
plt.show()

