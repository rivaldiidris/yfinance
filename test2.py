import yfinance as yf

# List of stock symbols
stocks = ['AAPL', 'GOOGL', 'AMZN', 'MSFT']

# Loop through each stock symbol and fetch data
for stock_symbol in stocks:
    # Fetch data for the stock symbol from 1st January 2021 to now
    stock_data = yf.download(stock_symbol, start='2021-01-01', end=None)

    # Print the stock symbol and its data
    print('Stock Symbol:', stock_symbol)
    print(stock_data)
    print('\n')  # Add a newline for better readability
