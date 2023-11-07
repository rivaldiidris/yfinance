import io
import pandas as pd
import yfinance as yf
import json
import gcsfs
import fastavro
import datetime

def stock_data(quotes:str.upper, start_date, end_date):
    
    try:    
        fetch_stock = yf.download(quotes, start=start_date, end=end_date, interval="1d")
        
        if fetch_stock.empty:
            raise ValueError("No data available for the specified date range.")
        
        fetch_stock['Date'] = fetch_stock.index
        fetch_stock.reset_index(drop=True, inplace=True)
        
        fetch_stock = fetch_stock[['Date','Open', 'High', 'Low', 'Close']]
        
        stock_data_json = fetch_stock.to_json(orient='records')
        stock_data_json = json.loads(stock_data_json)

        with open(f'/home/rivaldi/OTW_DE/yfinance/output/{quotes}-{start_date}.json', 'w') as file_in:
            
            for iterate_stock_data in stock_data_json:
                
                date_column = iterate_stock_data.get('Date')
                converted_date = pd.to_datetime(date_column, unit='ms').date()
                converted_date_str = converted_date.strftime('%Y-%m-%d')
                open_column = str(iterate_stock_data.get('Open'))
                high_column = str(iterate_stock_data.get('High'))
                low_column = str(iterate_stock_data.get('Low'))
                close_column = str(iterate_stock_data.get('Close'))
                
                payload = {
                    'Quotes':quotes,
                    'Date':converted_date_str,
                    'Open':open_column,
                    'High':high_column,
                    'Low':low_column,
                    'Close':close_column
                }
                stock_data_payload = json.dumps(payload)
                file_in.write(stock_data_payload + "\n")
    
    except:
        pass
                
                
if __name__ == "__main__":
    
    stock_data("aali.jk", "2023-07-02", "2023-07-03")