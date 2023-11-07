import pandas as pd
import yfinance as yf
import json
import gcsfs
import fastavro
import io
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from os import getenv


load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = getenv('GOOGLE_CLOUD_STORAGE_KEY')


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
        
        gfs = gcsfs.GCSFileSystem(project='pristine-valve-272815', token=json.loads(GOOGLE_APPLICATION_CREDENTIALS))
        
        # with open(f'/home/rivaldi/OTW_DE/yfinance/output/{quotes}-{start_date}.json', 'w') as file_in:
        with gfs.open(f'gs://raw_data_aldi/api/yfinance/stock-data/{quotes}/{quotes}-{start_date}.json', 'w') as file_in:
            
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


def currency(base_currency:str.upper, target_currency:str.upper, start_date, end_date=None):
    
    fetch_currency = yf.download(f'{base_currency}{target_currency}=X', start=start_date, end=end_date, interval="1d")

    fetch_currency['Date'] = fetch_currency.index
    fetch_currency.reset_index(drop=True, inplace=True)

    fetch_currency = fetch_currency[['Date' ,'Open']]

    fetch_currency_json = fetch_currency.to_json(orient='records')
    fetch_currency_json = json.loads(fetch_currency_json)
    
    # with self.gfs.open(f'gs://raw-yfinance/currency-data/{base_currency}{target_currency}-{start_date}.json', 'w') as file_in:
    with open(f'/home/rivaldi/OTW_DE/yfinance/output/{base_currency}{target_currency}-{start_date}', 'w') as file_in:
    
        for iterate_currency_json in fetch_currency_json:
            
            date_column = iterate_currency_json.get('Date')
            
            converted_date = pd.to_datetime(date_column, unit='ms').date()
            converted_date_str = converted_date.strftime('%Y-%m-%d')
            
            conversion_rate = iterate_currency_json.get('Open')
            conversion_rate_str = str(conversion_rate)
            reciprocal_rate = 1 / conversion_rate
            reciprocal_rate_str = "{:.6f}".format(reciprocal_rate)
            
            payload = {
                'Date':converted_date_str,
                f'{base_currency}_{target_currency}':conversion_rate_str,
                f'{target_currency}_{base_currency}':reciprocal_rate_str
            }
            
            currency_data = json.dumps(payload)
            file_in.write(currency_data + "\n")


def convert_avro(quotes, date):
    try:

        gfs = gcsfs.GCSFileSystem(project='pristine-valve-272815', token=json.loads(GOOGLE_APPLICATION_CREDENTIALS))

        schema =  {
                    "name": "StockData",
                    "type" : "record",
                    "fields" : [
                        {"name": "Date", "type": "string"},
                        {"name": "Quotes", "type": "string"},
                        {"name": "Open", "type": "float"},
                        {"name": "High", "type": "float"},
                        {"name": "Low", "type": "float"},
                        {"name": "Close", "type": "float"}
                    ]
                }

        with gfs.open(f"gs://raw_data_aldi/api/yfinance/stock-data/{quotes}/{quotes}-{date}.json", "rb") as f:
            json_data = json.load(f)   
            
            avro_data = {
                    "Date": json_data["Date"],
                    "Quotes": json_data["Quotes"],
                    "Open": json_data["Open"],
                    "High": json_data["High"],
                    "Low": json_data["Low"],
                    "Close": json_data["Close"]
                }

            # Create a bytes buffer to write the Avro data to
            bytes_buffer = io.BytesIO()

            # Write the Avro data
            fastavro.writer(bytes_buffer, schema, [avro_data])

            # Get the Avro data as bytes
            avro_bytes = bytes_buffer.getvalue()

            with gfs.open(f'gs://clean_data_aldi/api/yfinance/stock-data/{quotes}/{quotes}-{date}.avro', 'wb') as avro_file:
                avro_file.write(avro_bytes)
    except:
        pass
        print("file or directory doesn't exist")

def load_bigquery(table_id, quotes, write_disposition='WRITE_APPEND'):

    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_APPLICATION_CREDENTIALS))

        bq_client = bigquery.Client(project="pristine-valve-272815", credentials=creds)
        destination_tabel = bq_client.dataset('stock_data_datalake').table(table_id)
        

        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.AVRO,
            autodetect = True,
            write_disposition = write_disposition
        )
        
        uri = f'gs://clean_data_aldi/api/yfinance/stock-data/{quotes}/*.avro'
            
        load_job = bq_client.load_table_from_uri(
    
            uri, 
            destination_tabel, 
            job_config=job_config
            
            )
        
        load_job.result()
        
        print(f"Data loaded")
        
    except:
        pass
        print('file or directory not exist')

if __name__ == "__main__":
    stock_data('bbca.jk', '2023-07-08', '2023-07-09')