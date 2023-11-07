import io
import pandas as pd
import yfinance as yf
import json
import gcsfs
import fastavro
import datetime
from fastavro import reader, writer, parse_schema
from google.cloud import storage
from google.cloud.storage import Blob
from dotenv import load_dotenv
from os import getenv

load_dotenv()
gcs_key = getenv('GOOGLE_CLOUD_STORAGE_KEY')

def stock_data(quotes:str.upper, start_date, end_date):
    
    fetch_stock = yf.download(quotes, start=start_date, end=end_date, interval="1d")
    
    fetch_stock['Date'] = fetch_stock.index
    fetch_stock.reset_index(drop=True, inplace=True)
    
    fetch_stock = fetch_stock[['Date','Open', 'High', 'Low', 'Close']]
    
    stock_data_json = fetch_stock.to_json(orient='records')
    stock_data_json = json.loads(stock_data_json)
    
    # with self.gfs.open(f'gs://raw-yfinance/stock-data/{quotes}/{quotes}-{self.start_date}.json', 'w') as file_in:
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
    

def currency(self, base_currency:str.upper, target_currency:str.upper, start_date, end_date=None):
    
    fetch_currency = yf.download(f'{base_currency}{target_currency}=X', start=start_date, end=end_date, interval="1d")

    fetch_currency['Date'] = fetch_currency.index
    fetch_currency.reset_index(drop=True, inplace=True)

    fetch_currency = fetch_currency[['Date' ,'Open']]

    fetch_currency_json = fetch_currency.to_json(orient='records')
    fetch_currency_json = json.loads(fetch_currency_json)
    
    with self.gfs.open(f'gs://raw-yfinance/currency-data/{base_currency}{target_currency}-{start_date}.json', 'w') as file_in:
    # with open(f'/home/rivaldi/OTW_DE/yfinance/output/{base_currency}{target_currency}-{start_date}', 'w') as file_in:
    
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

def avro_stock_data():
    
    try:
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
        
        gcs_key = getenv('GOOGLE_CLOUD_STORAGE_KEY')
        
        creds = storage.Client.from_service_account_info(json.loads(gcs_key))
        client = storage.Client(project='pristine-valve-272815', credentials=creds)

        with storage.open('gs://clean_data_aldi/auto.jk.avro', 'wb') as avro_file:
            with storage.open('gs://raw_data_aldi/api/yfinance/stock-data/auto.jk/auto.jk-2023-07-08.json', 'rb') as json_file:
                json_data = json.load(json_file)

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

            # Write the Avro data to the GCS file
            avro_file.write(avro_bytes)

        print("Successfully created Avro file")
        
    except:
        pass
        print("file or directory doesn't exist")
        

def read_file_from_gcs():
    

    creds = storage.Client.from_service_account_info(json.loads(gcs_key))
    
    client = storage.Client(project='pristine-valve-272815', credentials=creds)

    # Get the bucket object
    bucket_name = 'raw_data_aldi'
    bucket = client.get_bucket(bucket_name)

    # Get the blob (file) object within the bucket
    blob_path = 'api/yfinance/stock-data/auto.jk/auto.jk-2023-07-08.json'
    blob = Blob.from_string(blob_path, bucket)

    # Read the file content
    file_content = blob.download_as_text()

    # Parse the file content as JSON
    json_data = json.loads(file_content)
    print(json_data)

if __name__ == "__main__":
    
    stock_data('bbca.jk', '2023-05-09', '2023-05-10')
    
    
        
# avro_stock_data()

# if __name__ == "__main__":
    # date = datetime.date.today()
    # date = date.strftime('%Y-%m-%d')
    # today_date = datetime.date.today() + datetime.timedelta(days=1)
    # target_date = "20210101"
    # target_date_obj = datetime.datetime.strptime(target_date, "%Y%m%d").date()
    
    # while today_date > target_date_obj:
    
    #     today_date.strftime("%Y-%m-%d")
    #     today_date -= datetime.timedelta(days=1)
    #     end_date = today_date + datetime.timedelta(days=1)
        
    #     today_date_str = str(today_date)
    #     end_date_str = str(end_date)
    #     stock_data('aali.jk', today_date_str, end_date_str)
    # stock_data('aali.jk', '2021-01-04', '2021-01-05')
    
