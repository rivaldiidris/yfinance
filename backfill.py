import datetime
from yfinance_function import stock_data, convert_avro, load_bigquery
from dotenv import load_dotenv
from os import getenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = getenv('GOOGLE_CLOUD_STORAGE_KEY')

quotes_1 = 'aali.jk'
quotes_2 = 'pwon.jk'
quotes_3 = 'auto.jk'
quotes_4 = 'bbca.jk'
quotes_5 = 'untr.jk'

table_id_1 = 'aali_jk_table'
table_id_2 = 'pwon_jk_table'
table_id_3 = 'auto_jk_table'
table_id_4 = 'bbca_jk_table'
table_id_5 = 'untr_jk_table'

base_currency = 'usd'
target_currency = 'idr'

# today_date = datetime.date.today() + datetime.timedelta(days=1)
# target_date = "20210101"
# target_date_obj = datetime.datetime.strptime(target_date, "%Y%m%d").date()

# while today_date > target_date_obj:
#     today_date.strftime("%Y-%m-%d")
#     today_date -= datetime.timedelta(days=1)
    
#     today_date_str = str(today_date)
#     today_date_str2 = str(today_date).replace('-','')
#     today_date_int = int(today_date_str2)
    
    # end_date = today_date + datetime.timedelta(days=1)
    # end_date_str = str(end_date)
    # end_date_str2 = str(end_date_str).replace('-', '')
    # print(end_date)
    
    # if today_date >= target_date_obj:
        # stock_data(quotes_5, today_date_str, end_date_str)
        # convert_avro(quotes_5, today_date_str)
        # load_bigquery(table_id_1, quotes_1, 'WRITE_TRUNCATE')
    #     print(f"convert data on {today_date}")
    # else:
    #     print(f"data from {target_date} successfully converted")
        
load_bigquery(table_id_1, quotes_1, 'WRITE_TRUNCATE')
        