#from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import configparser
class Stock:
    # constructor
    def __init__(self):
        self = self
    # scrape
    def scrape(self):
        url = 'https://finance.yahoo.com/screener/predefined/most_actives?count=100&offset=0'
        headers = {"User-Agent" : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
        request = requests.get(url, headers = headers)
        data = pd.read_html(request.text)[0]
        return data
    def analysis(self,data):
        change = data.sort_values(['Change'],ascending=[False]).head(40)
        change_rate = data.sort_values(['% Change'],ascending=[False]).head(40)
        volumn = data.sort_values(['Volume'],ascending=[False]).head(40)
        intersected_df = pd.merge(change, volumn,  how='inner')
        return intersected_df
    def save(self, data):
        
        conf = configparser.ConfigParser()
        
            
        conf.read(r"/Users/tiffanychao/Desktop/crawler/configfile.ini")      
        dbparam = conf["mysql"]
        res = dbparam["server"]
        engine = create_engine(res)
        try:
            conn = engine.connect()
            conn.execute("TRUNCATE TABLE stocktable_us")
            #df = pd.read_sql_query(sql, engine)
            stock_list.to_sql('stocktable_us',engine,if_exists='append')
            
            conn.close()
            engine.dispose()
            return stock_list
        except Exception as ex:
            print("Exception:", ex)
stock = Stock()  # create stock
df = stock.scrape()

stock_list = stock.analysis(df)
final_list = stock.save(stock_list)
print(final_list)
