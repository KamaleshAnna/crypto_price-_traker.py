#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import psycopg2
from datetime import datetime
import time
import signal
import logging


# In[2]:


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# In[3]:


DB_NAME = 'cryptodatabase'
DB_USER = 'kamalesh'
DB_PASSWORD = 'Seth@123'
DB_HOST = 'localhost'
DB_PORT = '5432'


# In[4]:


def create_table():
    logging.info("Creating table if not exists.")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            price NUMERIC,
            timestamp TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()

create_table()


# In[5]:


def store_data(symbol, price):
    logging.info(f"Inserting data for {symbol} at price {price}.")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO crypto_prices (symbol, price, timestamp)
        VALUES (%s, %s, %s)
    ''', (symbol, price, datetime.now()))
    conn.commit()
    cursor.close()
    conn.close()


# In[6]:


def fetch_prices():
    logging.info("Fetching prices from CoinMarketCap.")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '5000',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '0c5b2787-88ef-478f-848b-f7e26d7d598a',
    }

    response = requests.get(url, headers=headers, params=parameters)
    data = response.json()
    return data['data']


# In[7]:


class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self, signum, frame):
        self.kill_now = True

def main():
    killer = GracefulKiller()
    create_table()
    symbols = ['BTC', 'ETH', 'USDC', 'USDT', 'XLM', 'TRX']

    while not killer.kill_now:
        logging.info("Starting data fetch and insert cycle.")
        prices = fetch_prices()
        for crypto in prices:
            if crypto['symbol'] in symbols:
                store_data(crypto['symbol'], crypto['quote']['USD']['price'])
        logging.info("Cycle complete, sleeping for 30 seconds.")
        time.sleep(30)

main()

