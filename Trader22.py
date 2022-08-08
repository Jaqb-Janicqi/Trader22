import pandas as pd
import yfinance as yf
# keras      builds neural network layer by layer
from binance import Client
from datetime import datetime, timedelta
import os
import time
import csv

def initialize_client():
    keys = pd.read_csv('api_keys.csv', delimiter=',')
    client = Client(keys.loc[0, 'key'], keys.loc[0, 'secret'])
    return client

def str_to_datetime(date_str):
    return pd.to_datetime(date_str)

def datetime_to_str(date_time):
    return date_time.strftime('%Y-%m-%d')

def construct_file_name_from_date(date):
    filename = '{}.csv'.format(str_to_datetime(date))
    filename = filename.replace(':', '-')
    filename = filename.replace(' ', '_')
    return filename

def frame_to_csv(frame, name, path):
    if not os.path.exists(path):
        os.makedirs(path)
    frame.to_csv(path + '/' + name)

def get_price_data(symbol, current_date, end_date, interval):   # download and save raw data day by day, construct final frame from already downloaded data
    final_frame = frame = pd.DataFrame()
    path = '{}/{}/raw'.format(symbol, interval)
    while current_date < end_date:    # download or retreive data day by day until end_date
        filename = construct_file_name_from_date(current_date)
        if not os.path.exists(path + filename):
            frame = pd.DataFrame(client.get_historical_klines(symbol, interval=interval, start_str=datetime_to_str(current_date), end_str=datetime_to_str(current_date + timedelta(days=1))))
            frame = frame.iloc[:, 0:7]
            frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Number of Trades']
            frame.set_index('Time', inplace=True)
            frame.index = pd.to_datetime(frame.index, unit='ms')
            frame = frame.astype(float)
            frame = frame[frame.index.day == current_date.day] # drop all prices that are not from the same day as current_date (avoid duplicates with next day)
            frame_to_csv(frame, filename, path)
        else:
            frame = pd.read_csv(path + filename)
            frame.set_index('Time', inplace=True)
            frame = frame.astype(float)
        final_frame = pd.concat([final_frame, frame])
        frame = frame[0:0]
        current_date += timedelta(days=1)
    return final_frame

if __name__ == "__main__":
    client = initialize_client()
    symbol = 'BTCUSDT'
    start_date = '2020-01-01'
    end_date = '2020-01-02'
    interval = '15m'
    frame = get_price_data(symbol, str_to_datetime(start_date), str_to_datetime(end_date) + timedelta(minutes=135), interval)
    print(frame)