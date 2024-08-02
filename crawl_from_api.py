import requests
import datetime
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

def get_binance_klines(symbol, interval, end_time, fetch_limit=1000):
    all_klines = []
    current_time = end_time
    
    while True:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&endTime={current_time}&limit={fetch_limit}"
        response = requests.get(url)
        
        if response.status_code == 200:
            klines = response.json()
            num_records = len(all_klines)
            print(f"Fetched {num_records} records for {symbol} with interval {interval}")
            
            if not klines:
                break
            all_klines = klines + all_klines
            current_time = klines[0][0] - 1  # Update current_time to the time of the first kline fetched
            time.sleep(0.1)  # Respect rate limits
        else:
            print(f"Error fetching data: {response.status_code} for {symbol} with interval {interval}")
            response.raise_for_status()
        
    return all_klines

def save_klines_to_csv(klines, symbol, interval, folder='csv'):
    # Create folder if it doesn't exist
    os.makedirs(folder, exist_ok=True)
    
    # Select only the desired columns
    selected_columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time']
    df = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 
        'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 
        'Taker Buy Quote Asset Volume', 'Ignore'])
    df = df[selected_columns]
    
    filename = os.path.join(folder, f'{symbol}_{interval}.csv')
    df.to_csv(filename, index=False)
    print(f"Kline data for {symbol} with interval {interval} saved to {filename}")

def fetch_and_save_klines(symbol, interval, end_time):
    print(f"Fetching kline data for {symbol} with interval {interval}...")
    klines = get_binance_klines(symbol, interval, end_time)
    print(f"Saving kline data for {symbol} with interval {interval}...")
    save_klines_to_csv(klines, symbol, interval)

def fetch_all_klines_parallel(symbols, intervals, max_workers=4):
    end_time = int(datetime.datetime.now().timestamp() * 1000)  # current time in milliseconds
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for symbol in symbols:
            for interval in intervals:
                futures.append(executor.submit(fetch_and_save_klines, symbol, interval, end_time))
        
        # Wait for all futures to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")

# Example usage
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT']
intervals = ['1M', '1W', '3d', '1d', '12h', '8h', '6h', '4h']
# intervals = ['1M', '1W', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
fetch_all_klines_parallel(symbols, intervals)