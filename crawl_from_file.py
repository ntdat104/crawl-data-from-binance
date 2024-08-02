import os
import requests
from urllib.parse import urljoin
from datetime import datetime, timedelta
import zipfile
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Base URLs for Binance data
BASE_URLS = {
    'monthly': "https://data.binance.vision/data/spot/monthly/klines/",
    'daily': "https://data.binance.vision/data/spot/daily/klines/"
}

# Define the date range
start_date = datetime(2018, 8, 1)
end_date = datetime(2024, 8, 1)

# Choose data type ('monthly' or 'daily')
data_type = 'monthly'  # or 'monthly'

# List of symbols and intervals
# symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT']
# intervals = ['30m', '15m', '5m', '3m', '1m']
symbols = ['BTCUSDT']
intervals = ['1m']

# Base directory
BASE_DIR = 'static'

# Function to create a directory if it does not exist
def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Function to get the directory path for saving files based on symbol and interval
def get_save_dir(symbol, interval):
    return os.path.join(BASE_DIR, symbol, interval)

# Function to generate URLs for monthly data
def generate_monthly_urls(symbol, interval, start_date, end_date):
    base_url = urljoin(BASE_URLS['monthly'], f"{symbol}/{interval}/")
    current_date = start_date
    while current_date <= end_date:
        year_month = current_date.strftime("%Y-%m")
        url = urljoin(base_url, f"{symbol}-{interval}-{year_month}.zip")
        yield url
        # Move to the next month
        next_month = current_date.replace(day=28) + timedelta(days=4)
        current_date = next_month.replace(day=1)

# Function to generate URLs for daily data
def generate_daily_urls(symbol, interval, start_date, end_date):
    base_url = urljoin(BASE_URLS['daily'], f"{symbol}/{interval}/")
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        url = urljoin(base_url, f"{symbol}-{interval}-{date_str}.zip")
        yield url
        # Move to the next day
        current_date += timedelta(days=1)

# Function to download a file from a URL
def download_file(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Check for request errors
    with open(save_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Downloaded {url} to {save_path}")

# Function to extract CSV files from a ZIP archive
def extract_csv_from_zip(zip_path, extract_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"Extracted CSV files from {zip_path} to {extract_dir}")

# Function to merge all CSV files and save to a single file
def merge_csv_files(directory, output_file):
    all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    df_list = [pd.read_csv(file, header=None) for file in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv(output_file, index=False, header=False)  # Save without any modifications
    print(f"Merged CSV files into {output_file}")

# Function to create all required directories for symbols and intervals
def create_all_directories(symbols, intervals):
    for symbol in symbols:
        for interval in intervals:
            dir_path = get_save_dir(symbol, interval)
            create_directory(dir_path)

# Function to process data for a given symbol, interval, and data type
def process_symbol_interval(symbol, interval, start_date, end_date, data_type):
    save_dir = get_save_dir(symbol, interval)
    urls = list(generate_monthly_urls(symbol, interval, start_date, end_date)) if data_type == 'monthly' else list(generate_daily_urls(symbol, interval, start_date, end_date))
    
    # Download files concurrently
    with ThreadPoolExecutor() as executor:
        futures = []
        for url in urls:
            file_name = url.split("/")[-1]
            save_path = os.path.join(save_dir, file_name)
            futures.append(executor.submit(download_file, url, save_path))
        
        # Wait for all downloads to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred during download: {e}")
    
    # Extract and merge files after all downloads are complete
    for file_name in os.listdir(save_dir):
        if file_name.endswith('.zip'):
            zip_path = os.path.join(save_dir, file_name)
            extract_csv_from_zip(zip_path, save_dir)
    
    # Merge all CSV files into one
    output_csv = os.path.join(save_dir, f'{symbol}_{interval}.csv')
    merge_csv_files(save_dir, output_csv)

# Main script
def main(start_date, end_date, symbols, intervals, data_type):
    # Create all required directories before starting the crawl
    create_all_directories(symbols, intervals)
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for symbol in symbols:
            for interval in intervals:
                futures.append(executor.submit(process_symbol_interval, symbol, interval, start_date, end_date, data_type))
        
        # Wait for all tasks to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")
    print("Download, extraction, and merging complete.")

# Run the main script
main(start_date, end_date, symbols, intervals, data_type)