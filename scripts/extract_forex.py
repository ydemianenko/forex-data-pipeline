"""
Forex Data Extraction Script for dbt Pipeline

This script extracts forex data from Twelve Data API and uploads it to 
Google Cloud Storage. Designed to run via GitHub Actions automation.
"""
import os
import sys
import configparser
from twelvedata import TDClient
import pandas as pd
from datetime import datetime, timedelta
import time
from google.cloud import storage
import argparse


def load_api_key():
    """Load API key from environment variable or config file"""
    # Priority: environment variable -> config.ini
    api_key = os.getenv('TWELVE_DATA_API_KEY')
    if api_key:
        return api_key
    
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'fetch_data', 'config.ini')
    if os.path.exists(config_path):
        config.read(config_path)
        return config['DEFAULT']['api_key']
    
    raise ValueError("API key not found. Set TWELVE_DATA_API_KEY environment variable or config.ini")


def fetch_forex_data(symbol, interval, start_date, end_date, outputsize=800):
    """
    Fetch forex data using Twelve Data SDK
    
    Args:
        symbol (str): Currency pair (e.g., 'EUR/USD')
        interval (str): Time interval (e.g., '5min', '1h', '1day')
        start_date (str): Start date in format 'YYYY-MM-DD HH:MM:SS'
        end_date (str): End date in format 'YYYY-MM-DD HH:MM:SS'
        outputsize (int): Maximum number of records to fetch
    
    Returns:
        pandas.DataFrame: DataFrame with OHLC data or None if error
    """
    try:
        api_key = load_api_key()
        td = TDClient(apikey=api_key)
        
        ts = td.time_series(
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            start_date=start_date,
            end_date=end_date,
            timezone="UTC"
        )
        
        df = ts.as_pandas()
        print(f"✓ Fetched {len(df)} records for {symbol} from {start_date} to {end_date}")
        return df
    
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        return None


def fetch_single_date_with_retry(symbol, interval, date_str, max_retries=5):
    """
    Fetch data for a single date with exponential backoff retry
    
    Args:
        symbol (str): Currency pair
        interval (str): Time interval
        date_str (str): Date in format 'YYYY-MM-DD'
        max_retries (int): Maximum retry attempts
    
    Returns:
        pandas.DataFrame: DataFrame or None
    """
    attempt = 0
    delay_seconds = 10
    
    while attempt <= max_retries:
        try:
            df = fetch_forex_data(
                symbol=symbol,
                interval=interval,
                start_date=f"{date_str} 00:00:00",
                end_date=f"{date_str} 23:59:59"
            )
            return df
        
        except Exception as e:
            message = str(e).lower()
            is_rate_limit = "rate" in message or "credits" in message or "limit" in message
            
            if not is_rate_limit or attempt >= max_retries:
                print(f"✗ Failed after {attempt + 1} attempts")
                return None
            
            attempt += 1
            print(f"⏳ Rate limit hit. Waiting {delay_seconds}s (attempt {attempt}/{max_retries})...")
            time.sleep(delay_seconds)
            delay_seconds *= 2  # Exponential backoff
    
    return None


def validate_and_prepare_data(df, symbol, date_str):
    """
    Validate and prepare DataFrame for storage
    
    Args:
        df (pandas.DataFrame): Input DataFrame
        symbol (str): Currency pair
        date_str (str): Date string
    
    Returns:
        pandas.DataFrame: Prepared DataFrame
    """
    if df is None or df.empty:
        return df
    
    # Remove duplicates
    initial_count = len(df)
    df = df.drop_duplicates()
    if len(df) < initial_count:
        print(f"  Removed {initial_count - len(df)} duplicate records")
    
    # Sort by datetime index
    df = df.sort_index()
    
    # Add metadata columns
    df = df.copy()
    df['symbol'] = symbol
    df['extraction_date'] = pd.Timestamp.now()
    
    return df


def save_to_gcs_parquet(df, bucket_name, symbol, date_str):
    """
    Save DataFrame as Parquet and upload to Google Cloud Storage
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        bucket_name (str): GCS bucket name
        symbol (str): Currency pair (e.g., 'EUR/USD')
        date_str (str): Date string in format 'YYYY-MM-DD'
    
    Returns:
        str: GCS path if successful, None otherwise
    """
    try:
        # Parse date for partitioning
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        year = date_obj.year
        month = f"{date_obj.month:02d}"
        day = f"{date_obj.day:02d}"
        
        # Clean symbol for path (EUR/USD -> eur_usd)
        symbol_clean = symbol.replace('/', '_').lower()
        
        # Create GCS path with partitioning
        gcs_path = f"{symbol_clean}/year={year}/month={month}/data_{year}_{month}_{day}.parquet"
        
        # Temporary local file
        local_file = f"temp_{symbol_clean}_{year}_{month}_{day}.parquet"
        
        # Fix datetime to avoid TIMESTAMP_NANOS issues with BigQuery
        df_copy = df.copy()
        if df_copy.index.name and 'datetime' in df_copy.index.name.lower():
            df_copy.index = df_copy.index.astype('datetime64[us]')
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].astype('datetime64[us]')
        
        # Save as Parquet locally
        df_copy.to_parquet(local_file, engine='pyarrow', compression='snappy')
        
        # Upload to GCS (credentials from environment)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        
        blob.upload_from_filename(local_file)
        print(f"✓ Uploaded to gs://{bucket_name}/{gcs_path}")
        
        # Remove temporary file
        os.remove(local_file)
        
        return gcs_path
    
    except Exception as e:
        print(f"✗ Error uploading to GCS: {e}")
        return None


def run_daily_extraction(symbol='EUR/USD', interval='5min', bucket_name=None, 
                        date=None, lookback_days=1):
    """
    Run daily forex data extraction
    
    Args:
        symbol (str): Currency pair
        interval (str): Time interval
        bucket_name (str): GCS bucket name
        date (str): Specific date to fetch (YYYY-MM-DD), defaults to yesterday
        lookback_days (int): Number of days to look back if date not specified
    
    Returns:
        bool: True if successful
    """
    print("=" * 80)
    print("🚀 Forex Data Extraction Pipeline")
    print("=" * 80)
    
    # Default to yesterday if no date specified
    if date is None:
        target_date = datetime.now() - timedelta(days=lookback_days)
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        date_str = date
    
    # Get bucket name from environment if not provided
    if bucket_name is None:
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        if not bucket_name:
            print("✗ GCS_BUCKET_NAME not provided")
            return False
    
    print(f"📅 Date: {date_str}")
    print(f"💱 Symbol: {symbol}")
    print(f"⏱️  Interval: {interval}")
    print(f"🪣 Bucket: {bucket_name}")
    print("=" * 80)
    
    # Extract
    print(f"\n[1/3] EXTRACT: Fetching data for {date_str}...")
    df = fetch_single_date_with_retry(symbol, interval, date_str)
    
    if df is None or df.empty:
        print(f"✗ No data available for {date_str}")
        return False
    
    # Transform
    print(f"\n[2/3] TRANSFORM: Validating and preparing data...")
    df = validate_and_prepare_data(df, symbol, date_str)
    print(f"  Records: {len(df)}")
    
    # Load
    print(f"\n[3/3] LOAD: Uploading to GCS...")
    gcs_path = save_to_gcs_parquet(df, bucket_name, symbol, date_str)
    
    if gcs_path:
        print("\n" + "=" * 80)
        print("✅ Pipeline completed successfully!")
        print("=" * 80)
        return True
    else:
        print("\n" + "=" * 80)
        print("✗ Pipeline failed")
        print("=" * 80)
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Extract forex data and upload to GCS')
    parser.add_argument('--symbol', default='EUR/USD', help='Currency pair (default: EUR/USD)')
    parser.add_argument('--interval', default='5min', help='Time interval (default: 5min)')
    parser.add_argument('--bucket', help='GCS bucket name (or set GCS_BUCKET_NAME env var)')
    parser.add_argument('--date', help='Specific date to fetch (YYYY-MM-DD)')
    parser.add_argument('--lookback-days', type=int, default=1, 
                       help='Days to look back if no date specified (default: 1)')
    
    args = parser.parse_args()
    
    success = run_daily_extraction(
        symbol=args.symbol,
        interval=args.interval,
        bucket_name=args.bucket,
        date=args.date,
        lookback_days=args.lookback_days
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
