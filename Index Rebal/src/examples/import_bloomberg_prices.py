#!/usr/bin/env python
"""
Bloomberg Price Data Import Script

A simple script to import price data from Bloomberg API into the price_data database table.
This script connects to Bloomberg, fetches OHLCV data, and saves it to the database.

Usage Examples:
    # Import price data for a specific ticker for the last 30 days
    python import_bloomberg_prices.py --ticker AAPL US Equity
    
    # Import price data for multiple tickers with a specific date range
    python import_bloomberg_prices.py --tickers "AAPL US Equity,MSFT US Equity" --start-date 2023-01-01 --end-date 2023-01-31
    
    # Import price data for constituents of an index
    python import_bloomberg_prices.py --index SP500 --lookback 90
    
    # Import price data for constituents of multiple indices
    python import_bloomberg_prices.py --indices SP500,RUSSELL2000 --lookback 30

    # Import price data for all indices in the database (constituents only)
    python import_bloomberg_prices.py --all-indices --lookback 30

    # Import price data for indices and their constituents
    python import_bloomberg_prices.py --all-indices --include-index-prices --lookback 30

    # Import price data for a specific index and the index itself
    python import_bloomberg_prices.py --index SP500 --include-index-prices
"""

import sys
import os
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase
from src.data.bloomberg_client import BloombergClient

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('import_bloomberg_prices')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Import price data from Bloomberg to the database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Import price data for a specific ticker
  %(prog)s --ticker "AAPL US Equity"
  
  # Import price data for multiple tickers
  %(prog)s --tickers "AAPL US Equity,MSFT US Equity,GOOGL US Equity"
  
  # Import price data for index constituents
  %(prog)s --index SP500
  
  # Import price data for all indices in the database
  %(prog)s --all-indices
"""
    )
    
    # Database options
    parser.add_argument('--db-path', type=str, default='index_data.db',
                        help='Path to the database file (default: index_data.db)')
    
    # Source selection - mutually exclusive group
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--ticker', type=str,
                             help='Bloomberg ticker to fetch price data for')
    source_group.add_argument('--tickers', type=str,
                             help='Comma-separated list of Bloomberg tickers')
    source_group.add_argument('--index', type=str,
                             help='Fetch price data for all constituents of this index')
    source_group.add_argument('--indices', type=str,
                             help='Comma-separated list of indices to fetch constituent price data for')
    source_group.add_argument('--all-indices', action='store_true',
                             help='Fetch price data for all indices in the database and their constituents')
    
    # Date range options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument('--lookback', type=int, default=30,
                           help='Number of days to look back from today (default: 30)')
    date_group.add_argument('--date-range', type=str, nargs=2,
                           help='Date range in format YYYY-MM-DD YYYY-MM-DD')
    
    # Additional Bloomberg options
    parser.add_argument('--fields', type=str, 
                       default='OPEN,HIGH,LOW,PX_LAST,VOLUME,DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',
                       help='Fields to fetch from Bloomberg (default includes OHLCV and total return)')
    parser.add_argument('--use-cache', action='store_true',
                       help='Use cached Bloomberg data when available')
    
    # Operation options
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing price data for the same ticker/date')
    parser.add_argument('--include-index-prices', action='store_true',
                       help='Also import price data for the indices themselves, not just constituents')
    
    return parser.parse_args()

def get_date_range(args):
    """Get start and end dates based on args"""
    if args.date_range:
        start_date = args.date_range[0]
        end_date = args.date_range[1]
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.lookback)).strftime('%Y-%m-%d')
    
    return start_date, end_date

def fetch_price_data_for_ticker(client, ticker, start_date, end_date, fields):
    """Fetch historical price data for a specific ticker from Bloomberg"""
    logger.info(f"Fetching price data for {ticker} from {start_date} to {end_date}")
    
    try:
        # Get historical data from Bloomberg
        data = client.get_historical_data(
            tickers=[ticker],
            fields=fields.split(','),
            start_date=start_date,
            end_date=end_date
        )
        
        if data.empty:
            logger.warning(f"No data returned from Bloomberg for {ticker}")
            return pd.DataFrame()
        
        # Rename Bloomberg fields to match our database schema
        field_mapping = {
            'OPEN': 'open',
            'HIGH': 'high',
            'LOW': 'low',
            'PX_LAST': 'close',
            'VOLUME': 'volume',
            'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS': 'return'
        }
        
        df = data.copy()
        
        # Rename columns based on mapping
        for bbg_field, db_field in field_mapping.items():
            if bbg_field in df.columns:
                df.rename(columns={bbg_field: db_field}, inplace=True)
        
        # Add ticker column if multiple tickers were requested
        if 'ticker' not in df.columns:
            df['ticker'] = ticker
        
        # Calculate returns if the field wasn't available from Bloomberg
        if 'return' not in df.columns:
            logger.info(f"Return field not available from Bloomberg for {ticker}, calculating from price data")
            df['return'] = df.groupby('ticker')['close'].pct_change()
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

def fetch_price_data_for_index_constituents(db, client, index_id, start_date, end_date, fields):
    """Fetch price data for all constituents of an index"""
    logger.info(f"Fetching constituents for index {index_id}")
    
    try:
        # Get current constituents for the index
        constituents = db.get_index_constituents(index_id)
        
        if constituents.empty:
            logger.warning(f"No constituents found for index {index_id}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(constituents)} constituents for {index_id}")
        
        # Extract tickers (might need mapping to Bloomberg tickers)
        constituent_tickers = constituents['symbol'].tolist()
        
        # Convert tickers to Bloomberg format if needed
        bloomberg_tickers = []
        for ticker in constituent_tickers:
            # Simple mapping - in a real app you might need more robust mapping
            if not ticker.endswith(' Equity'):
                bloomberg_ticker = f"{ticker} US Equity"
            else:
                bloomberg_ticker = ticker
            bloomberg_tickers.append(bloomberg_ticker)
        
        # Fetch data for each constituent
        all_data = []
        for ticker in bloomberg_tickers:
            df = fetch_price_data_for_ticker(client, ticker, start_date, end_date, fields)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            logger.warning(f"No price data found for any constituents of {index_id}")
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
        
    except Exception as e:
        logger.error(f"Error fetching constituent data for index {index_id}: {e}")
        return pd.DataFrame()

def import_price_data_to_db(db, df, overwrite=False):
    """Import price data from DataFrame to database"""
    if df.empty:
        logger.warning("No data to import")
        return 0
    
    # Ensure we have all required columns
    required_cols = ['ticker', 'date', 'open', 'high', 'low', 'close']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        logger.error(f"Missing required columns in data: {', '.join(missing_cols)}")
        return 0
    
    # Add volume and return if they don't exist
    if 'volume' not in df.columns:
        df['volume'] = 0
    
    if 'return' not in df.columns:
        logger.info("Return data not available, calculating from price data")
        df['return'] = df.groupby('ticker')['close'].pct_change()
    
    # Convert date column to string format if it's not already
    if not isinstance(df['date'].iloc[0], str):
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Import each row to the database
    count = 0
    for _, row in df.iterrows():
        try:
            # Check if we already have data for this ticker/date combination
            if not overwrite:
                query = "SELECT 1 FROM price_data WHERE ticker = ? AND date = ?"
                result = db.cursor.execute(query, (row['ticker'], row['date'])).fetchone()
                
                if result:
                    # Data already exists, skip
                    continue
            
            # Insert or replace the data
            db.cursor.execute('''
            INSERT OR REPLACE INTO price_data 
            (ticker, date, open, high, low, close, volume, return)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['ticker'],
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['return']
            ))
            
            count += 1
            
        except Exception as e:
            logger.error(f"Error importing price data for {row['ticker']} on {row['date']}: {e}")
    
    logger.info(f"Successfully imported {count} price data records")
    return count

def fetch_all_indices_from_db(db):
    """Fetch all index IDs and Bloomberg tickers from the database"""
    logger.info("Fetching all indices from the database")
    
    try:
        indices_df = db.get_all_indices()
        
        if indices_df.empty:
            logger.warning("No indices found in the database")
            return []
        
        # Create a list of (index_id, bloomberg_ticker) tuples
        indices = list(zip(indices_df['index_id'], indices_df['bloomberg_ticker']))
        logger.info(f"Found {len(indices)} indices in the database")
        return indices
        
    except Exception as e:
        logger.error(f"Error fetching indices from the database: {e}")
        return []

def fetch_index_price_data(client, bloomberg_ticker, start_date, end_date, fields):
    """Fetch price data for an index using its Bloomberg ticker"""
    logger.info(f"Fetching price data for index {bloomberg_ticker} from {start_date} to {end_date}")
    
    try:
        # Get historical data from Bloomberg
        data = client.get_historical_data(
            tickers=[bloomberg_ticker],
            fields=fields.split(','),
            start_date=start_date,
            end_date=end_date
        )
        
        if data.empty:
            logger.warning(f"No data returned from Bloomberg for index {bloomberg_ticker}")
            return pd.DataFrame()
        
        # Process the data in the same way as regular tickers
        df = data.copy()
        
        # Rename Bloomberg fields to match our database schema
        field_mapping = {
            'OPEN': 'open',
            'HIGH': 'high',
            'LOW': 'low',
            'PX_LAST': 'close',
            'VOLUME': 'volume',
            'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS': 'return'
        }
        
        # Rename columns based on mapping
        for bbg_field, db_field in field_mapping.items():
            if bbg_field in df.columns:
                df.rename(columns={bbg_field: db_field}, inplace=True)
        
        # Add ticker column (use the Bloomberg ticker as the ticker)
        df['ticker'] = bloomberg_ticker
        
        # Calculate returns if not available
        if 'return' not in df.columns:
            logger.info(f"Return field not available from Bloomberg for {bloomberg_ticker}, calculating from price data")
            df['return'] = df.groupby('ticker')['close'].pct_change()
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data for index {bloomberg_ticker}: {e}")
        return pd.DataFrame()

def main():
    """Main function to import Bloomberg price data to the database"""
    args = parse_args()
    
    # Get date range
    start_date, end_date = get_date_range(args)
    
    # Initialize database connection
    db = IndexDatabase(args.db_path)
    logger.info(f"Connected to database: {args.db_path}")
    
    # Initialize Bloomberg client
    client = BloombergClient(use_cached_data=args.use_cache)
    logger.info("Connected to Bloomberg API")
    
    try:
        all_dfs = []  # To collect all dataframes
        
        # Fetch data based on source selection
        if args.ticker:
            logger.info(f"Fetching price data for ticker: {args.ticker}")
            df = fetch_price_data_for_ticker(client, args.ticker, start_date, end_date, args.fields)
            if not df.empty:
                all_dfs.append(df)
        
        elif args.tickers:
            tickers = [t.strip() for t in args.tickers.split(',')]
            logger.info(f"Fetching price data for {len(tickers)} tickers")
            
            # Fetch data for each ticker
            for ticker in tickers:
                ticker_df = fetch_price_data_for_ticker(client, ticker, start_date, end_date, args.fields)
                if not ticker_df.empty:
                    all_dfs.append(ticker_df)
        
        elif args.index:
            index_id = args.index
            logger.info(f"Fetching price data for constituents of index: {index_id}")
            
            # Fetch constituent data
            constituents_df = fetch_price_data_for_index_constituents(db, client, index_id, start_date, end_date, args.fields)
            if not constituents_df.empty:
                all_dfs.append(constituents_df)
            
            # If requested, also fetch price data for the index itself
            if args.include_index_prices:
                # Get the Bloomberg ticker for this index
                query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
                result = db.cursor.execute(query, (index_id,)).fetchone()
                
                if result:
                    bloomberg_ticker = result[0]
                    logger.info(f"Fetching price data for index {index_id} ({bloomberg_ticker})")
                    
                    index_df = fetch_index_price_data(client, bloomberg_ticker, start_date, end_date, args.fields)
                    if not index_df.empty:
                        all_dfs.append(index_df)
                else:
                    logger.warning(f"Bloomberg ticker not found for index {index_id}")
        
        elif args.indices:
            indices = [idx.strip() for idx in args.indices.split(',')]
            logger.info(f"Fetching price data for constituents of {len(indices)} indices")
            
            # Fetch data for each index
            for index_id in indices:
                # Fetch constituent data
                constituent_df = fetch_price_data_for_index_constituents(db, client, index_id, start_date, end_date, args.fields)
                if not constituent_df.empty:
                    all_dfs.append(constituent_df)
                
                # If requested, also fetch price data for the index itself
                if args.include_index_prices:
                    # Get the Bloomberg ticker for this index
                    query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
                    result = db.cursor.execute(query, (index_id,)).fetchone()
                    
                    if result:
                        bloomberg_ticker = result[0]
                        logger.info(f"Fetching price data for index {index_id} ({bloomberg_ticker})")
                        
                        index_df = fetch_index_price_data(client, bloomberg_ticker, start_date, end_date, args.fields)
                        if not index_df.empty:
                            all_dfs.append(index_df)
                    else:
                        logger.warning(f"Bloomberg ticker not found for index {index_id}")
        
        elif args.all_indices:
            logger.info("Fetching price data for all indices in the database")
            
            # Get all indices from the database
            indices = fetch_all_indices_from_db(db)
            
            if not indices:
                logger.error("No indices found in the database")
                return 1
            
            # Fetch data for each index
            for index_id, bloomberg_ticker in indices:
                # Fetch constituent data
                constituent_df = fetch_price_data_for_index_constituents(db, client, index_id, start_date, end_date, args.fields)
                if not constituent_df.empty:
                    all_dfs.append(constituent_df)
                
                # If requested, also fetch price data for the index itself
                if args.include_index_prices:
                    logger.info(f"Fetching price data for index {index_id} ({bloomberg_ticker})")
                    
                    index_df = fetch_index_price_data(client, bloomberg_ticker, start_date, end_date, args.fields)
                    if not index_df.empty:
                        all_dfs.append(index_df)
        
        # Combine all dataframes if we have any
        if not all_dfs:
            logger.warning("No data was fetched from Bloomberg")
            return 1
            
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Import data to database
        count = import_price_data_to_db(db, df, args.overwrite)
        
        # Commit changes
        db.conn.commit()
        
        # Summary
        logger.info(f"Bloomberg price data import complete: {count} records imported")
        
    except Exception as e:
        logger.error(f"Error during Bloomberg price data import: {e}")
        db.conn.rollback()
        return 1
        
    finally:
        # Clean up resources
        client.close()
        db.conn.close()
        logger.info("Connections closed")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 