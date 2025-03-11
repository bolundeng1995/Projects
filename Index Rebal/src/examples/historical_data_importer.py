#!/usr/bin/env python
"""
Historical Data Importer

This script imports 2 years of historical data for major indices and their constituents.
It fetches data from Bloomberg and stores it in the SQLite database.

Usage:
    python historical_data_importer.py

Requirements:
    - Bloomberg Terminal with API access
    - Valid Bloomberg subscription with access to index data
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import time
from tqdm import tqdm

# Import core classes
from src.data.database import IndexDatabase
from src.data.bloomberg_client import BloombergClient
from src.data.importers.price_data import PriceDataImporter
from src.data.importers.index_constituents import IndexConstituentImporter
from src.data.calendar import RebalanceCalendar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('historical_import.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Define the lookback period for all data imports (90 days)
LOOKBACK_DAYS = 180  # Changed from 730 days (2 years) to 90 days

# Define indices to import
INDICES = [
    # Index_ID, Index_Name, Bloomberg_Ticker, Rebalance_Frequency, Description
    ("SP500", "S&P 500", "SPX Index", "Quarterly", "Large-cap US equities"),
    # ("SP400", "S&P MidCap 400", "MID Index", "Quarterly", "Mid-cap US equities"),
    # ("SP600", "S&P SmallCap 600", "SML Index", "Quarterly", "Small-cap US equities"),
    # ("RUSSELL1000", "Russell 1000", "RIY Index", "Annual", "Large-cap US equities"),
    # ("RUSSELL2000", "Russell 2000", "RTY Index", "Annual", "Small-cap US equities"),
    # ("NASDAQ100", "Nasdaq 100", "NDX Index", "Quarterly", "Large-cap tech-focused equities"),
    # ("MSCI_EAFE", "MSCI EAFE", "MXEA Index", "Quarterly", "Developed markets ex-US and Canada"),
    # ("MSCI_EM", "MSCI Emerging Markets", "MXEF Index", "Quarterly", "Emerging markets")
]

def initialize_database(db_path='index_history.db'):
    """Initialize the database with indices to track"""
    db = IndexDatabase(db_path)
    logger.info(f"Database initialized at {db_path}")
    return db

def add_indices(db):
    """Add indices to the database"""
    for index_id, index_name, bloomberg_ticker, rebalance_frequency, description in INDICES:
        db.add_index(
            index_id=index_id,
            index_name=index_name,
            bloomberg_ticker=bloomberg_ticker,
            rebalance_frequency=rebalance_frequency,
            description=description
        )
    logger.info(f"Added {len(INDICES)} indices to database")

def import_historical_prices(db, bloomberg, lookback_days=LOOKBACK_DAYS):
    """Import historical prices for indices"""
    price_importer = PriceDataImporter(db, bloomberg)
    
    # Calculate date range for logging only
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logger.info(f"Importing price history from {start_date_str} to {end_date_str}")
    
    # Get indices from database
    indices_df = db.get_all_indices()
    index_ids = indices_df['index_id'].tolist()
    
    # Import index prices using lookback_days
    for index_id in tqdm(index_ids, desc="Importing index prices"):
        logger.info(f"Importing prices for {index_id}")
        price_importer.update_index_prices([index_id], lookback_days=lookback_days)
        time.sleep(1)  # Avoid overwhelming Bloomberg API
    
    logger.info("Completed importing index prices")
    return indices_df

def import_historical_constituents(db, bloomberg, indices_df):
    """Import historical constituents and their price data"""
    constituent_importer = IndexConstituentImporter(db, bloomberg)
    price_importer = PriceDataImporter(db, bloomberg)
    
    # Use the global lookback period
    lookback_days = LOOKBACK_DAYS
    
    # Calculate date range for logging purposes
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Process one index at a time - starting with SPX
    for _, row in tqdm(indices_df.iterrows(), desc="Processing indices", total=len(indices_df)):
        index_id = row['index_id']
        bloomberg_ticker = row['bloomberg_ticker']
        
        # Process SPX first, then others
        if index_id == "SP500" or (index_id != "SP500" and indices_df.iloc[0]['index_id'] != "SP500"):
            logger.info(f"Importing current constituents for {index_id}")
            
            # Import current constituents
            count = constituent_importer.import_current_constituents(index_id)
            logger.info(f"Imported {count} constituents for {index_id}")
            
            # Get constituent list
            constituents = db.get_current_constituents(index_id)
            
            if not constituents.empty:
                # Import constituent price history
                logger.info(f"Importing price history for {len(constituents)} constituents of {index_id}")
                
                for i, c_row in tqdm(constituents.iterrows(), desc=f"{index_id} constituents", total=len(constituents)):
                    ticker = c_row['ticker']
                    price_importer.update_constituent_prices(ticker, lookback_days=lookback_days)
                    time.sleep(0.5)  # Avoid overwhelming Bloomberg API
            
            # Import historical changes
            logger.info(f"Importing historical constituent changes for {index_id}")
            constituent_importer.import_historical_changes(index_id, lookback_days=lookback_days)
            
            # Allow some time before moving to next index
            time.sleep(5)

def import_rebalance_calendar(db, bloomberg):
    """Import rebalance dates for all indices"""
    calendar = RebalanceCalendar(db, bloomberg)
    logger.info("Updating rebalance calendar for all indices")
    event_count = calendar.update_all_calendars()
    logger.info(f"Added {event_count} rebalance events to calendar")

def display_imported_data_summary(db):
    """Display a summary of the data that was imported into the database"""
    print("\n===== IMPORT SUMMARY =====\n")
    
    # 1. Show indices summary
    indices = db.get_all_indices()
    print(f"Imported {len(indices)} indices:")
    for _, row in indices.iterrows():
        print(f"  • {row['index_name']} ({row['index_id']}) - {row['bloomberg_ticker']}")
    
    # 2. Show constituent counts with first/last 5
    print("\nConstituent samples:")
    for index_id in indices['index_id']:
        constituents = db.get_current_constituents(index_id)
        print(f"\n  • {index_id}: {len(constituents)} constituents")
        
        if not constituents.empty:
            print("\n    First 5 constituents by weight:")
            print(constituents[['ticker', 'company_name', 'weight']].head(5).to_string(index=False))
            
            if len(constituents) > 10:  # Only show last 5 if we have enough rows
                print("\n    Last 5 constituents by weight:")
                print(constituents[['ticker', 'company_name', 'weight']].tail(5).to_string(index=False))
    
    # 3. Show price data first/last 5 rows
    print("\nPrice data samples:")
    for index_id in indices['index_id']:
        # Get the Bloomberg ticker for this index
        ticker = indices[indices['index_id'] == index_id]['bloomberg_ticker'].iloc[0]
        ticker_clean = ticker.split()[0]  # Remove " Index" part
        
        # Get all price data (or at least enough to show head/tail)
        price_data = db.get_price_data(ticker_clean, limit=1000)  # Limit to avoid huge queries
        
        if not price_data.empty:
            total_rows = len(price_data)
            print(f"\n  • {index_id} prices ({total_rows} days of data):")
            
            print("\n    Most recent 5 days:")
            print(price_data[['date', 'open', 'high', 'low', 'close']].head(5).to_string(index=False))
            
            if total_rows > 10:  # Only show oldest 5 if we have enough rows
                print("\n    Oldest 5 days:")
                print(price_data[['date', 'open', 'high', 'low', 'close']].tail(5).to_string(index=False))
    
    # 4. Show rebalance events first/last 5
    print("\nRebalance events:")
    rebalance_events = db.get_upcoming_rebalance_events(days_ahead=365)  # Get a full year
    if not rebalance_events.empty:
        total_events = len(rebalance_events)
        print(f"Found {total_events} upcoming rebalance events")
        
        print("\n  Earliest upcoming events:")
        for _, event in rebalance_events.head(5).iterrows():
            print(f"  • {event['index_id']}: Announcement: {event['announcement_date']}, Implementation: {event['implementation_date']}")
        
        if total_events > 10:
            print("\n  Latest upcoming events:")
            for _, event in rebalance_events.tail(5).iterrows():
                print(f"  • {event['index_id']}: Announcement: {event['announcement_date']}, Implementation: {event['implementation_date']}")
    else:
        print("  • No upcoming rebalance events found")
    
    print("\n===== END OF SUMMARY =====")

def main():
    """Main function to run the data import process"""
    # Initialize database
    db = initialize_database()
    
    # Connect to Bloomberg
    bloomberg = BloombergClient()
    
    if not bloomberg.start_session():
        logger.error("Failed to connect to Bloomberg. Exiting.")
        return
        
    logger.info("Successfully connected to Bloomberg API")
    
    try:
        # Add indices to database
        add_indices(db)
        
        # Import historical prices for indices
        indices_df = import_historical_prices(db, bloomberg)
        
        # Import constituent data and prices (SPX first, then others)
        import_historical_constituents(db, bloomberg, indices_df.sort_values(by="index_id", ascending=False))
        
        # Import rebalance calendar
        import_rebalance_calendar(db, bloomberg)
        
        logger.info("Historical data import completed successfully")
        
        # Display summary of imported data
        display_imported_data_summary(db)
        
    except Exception as e:
        logger.error(f"Error during data import: {e}")
    finally:
        # Close Bloomberg session
        bloomberg.stop_session()
        logger.info("Bloomberg session closed")

if __name__ == "__main__":
    main() 