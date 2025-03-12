#!/usr/bin/env python
"""
Index and Price Data Import Script

This script provides a unified interface for importing both index constituent data 
and price data (OHLCV) into the database. It supports multiple data sources and 
various import configurations.

Usage:
    # Import index metadata and current constituents
    python import_market_data.py --import-type metadata --index SP500,RUSSELL2000
    
    # Import current constituent data 
    python import_market_data.py --import-type constituents --index SP500
    
    # Import price data for an index for the last 30 days
    python import_market_data.py --import-type prices --index SP500 --lookback 30
    
    # Import price data for all constituents of an index for a specific date range
    python import_market_data.py --import-type constituent-prices --index SP500 --date-range 2023-01-01 2023-01-31
    
    # Comprehensive import - metadata, constituents and prices
    python import_market_data.py --import-type all --index SP500 --lookback 365
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import pandas as pd

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase
from src.data.importers.index_constituents import FileBasedConstituentProvider, IndexConstituentImporter
from src.data.importers.price_data import PriceDataImporter
from src.data.bloomberg_client import BloombergClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('market_data_import.log')
    ]
)

logger = logging.getLogger('import_market_data')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Import index and price data')
    
    # Import type selection
    parser.add_argument('--import-type', type=str, required=True, 
                      choices=['metadata', 'constituents', 'prices', 'constituent-prices', 'all'],
                      help='Type of data to import')
    
    # Index selection
    index_group = parser.add_mutually_exclusive_group(required=True)
    index_group.add_argument('--index', type=str, help='Comma-separated list of index IDs (e.g., SP500,RUSSELL2000)')
    index_group.add_argument('--all-indices', action='store_true', help='Import data for all indices in the database')
    
    # Date options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument('--date', type=str, help='Import data for a specific date (YYYY-MM-DD)')
    date_group.add_argument('--date-range', type=str, nargs=2, 
                           help='Import data for a date range (YYYY-MM-DD YYYY-MM-DD)')
    date_group.add_argument('--lookback', type=int, default=30, 
                           help='Number of days to look back from current date (default: 30)')
    
    # Database options
    parser.add_argument('--db-path', type=str, default='index_data.db', 
                       help='Path to the SQLite database file')
    
    # Data source options
    parser.add_argument('--data-source', type=str, choices=['file', 'bloomberg', 'both'], default='file',
                       help='Data source (default: file)')
    
    # File-based options
    parser.add_argument('--data-folder', type=str, 
                       default=r"\\deai.us.world.socgen\ebtsadm\indexman\PROD\RAW_FILES",
                       help='Path to the data folder containing constituent files')
    
    # Bloomberg options
    parser.add_argument('--use-cached-data', action='store_true',
                       help='Use cached Bloomberg data when available')
    
    # Additional options
    parser.add_argument('--verify', action='store_true', 
                       help='Verify imported data after import')
    parser.add_argument('--overwrite', action='store_true', 
                       help='Overwrite existing data for the same date')
                       
    return parser.parse_args()

def get_indices_to_import(db: IndexDatabase, index_arg: Optional[str]) -> List[str]:
    """Get list of indices to import"""
    if index_arg:
        # Parse comma-separated list
        return [idx.strip() for idx in index_arg.split(',')]
    else:
        # Get all indices from the database
        query = "SELECT index_id FROM index_metadata"
        df = pd.read_sql_query(query, db.conn)
        if df.empty:
            logger.error("No indices found in the database. Please add index metadata first.")
            sys.exit(1)
        return df['index_id'].tolist()

def get_date_range(args: argparse.Namespace) -> tuple:
    """Determine start and end dates based on arguments"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    if args.date:
        # Single date - use it for both start and end
        return args.date, args.date
    elif args.date_range:
        # Explicit date range
        return args.date_range[0], args.date_range[1]
    else:
        # Use lookback period
        start_date = (datetime.now() - timedelta(days=args.lookback)).strftime('%Y-%m-%d')
        return start_date, end_date

def import_index_metadata(db: IndexDatabase, indices: List[str]) -> Dict[str, bool]:
    """
    Import or update index metadata for specified indices
    """
    results = {}
    
    # For this function to work, we'd need to have a mapping of index IDs to their metadata
    # For now, we'll just return a placeholder and log a message
    for index_id in indices:
        logger.info(f"Importing/updating metadata for {index_id}")
        
        # In a real implementation, we would get the metadata from a source and add it
        # Example (pseudocode):
        # metadata = get_index_metadata_from_source(index_id)
        # success = db.add_or_update_index_metadata(index_id, metadata)
        
        # Placeholder:
        results[index_id] = True
        logger.warning(f"Metadata import for {index_id} is not fully implemented")
    
    return results

def import_constituents(importer: IndexConstituentImporter, indices: List[str], 
                        date: Optional[str] = None) -> Dict[str, int]:
    """
    Import constituent data for specified indices
    """
    results = {}
    
    for index_id in indices:
        if date:
            # Import historical constituents for specific date
            logger.info(f"Importing historical constituents for {index_id} as of {date}")
            count = importer.import_historical_constituents(index_id, date)
        else:
            # Import current constituents
            logger.info(f"Importing current constituents for {index_id}")
            count = importer.import_current_constituents(index_id)
            
        results[index_id] = count
    
    return results

def import_price_data(price_importer: PriceDataImporter, indices: List[str], 
                     start_date: str, end_date: str) -> Dict[str, bool]:
    """
    Import price data for specified indices
    """
    results = {}
    
    for index_id in indices:
        logger.info(f"Importing price data for index {index_id} from {start_date} to {end_date}")
        
        # Calculate lookback days
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        lookback_days = (end_dt - start_dt).days + 1
        
        try:
            # Update index prices
            price_importer.update_index_prices([index_id], lookback_days)
            results[index_id] = True
        except Exception as e:
            logger.error(f"Error updating price data for {index_id}: {e}")
            results[index_id] = False
    
    return results

def import_constituent_price_data(db: IndexDatabase, price_importer: PriceDataImporter, 
                                indices: List[str], start_date: str, end_date: str) -> Dict[str, Dict[str, bool]]:
    """
    Import price data for all constituents of specified indices
    """
    results = {}
    
    for index_id in indices:
        logger.info(f"Importing constituent price data for {index_id} from {start_date} to {end_date}")
        
        # Get the constituents for this index
        constituents_df = db.get_index_constituents(index_id)
        if constituents_df.empty:
            logger.warning(f"No constituents found for {index_id}")
            results[index_id] = {}
            continue
        
        # Calculate lookback days
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        lookback_days = (end_dt - start_dt).days + 1
        
        constituent_results = {}
        for _, row in constituents_df.iterrows():
            ticker = row['symbol']
            try:
                # Update price data for this constituent
                logger.info(f"Updating price data for {ticker} ({lookback_days} days)")
                success = price_importer.update_constituent_prices(ticker, lookback_days)
                constituent_results[ticker] = success
            except Exception as e:
                logger.error(f"Error updating price data for {ticker}: {e}")
                constituent_results[ticker] = False
        
        results[index_id] = constituent_results
    
    return results

def verify_imported_data(db: IndexDatabase, import_type: str, indices: List[str], 
                        start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, bool]:
    """
    Verify that data was correctly imported
    """
    results = {}
    
    for index_id in indices:
        if import_type in ['metadata', 'all']:
            # Verify index metadata
            query = "SELECT * FROM index_metadata WHERE index_id = ?"
            metadata_df = pd.read_sql_query(query, db.conn, params=(index_id,))
            
            if metadata_df.empty:
                logger.warning(f"Verification failed: No metadata found for {index_id}")
                results[f"{index_id}_metadata"] = False
            else:
                logger.info(f"Verified metadata for {index_id}")
                results[f"{index_id}_metadata"] = True
        
        if import_type in ['constituents', 'all']:
            # Verify constituent data
            if start_date:
                # Verify specific date
                constituents_df = db.get_index_constituents(index_id, start_date)
                date_str = start_date
            else:
                # Verify latest data
                constituents_df = db.get_index_constituents(index_id)
                date_str = "latest"
            
            if constituents_df.empty:
                logger.warning(f"Verification failed: No constituent data for {index_id} ({date_str})")
                results[f"{index_id}_constituents"] = False
            else:
                logger.info(f"Verified {len(constituents_df)} constituents for {index_id} ({date_str})")
                results[f"{index_id}_constituents"] = True
        
        if import_type in ['prices', 'all']:
            # Verify price data
            query = """
                SELECT COUNT(*) as count
                FROM price_data
                WHERE ticker = ? AND date BETWEEN ? AND ?
            """
            
            if not start_date:
                # Use recent dates if not specified
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=30)
                start_date = start_dt.strftime('%Y-%m-%d')
                end_date = end_dt.strftime('%Y-%m-%d')
            
            price_df = pd.read_sql_query(query, db.conn, params=(index_id, start_date, end_date))
            
            if price_df.iloc[0]['count'] == 0:
                logger.warning(f"Verification failed: No price data for {index_id} between {start_date} and {end_date}")
                results[f"{index_id}_prices"] = False
            else:
                logger.info(f"Verified {price_df.iloc[0]['count']} price records for {index_id}")
                results[f"{index_id}_prices"] = True
    
    return results

def main():
    """Main function to run the import process"""
    args = parse_args()
    
    # Initialize database connection
    db = IndexDatabase(args.db_path)
    
    # Get indices to import
    indices = get_indices_to_import(db, args.index)
    logger.info(f"Preparing to import data for indices: {', '.join(indices)}")
    
    # Get date range (if applicable)
    start_date, end_date = get_date_range(args)
    
    # Initialize importers based on data source
    if args.data_source in ['file', 'both']:
        constituent_importer = IndexConstituentImporter(db, args.data_folder)
    
    if args.data_source in ['bloomberg', 'both']:
        bloomberg_client = BloombergClient(use_cached_data=args.use_cached_data)
        price_importer = PriceDataImporter(db, bloomberg_client)
    elif args.import_type in ['prices', 'constituent-prices', 'all']:
        logger.error("Price data import requires Bloomberg data source")
        return 1
    
    # Perform imports based on requested type
    results = {}
    try:
        # Import index metadata if requested
        if args.import_type in ['metadata', 'all']:
            metadata_results = import_index_metadata(db, indices)
            results['metadata'] = metadata_results
        
        # Import constituent data if requested
        if args.import_type in ['constituents', 'all']:
            if args.data_source in ['file', 'both']:
                if args.date:
                    constituent_results = import_constituents(constituent_importer, indices, args.date)
                else:
                    constituent_results = import_constituents(constituent_importer, indices)
                
                results['constituents'] = constituent_results
            else:
                logger.warning("Constituent import requested but file data source not enabled")
        
        # Import price data if requested
        if args.import_type in ['prices', 'all']:
            if args.data_source in ['bloomberg', 'both']:
                price_results = import_price_data(price_importer, indices, start_date, end_date)
                results['prices'] = price_results
            else:
                logger.warning("Price data import requested but Bloomberg data source not enabled")
        
        # Import constituent price data if requested
        if args.import_type in ['constituent-prices', 'all']:
            if args.data_source in ['bloomberg', 'both']:
                constituent_price_results = import_constituent_price_data(
                    db, price_importer, indices, start_date, end_date)
                results['constituent_prices'] = constituent_price_results
            else:
                logger.warning("Constituent price import requested but Bloomberg data source not enabled")
        
        # Display results
        logger.info("Import results summary:")
        for import_type, type_results in results.items():
            logger.info(f"  {import_type.capitalize()}:")
            
            if isinstance(type_results, dict):
                for key, value in type_results.items():
                    if isinstance(value, dict):
                        # For nested results (e.g., constituent prices)
                        success_count = sum(1 for v in value.values() if v)
                        total_count = len(value)
                        if total_count > 0:
                            logger.info(f"    {key}: {success_count}/{total_count} successful")
                        else:
                            logger.warning(f"    {key}: No items processed")
                    elif isinstance(value, (bool, int)):
                        # For simple results
                        if value:
                            if isinstance(value, bool):
                                logger.info(f"    {key}: Success")
                            else:  # int
                                logger.info(f"    {key}: {value} records imported")
                        else:
                            logger.warning(f"    {key}: Failed")
        
        # Verify imported data if requested
        if args.verify:
            logger.info("Verifying imported data...")
            verification_results = verify_imported_data(
                db, args.import_type, indices, start_date, end_date)
            
            # Display verification results
            logger.info("Verification results:")
            for key, success in verification_results.items():
                if success:
                    logger.info(f"  {key}: Passed")
                else:
                    logger.warning(f"  {key}: Failed")
    
    except Exception as e:
        logger.error(f"Error during import: {e}", exc_info=True)
        return 1
        
    finally:
        # Clean up
        db.conn.close()
        if 'bloomberg_client' in locals():
            bloomberg_client.close()
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 