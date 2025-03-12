#!/usr/bin/env python
"""
Index Data Import Script

This script imports index constituent data from files into the database.
It supports importing current constituents, historical constituents for specific dates,
or bulk import of all available historical data.

Usage:
    python import_index_data.py --index SP500 --current
    python import_index_data.py --index SP500 --date 2023-01-31
    python import_index_data.py --index SP500 --all-history
    python import_index_data.py --index SP500,RUSSELL2000 --date-range 2023-01-01 2023-03-31
    python import_index_data.py --all-indices --current
"""

import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase
from src.data.importers.index_constituents import FileBasedConstituentProvider, IndexConstituentImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('index_import.log')
    ]
)

logger = logging.getLogger('import_index_data')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Import index constituent data')
    
    # Index selection
    index_group = parser.add_mutually_exclusive_group(required=True)
    index_group.add_argument('--index', type=str, help='Comma-separated list of index IDs (e.g., SP500,RUSSELL2000)')
    index_group.add_argument('--all-indices', action='store_true', help='Import data for all indices in the database')
    
    # Import mode
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--current', action='store_true', help='Import current constituents')
    mode_group.add_argument('--date', type=str, help='Import constituents for a specific date (YYYY-MM-DD)')
    mode_group.add_argument('--date-range', type=str, nargs=2, 
                           help='Import constituents for a date range (YYYY-MM-DD YYYY-MM-DD)')
    mode_group.add_argument('--all-history', action='store_true', help='Import all available historical data')
    
    # Database options
    parser.add_argument('--db-path', type=str, default='index_data.db', 
                       help='Path to the SQLite database file')
    
    # Additional options
    parser.add_argument('--data-folder', type=str, 
                       default=r"\\deai.us.world.socgen\ebtsadm\indexman\PROD\RAW_FILES",
                       help='Path to the data folder containing constituent files')
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

def import_current_constituents(importer: IndexConstituentImporter, indices: List[str]) -> dict:
    """Import current constituents for specified indices"""
    results = {}
    
    for index_id in indices:
        logger.info(f"Importing current constituents for {index_id}")
        count = importer.import_current_constituents(index_id)
        results[index_id] = count
        
    return results

def import_historical_constituents(importer: IndexConstituentImporter, 
                                  indices: List[str], 
                                  date: str) -> dict:
    """Import historical constituents for specified indices and date"""
    results = {}
    
    for index_id in indices:
        logger.info(f"Importing historical constituents for {index_id} as of {date}")
        count = importer.import_historical_constituents(index_id, date)
        results[index_id] = count
        
    return results

def import_date_range(importer: IndexConstituentImporter, 
                     indices: List[str], 
                     start_date: str, 
                     end_date: str) -> dict:
    """Import constituents for specified indices and date range"""
    results = {}
    
    # Convert string dates to datetime for iteration
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    for index_id in indices:
        total_count = 0
        
        # Iterate through each date in the range
        current = start
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            logger.info(f"Importing constituents for {index_id} as of {date_str}")
            
            count = importer.import_historical_constituents(index_id, date_str)
            total_count += count
            
            # Move to next date
            current += timedelta(days=1)
        
        results[index_id] = total_count
        
    return results

def import_all_history(importer: IndexConstituentImporter, indices: List[str]) -> dict:
    """Import all available historical data for specified indices"""
    results = {}
    
    for index_id in indices:
        logger.info(f"Importing all available historical data for {index_id}")
        date_count = importer.import_all_available_history(index_id)
        results[index_id] = date_count
        
    return results

def verify_imported_data(db: IndexDatabase, index_id: str, date: Optional[str] = None) -> bool:
    """Verify that data was correctly imported"""
    if date:
        # Verify specific date
        df = db.get_index_constituents(index_id, date)
        if df.empty:
            logger.warning(f"Verification failed: No data found for {index_id} on {date}")
            return False
            
        logger.info(f"Verified {len(df)} constituents for {index_id} on {date}")
    else:
        # Verify latest data
        df = db.get_index_constituents(index_id)
        if df.empty:
            logger.warning(f"Verification failed: No data found for {index_id}")
            return False
            
        # Get the reference date
        reference_date = df.iloc[0]['reference_date'] if 'reference_date' in df.columns else 'unknown date'
        logger.info(f"Verified {len(df)} constituents for {index_id} as of {reference_date}")
    
    return True

def main():
    """Main function to run the import process"""
    args = parse_args()
    
    # Initialize database connection
    db = IndexDatabase(args.db_path)
    
    # Create importer with the file-based provider
    importer = IndexConstituentImporter(db, args.data_folder)
    
    # Get indices to import
    indices = get_indices_to_import(db, args.index)
    logger.info(f"Preparing to import data for indices: {', '.join(indices)}")
    
    # Perform the import based on selected mode
    try:
        if args.current:
            results = import_current_constituents(importer, indices)
        elif args.date:
            results = import_historical_constituents(importer, indices, args.date)
        elif args.date_range:
            results = import_date_range(importer, indices, args.date_range[0], args.date_range[1])
        elif args.all_history:
            results = import_all_history(importer, indices)
            
        # Display results
        logger.info("Import results:")
        for index_id, count in results.items():
            if count > 0:
                logger.info(f"  {index_id}: {count} records imported")
            else:
                logger.warning(f"  {index_id}: No data imported")
                
        # Verify imported data if requested
        if args.verify:
            logger.info("Verifying imported data...")
            for index_id in indices:
                if args.date:
                    verify_imported_data(db, index_id, args.date)
                elif args.current:
                    verify_imported_data(db, index_id)
                # For date range and all history, we skip verification as it would be too verbose
    
    except Exception as e:
        logger.error(f"Error during import: {e}", exc_info=True)
        return 1
        
    finally:
        # Clean up
        db.conn.close()
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 