#!/usr/bin/env python
"""
Import Index Constituents Script (V2)

This script uses the FileBasedConstituentProvider and IndexConstituentImporter classes 
from index_constituents.py to import constituent data into the database.

Usage examples:
    # Import all indices' latest constituent data
    python import_index_constituents_v2.py
    
    # Import constituents for a specific index
    python import_index_constituents_v2.py --index SP500
    
    # Import with a specific as-of date
    python import_index_constituents_v2.py --date 2023-12-31
    
    # Import all historical data for an index
    python import_index_constituents_v2.py --index SP500 --all-history
    
    # Import with debug information
    python import_index_constituents_v2.py --debug
"""

import sys
import os
import argparse
import logging
from datetime import datetime
import pandas as pd

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase
from src.data.importers.index_constituents import FileBasedConstituentProvider, IndexConstituentImporter

# Default path to data files
DEFAULT_DATA_FOLDER = r"\\deai.us.world.socgen\ebtsadm\indexman\PROD\RAW_FILES"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('constituent_import.log')
    ]
)
logger = logging.getLogger('import_index_constituents_v2')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Import index constituent data into the database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Index selection
    parser.add_argument('--index', type=str,
                       help='ID of the index to import (default: all indices)')
    
    # Date options
    parser.add_argument('--date', type=str,
                       help='Reference date for the constituent data (YYYY-MM-DD)')
    
    # Database options
    parser.add_argument('--db-path', type=str, default='index_data.db',
                       help='Path to the database file')
    
    # Data folder option
    parser.add_argument('--data-folder', type=str, default=DEFAULT_DATA_FOLDER,
                       help='Path to the folder containing constituent data files')
    
    # Operation options
    parser.add_argument('--all-history', action='store_true',
                       help='Import all available historical data')
    parser.add_argument('--analyze-changes', action='store_true',
                       help='Analyze constituent changes after import')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    return parser.parse_args()

def get_all_indices(db):
    """Get a list of all indices in the database"""
    query = "SELECT index_id FROM index_metadata"
    results = db.cursor.execute(query).fetchall()
    return [row[0] for row in results]

def main():
    """Main function to import index constituent data"""
    args = parse_args()
    
    # Enable debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('src.data.importers.index_constituents').setLevel(logging.DEBUG)
    
    # Check if data folder exists
    if not os.path.exists(args.data_folder):
        logger.error(f"Data folder not found: {args.data_folder}")
        return 1
    
    # Initialize database connection
    db = IndexDatabase(args.db_path)
    logger.info(f"Connected to database: {args.db_path}")
    
    # Initialize constituent importer
    importer = IndexConstituentImporter(db, args.data_folder)
    provider = importer.provider  # Access the underlying FileBasedConstituentProvider
    
    # Get indices to process
    indices_to_process = []
    if args.index:
        # Check if the index exists in the database
        query = "SELECT 1 FROM index_metadata WHERE index_id = ?"
        result = db.cursor.execute(query, (args.index,)).fetchone()
        if result:
            indices_to_process = [args.index]
        else:
            logger.error(f"Index {args.index} not found in database")
            db.conn.close()
            return 1
    else:
        indices_to_process = get_all_indices(db)
    
    if not indices_to_process:
        logger.error("No indices found to process. Please check your database has index metadata.")
        db.conn.close()
        return 1
    
    logger.info(f"Preparing to import {len(indices_to_process)} indices")
    
    # Track import statistics
    total_imports = 0
    dates_imported = 0
    
    for index_id in indices_to_process:
        logger.info(f"Processing index: {index_id}")
        
        # Import based on the options
        if args.all_history:
            # Import all available historical data
            logger.info(f"Importing all available historical data for {index_id}")
            date_count = importer.import_all_available_history(index_id)
            logger.info(f"Imported data for {date_count} dates for {index_id}")
            dates_imported += date_count
        
        elif args.date:
            # Import for a specific date
            logger.info(f"Importing data for {index_id} as of {args.date}")
            count = importer.import_historical_constituents(index_id, args.date)
            logger.info(f"Imported {count} constituents for {index_id} as of {args.date}")
            total_imports += count
            if count > 0:
                dates_imported += 1
        
        else:
            # Import latest available data
            logger.info(f"Importing latest available data for {index_id}")
            count = importer.import_current_constituents(index_id)
            logger.info(f"Imported {count} constituents for {index_id}")
            total_imports += count
            if count > 0:
                dates_imported += 1
        
        # Analyze changes if requested
        if args.analyze_changes and provider.find_available_reference_dates(index_id):
            # Get the two most recent dates with data
            available_dates = provider.find_available_reference_dates(index_id)
            
            if len(available_dates) >= 2:
                end_date = available_dates[0]
                start_date = available_dates[1]
                
                logger.info(f"Analyzing changes for {index_id} between {start_date} and {end_date}")
                changes = importer.detect_changes(index_id, start_date, end_date)
                
                if not changes.empty:
                    additions = len(changes[changes['event_type'] == 'ADDITION'])
                    deletions = len(changes[changes['event_type'] == 'DELETION'])
                    weight_changes = len(changes[changes['event_type'] == 'WEIGHT_CHANGE'])
                    
                    logger.info(f"Found {len(changes)} changes:")
                    logger.info(f"  - Additions: {additions}")
                    logger.info(f"  - Deletions: {deletions}")
                    logger.info(f"  - Weight changes: {weight_changes}")
                    
                    # Save the changes to a CSV file
                    output_file = f"{index_id}_changes_{start_date}_to_{end_date}.csv"
                    changes.to_csv(output_file, index=False)
                    logger.info(f"Saved changes to {output_file}")
                else:
                    logger.info(f"No changes detected for {index_id}")
    
    # Print summary
    if args.all_history:
        logger.info(f"Import completed: processed {dates_imported} dates across {len(indices_to_process)} indices")
    else:
        logger.info(f"Import completed: {total_imports} constituents imported across {len(indices_to_process)} indices")
    
    # Close database connection
    db.conn.close()
    logger.info("Database connection closed")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())