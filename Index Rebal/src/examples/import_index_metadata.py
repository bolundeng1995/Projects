#!/usr/bin/env python
"""
Index Metadata Import Script

A simple script to populate the index_metadata table in the database.
This creates basic information about indices that can then be used
for importing constituent and price data.

Usage Examples:
    # Import all predefined indices into the default database
    python import_index_metadata.py
    
    # Import to a custom database location
    python import_index_metadata.py --db-path /path/to/my/database.db
    
    # Debug mode with more verbose output
    python import_index_metadata.py --debug
"""

import sys
import os
import logging
import argparse

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('import_index_metadata')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Import index metadata to the database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Import all predefined indices
  %(prog)s
  
  # Use a custom database path
  %(prog)s --db-path /path/to/custom/index_data.db
  
  # Enable debug logging
  %(prog)s --debug
"""
    )
    parser.add_argument('--db-path', type=str, default='index_data.db',
                        help='Path to the database file (default: index_data.db)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    return parser.parse_args()

def main():
    """Main function to import index metadata to the database"""
    # Specify database path (default is 'index_data.db' in current directory)
    db_path = 'index_data.db'
    
    # Initialize database connection
    db = IndexDatabase(db_path)
    logger.info(f"Connected to database: {db_path}")
    
    # Define index metadata to import
    indices = [
        {
            'index_id': 'SP500',
            'index_name': 'S&P 500',
            'bloomberg_ticker': 'SPX Index',
            'rebalance_frequency': 'Quarterly',
            'description': 'Large-cap US equity index with 500 leading companies'
        },
        {
            'index_id': 'SP400',
            'index_name': 'S&P MidCap 400',
            'bloomberg_ticker': 'MID Index',
            'rebalance_frequency': 'Quarterly',
            'description': 'Mid-cap US equity index with 400 companies'
        },
        {
            'index_id': 'SP600',
            'index_name': 'S&P SmallCap 600',
            'bloomberg_ticker': 'SML Index',
            'rebalance_frequency': 'Quarterly',
            'description': 'Small-cap US equity index with 600 companies'
        },
        {
            'index_id': 'RUSSELL1000',
            'index_name': 'Russell 1000',
            'bloomberg_ticker': 'RIY Index',
            'rebalance_frequency': 'Annual',
            'description': 'Large-cap US equity index with approximately 1000 companies'
        },
        {
            'index_id': 'RUSSELL2000',
            'index_name': 'Russell 2000',
            'bloomberg_ticker': 'RTY Index',
            'rebalance_frequency': 'Annual',
            'description': 'Small-cap US equity index with approximately 2000 companies'
        },
        {
            'index_id': 'NASDAQ100',
            'index_name': 'NASDAQ-100',
            'bloomberg_ticker': 'NDX Index',
            'rebalance_frequency': 'Annual',
            'description': 'Large-cap growth index of 100 non-financial companies listed on NASDAQ'
        },
        {
            'index_id': 'MSCI_EAFE',
            'index_name': 'MSCI EAFE',
            'bloomberg_ticker': 'MXEA Index',
            'rebalance_frequency': 'Quarterly',
            'description': 'Developed markets index excluding US and Canada'
        },
        {
            'index_id': 'MSCI_EM',
            'index_name': 'MSCI Emerging Markets',
            'bloomberg_ticker': 'MXEF Index',
            'rebalance_frequency': 'Quarterly',
            'description': 'Index of mid and large-cap companies in emerging markets'
        }
    ]
    
    # Import each index to the database
    success_count = 0
    for index in indices:
        logger.info(f"Importing metadata for {index['index_name']} ({index['index_id']})")
        
        result = db.add_index(
            index_id=index['index_id'],
            index_name=index['index_name'],
            bloomberg_ticker=index['bloomberg_ticker'],
            rebalance_frequency=index['rebalance_frequency'],
            description=index['description']
        )
        
        if result:
            success_count += 1
            logger.info(f"Successfully imported {index['index_id']}")
        else:
            logger.error(f"Failed to import {index['index_id']}")
    
    # Summary
    logger.info(f"Import complete: {success_count}/{len(indices)} indices imported successfully")
    
    # Close database connection
    db.conn.close()
    logger.info("Database connection closed")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 