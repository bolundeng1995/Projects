#!/usr/bin/env python
"""
Index Metadata Reader

A simple script to read and display data from the index_metadata table.
Provides options to filter by index_id or display all indices.

Usage Examples:
    # Display all indices in tabular format
    python read_index_metadata.py
    
    # Look up a specific index by ID
    python read_index_metadata.py --index-id SP500
    
    # Find all quarterly rebalanced indices
    python read_index_metadata.py --filter Quarterly
    
    # Export all indices to CSV
    python read_index_metadata.py --format csv --output indices.csv
    
    # Find all S&P indices and output as JSON
    python read_index_metadata.py --filter "S&P" --format json
    
    # Use a custom database path
    python read_index_metadata.py --db-path /path/to/custom/index_data.db
"""

import sys
import os
import logging
import argparse
import pandas as pd

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data.database import IndexDatabase

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('read_index_metadata')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Read index metadata from the database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Display all indices
  %(prog)s
  
  # Look up a specific index
  %(prog)s --index-id SP500
  
  # Find all indices with 'Quarterly' in any field
  %(prog)s --filter Quarterly
  
  # Export to CSV file
  %(prog)s --format csv --output indices.csv
"""
    )
    parser.add_argument('--db-path', type=str, default='index_data.db',
                        help='Path to the database file (default: index_data.db)')
    parser.add_argument('--index-id', type=str, help='Specific index ID to lookup')
    parser.add_argument('--filter', type=str, help='Filter by text in any field')
    parser.add_argument('--format', choices=['table', 'csv', 'json'], default='table',
                        help='Output format (default: table)')
    parser.add_argument('--output', type=str, help='Write output to file instead of console')
    return parser.parse_args()

def get_all_indices(db):
    """Get all indices from the database"""
    return db.get_all_indices()

def get_index_by_id(db, index_id):
    """Get a specific index by ID"""
    query = "SELECT * FROM index_metadata WHERE index_id = ?"
    return pd.read_sql_query(query, db.conn, params=(index_id,))

def filter_indices(df, filter_text):
    """Filter indices by text in any field"""
    if df.empty:
        return df
        
    # Convert all columns to string for filtering
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Filter rows where any column contains the filter text
    mask = df.apply(lambda row: row.str.contains(filter_text, case=False).any(), axis=1)
    return df[mask]

def display_indices(df, format_type='table', output_file=None):
    """Display indices in the specified format"""
    if df.empty:
        print("No indices found matching the criteria.")
        return

    # Format the output
    if format_type == 'csv':
        output = df.to_csv(index=False)
    elif format_type == 'json':
        output = df.to_json(orient='records', indent=2)
    else:  # table
        output = df.to_string(index=False)
    
    # Write to file or display
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output)
        print(f"Output written to {output_file}")
    else:
        print(output)

def main():
    """Main function to read index metadata from the database"""
    args = parse_args()
    
    # Initialize database connection
    db = IndexDatabase(args.db_path)
    logger.info(f"Connected to database: {args.db_path}")
    
    try:
        # Get indices based on arguments
        if args.index_id:
            logger.info(f"Retrieving index with ID: {args.index_id}")
            indices_df = get_index_by_id(db, args.index_id)
        else:
            logger.info("Retrieving all indices")
            indices_df = get_all_indices(db)
        
        # Apply filter if specified
        if args.filter:
            logger.info(f"Filtering indices by: {args.filter}")
            indices_df = filter_indices(indices_df, args.filter)
        
        # Display results
        display_indices(indices_df, args.format, args.output)
        
        # Summary
        logger.info(f"Found {len(indices_df)} indices matching the criteria")
        
    except Exception as e:
        logger.error(f"Error retrieving indices: {e}")
        return 1
    finally:
        # Close database connection
        db.conn.close()
        logger.info("Database connection closed")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 