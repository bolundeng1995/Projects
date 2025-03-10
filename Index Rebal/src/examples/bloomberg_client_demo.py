#!/usr/bin/env python
"""
Bloomberg Client Demo

This script demonstrates how to use the various functions in the BloombergClient class
to interact with the Bloomberg API and retrieve data.

Requirements:
    - Bloomberg Terminal with API access
    - Valid Bloomberg subscription
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Bloomberg client
from src.data.bloomberg_client import BloombergClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("bloomberg_demo")

def main():
    """Main function to demonstrate the Bloomberg client functionality"""
    
    # Initialize the Bloomberg client
    # By default, it connects to localhost:8194
    bloomberg = BloombergClient()
    logger.info("Bloomberg client initialized")
    
    # You can also specify custom host and port
    # bloomberg = BloombergClient(host='custom_host', port=8195)
    
    # Start the session
    if not bloomberg.start_session():
        logger.error("Failed to start Bloomberg session. Exiting.")
        return
    
    logger.info("Bloomberg session started successfully")
    
    try:
        # Calculate date ranges for queries
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date_30d = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        start_date_90d = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        start_date_1y = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # ------------------------------------------------------------
        # 1. Basic tests to get current market data
        # ------------------------------------------------------------
        logger.info("DEMO 1: Getting current market data")
        
        # Get current market data for a list of securities
        securities = ["AAPL US Equity", "MSFT US Equity", "AMZN US Equity"]
        fields = ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "VOLUME"]
        
        market_data = bloomberg.get_current_data(securities, fields)
        
        if not market_data.empty:
            logger.info("Current market data:")
            print(market_data)
            print("\n")
        else:
            logger.warning("No current market data retrieved")
        
        # ------------------------------------------------------------
        # 2. Get historical price data
        # ------------------------------------------------------------
        logger.info("DEMO 2: Getting historical price data")
        
        # Get 30 days of historical data for major indices
        index_tickers = ["SPX Index", "NDX Index", "INDU Index"]
        price_fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "VOLUME"]
        
        historical_data = bloomberg.get_historical_data(
            securities=index_tickers,
            fields=price_fields,
            start_date=start_date_30d,
            end_date=end_date
        )
        
        if historical_data:
            for ticker, data in historical_data.items():
                logger.info(f"Historical data for {ticker}:")
                print(data.head())
                print("\n")
        else:
            logger.warning("No historical data retrieved")
        
        # ------------------------------------------------------------
        # 3. Get security information
        # ------------------------------------------------------------
        logger.info("DEMO 3: Getting security information")
        
        equity_tickers = ["AAPL US Equity", "MSFT US Equity", "GOOGL US Equity"]
        info_fields = ["SECURITY_NAME", "GICS_SECTOR_NAME", "GICS_INDUSTRY_NAME", "CUR_MKT_CAP"]
        
        security_info = bloomberg.get_security_info(equity_tickers, info_fields)
        
        if not security_info.empty:
            logger.info("Security information:")
            print(security_info)
            print("\n")
        else:
            logger.warning("No security information retrieved")
        
        # ------------------------------------------------------------
        # 4. Get index member weights
        # ------------------------------------------------------------
        logger.info("DEMO 4: Getting index member weights")
        
        member_weights = bloomberg.get_index_member_weights("SPX Index")
        
        if not member_weights.empty:
            logger.info("S&P 500 member weights (top 10):")
            print(member_weights.head(10))
            print(f"Total members: {len(member_weights)}")
            print("\n")
        else:
            logger.warning("No member weights retrieved")
        
        # ------------------------------------------------------------
        # 5. Get corporate actions
        # ------------------------------------------------------------
        logger.info("DEMO 5: Getting corporate actions")
        
        corporate_actions = bloomberg.get_corporate_actions(
            securities=["AAPL US Equity", "MSFT US Equity", "AMZN US Equity"],
            start_date=start_date_90d,
            end_date=end_date
        )
        
        if not corporate_actions.empty:
            logger.info("Corporate actions:")
            print(corporate_actions)
            print("\n")
        else:
            logger.info("No corporate actions found in the last 90 days")
        
        # ------------------------------------------------------------
        # 6. Get ticker changes
        # ------------------------------------------------------------
        logger.info("DEMO 6: Getting ticker changes")
        
        ticker_changes = bloomberg.get_ticker_changes(
            start_date=start_date_90d,
            end_date=end_date
        )
        
        if not ticker_changes.empty:
            logger.info("Recent ticker changes:")
            print(ticker_changes)
            print("\n")
        else:
            logger.info("No ticker changes found in the last 90 days")
        
        # ------------------------------------------------------------
        # 7. Get index changes
        # ------------------------------------------------------------
        logger.info("DEMO 7: Getting index changes")
        
        # Set dates for comparison (e.g., start of month vs end of month)
        start_date = "2023-01-01"
        end_date = "2023-03-31"
        
        # Get index changes by comparing members between two dates
        index_changes = bloomberg.get_index_changes("SPX Index", start_date, end_date)
        
        if not index_changes.empty:
            # Count changes by type
            additions = index_changes[index_changes['change_type'] == 'ADD']
            deletions = index_changes[index_changes['change_type'] == 'DELETE']
            weight_changes = index_changes[index_changes['change_type'] == 'WEIGHT_CHG']
            
            # Print summary
            logger.info(f"S&P 500 changes from {start_date} to {end_date}:")
            logger.info(f"  Additions: {len(additions)}")
            logger.info(f"  Deletions: {len(deletions)}")
            logger.info(f"  Weight changes: {len(weight_changes)}")
            
            # Print details of each change type
            if not additions.empty:
                logger.info("\nAdditions:")
                print(additions[['ticker', 'new_weight']].head(5))
                if len(additions) > 5:
                    print(f"... and {len(additions) - 5} more additions")
            
            if not deletions.empty:
                logger.info("\nDeletions:")
                print(deletions[['ticker', 'old_weight']].head(5))
                if len(deletions) > 5:
                    print(f"... and {len(deletions) - 5} more deletions")
            
            if not weight_changes.empty:
                logger.info("\nLargest weight changes:")
                # Sort by absolute weight change
                weight_changes['abs_change'] = abs(weight_changes['new_weight'] - weight_changes['old_weight'])
                largest_changes = weight_changes.sort_values('abs_change', ascending=False)
                print(largest_changes[['ticker', 'old_weight', 'new_weight']].head(5))
                if len(weight_changes) > 5:
                    print(f"... and {len(weight_changes) - 5} more weight changes")
        else:
            logger.warning("No index changes found")
        
        print("\n")
        
    except Exception as e:
        logger.error(f"Error during Bloomberg data retrieval: {e}")
    finally:
        # Always stop the session when done
        bloomberg.stop_session()
        logger.info("Bloomberg session closed")

if __name__ == "__main__":
    main() 