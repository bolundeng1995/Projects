#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bloomberg Corporate Actions Demo Script

This script demonstrates how to use the BloombergClient to retrieve and 
analyze corporate action data for stocks.

Usage:
    python corporate_actions_demo.py
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our Bloomberg client
from data.bloomberg_client import BloombergClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('corporate_actions_demo')

def analyze_dividends(dividend_df: pd.DataFrame) -> None:
    """Analyze dividend actions"""
    if dividend_df.empty:
        logger.info("No dividend actions found")
        return
    
    # Calculate basic dividend statistics
    logger.info(f"Total dividend actions: {len(dividend_df)}")
    
    # Calculate average dividend amount
    if 'amount' in dividend_df.columns:
        avg_dividend = dividend_df['amount'].mean()
        logger.info(f"Average dividend amount: {avg_dividend:.4f}")
        
        # Get highest dividend
        highest_dividend = dividend_df.sort_values('amount', ascending=False).iloc[0]
        logger.info(f"Highest dividend: {highest_dividend['amount']:.4f} by {highest_dividend['ticker']}")
    
    # Group by ticker and count
    div_counts = dividend_df['ticker'].value_counts()
    logger.info("\nCompanies with most dividend actions:")
    print(div_counts.head(5))

def analyze_splits(split_df: pd.DataFrame) -> None:
    """Analyze stock split actions"""
    if split_df.empty:
        logger.info("No stock split actions found")
        return
    
    logger.info(f"Total stock split actions: {len(split_df)}")
    
    # Calculate split ratio distribution
    if 'ratio' in split_df.columns:
        common_ratios = split_df['ratio'].value_counts()
        logger.info("\nMost common split ratios:")
        print(common_ratios.head(5))

def analyze_mergers(merger_df: pd.DataFrame) -> None:
    """Analyze merger and acquisition actions"""
    if merger_df.empty:
        logger.info("No merger actions found")
        return
    
    logger.info(f"Total merger/acquisition actions: {len(merger_df)}")
    
    # Group by acquirer
    if 'acquirer' in merger_df.columns:
        acquirers = merger_df['acquirer'].value_counts()
        logger.info("\nMost active acquirers:")
        print(acquirers.head(5))
    
    # Show largest deals by value
    if 'deal_value' in merger_df.columns:
        largest_deals = merger_df.sort_values('deal_value', ascending=False)
        logger.info("\nLargest deals by value:")
        print(largest_deals[['ticker', 'acquirer', 'deal_value', 'announcement_date']].head(5))

def process_corporate_actions(ca_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Process corporate actions DataFrame to separate different types
    
    Args:
        ca_df: Raw corporate actions DataFrame
    
    Returns:
        Dictionary of DataFrames for each action type
    """
    if ca_df.empty:
        return {
            'dividends': pd.DataFrame(),
            'splits': pd.DataFrame(),
            'spinoffs': pd.DataFrame(),
            'mergers': pd.DataFrame(),
            'name_changes': pd.DataFrame(),
            'other': pd.DataFrame()
        }
    
    # Initialize result dictionary
    action_dfs = {}
    
    # Filter dividends
    if 'action_type' in ca_df.columns:
        action_dfs['dividends'] = ca_df[ca_df['action_type'].str.contains('DIV', na=False)]
        
        # Filter splits
        action_dfs['splits'] = ca_df[ca_df['action_type'].str.contains('SPLIT', na=False)]
        
        # Filter spinoffs
        action_dfs['spinoffs'] = ca_df[ca_df['action_type'].str.contains('SPINOFF', na=False)]
        
        # Filter mergers and acquisitions
        merger_mask = (
            ca_df['action_type'].str.contains('MERGER', na=False) |
            ca_df['action_type'].str.contains('ACQUISITION', na=False)
        )
        action_dfs['mergers'] = ca_df[merger_mask]
        
        # Filter name changes
        action_dfs['name_changes'] = ca_df[ca_df['action_type'].str.contains('NAME', na=False)]
        
        # Everything else
        all_masks = (
            ca_df['action_type'].str.contains('DIV', na=False) |
            ca_df['action_type'].str.contains('SPLIT', na=False) |
            ca_df['action_type'].str.contains('SPINOFF', na=False) |
            ca_df['action_type'].str.contains('MERGER', na=False) |
            ca_df['action_type'].str.contains('ACQUISITION', na=False) |
            ca_df['action_type'].str.contains('NAME', na=False)
        )
        action_dfs['other'] = ca_df[~all_masks]
    else:
        # If no action_type column, return empty DataFrames
        action_dfs = {
            'dividends': pd.DataFrame(),
            'splits': pd.DataFrame(),
            'spinoffs': pd.DataFrame(),
            'mergers': pd.DataFrame(),
            'name_changes': pd.DataFrame(),
            'other': pd.DataFrame()
        }
    
    return action_dfs

def main():
    """Main function to run the corporate actions demo"""
    logger.info("Starting Bloomberg Corporate Actions Demo")
    
    # Initialize Bloomberg client
    bloomberg = BloombergClient()
    if not bloomberg.start_session():
        logger.error("Failed to start Bloomberg session. Exiting.")
        return
    
    try:
        # Set date range (last 6 months)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        
        logger.info(f"Retrieving corporate actions from {start_date} to {end_date}")
        
        # ------------------------------------------------------------
        # 1. Get corporate actions for S&P 500 companies
        # ------------------------------------------------------------
        logger.info("DEMO 1: Getting corporate actions for S&P 500 constituents")
        
        # First get S&P 500 index members
        sp500_members = bloomberg.get_index_members("SPX Index")
        
        if not sp500_members:
            logger.error("Failed to retrieve S&P 500 constituents. Trying a smaller set of companies.")
            # Use a small set of well-known companies instead
            sp500_members = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "JNJ", "WMT"]
        
        logger.info(f"Found {len(sp500_members)} S&P 500 constituents")
        
        # Sample a subset of companies to avoid excessive API calls
        import random
        if len(sp500_members) > 50:
            sample_tickers = random.sample(sp500_members, 50)
        else:
            sample_tickers = sp500_members
        
        # Convert to Bloomberg Equity format
        sample_securities = [f"{ticker} US Equity" for ticker in sample_tickers]
        
        # Get corporate actions
        corporate_actions = bloomberg.get_corporate_actions(
            securities=sample_securities, 
            start_date=start_date, 
            end_date=end_date
        )
        
        if corporate_actions.empty:
            logger.warning("No corporate actions found for the selected securities")
        else:
            logger.info(f"Retrieved {len(corporate_actions)} corporate actions")
            
            # Process actions by type
            action_types = process_corporate_actions(corporate_actions)
            
            # Print action counts by type
            logger.info("\nCorporate actions by type:")
            for action_type, df in action_types.items():
                logger.info(f"  {action_type.capitalize()}: {len(df)}")
            
            # Analyze different action types
            logger.info("\n--- Dividend Analysis ---")
            analyze_dividends(action_types['dividends'])
            
            logger.info("\n--- Stock Split Analysis ---")
            analyze_splits(action_types['splits'])
            
            logger.info("\n--- Merger & Acquisition Analysis ---")
            analyze_mergers(action_types['mergers'])
            
            # Show a sample of each action type
            for action_type, df in action_types.items():
                if not df.empty:
                    logger.info(f"\nSample of {action_type} actions:")
                    print(df.head(3))
        
        # ------------------------------------------------------------
        # 2. Get M&A deals for a specific time period
        # ------------------------------------------------------------
        logger.info("\n\nDEMO 2: Getting M&A deals for a specific time period")
        
        # Get M&A data for the last 3 months
        ma_start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        ma_deals = bloomberg.get_ma_deals(ma_start_date, end_date)
        
        if ma_deals.empty:
            logger.warning("No M&A deals found for the specified time period")
        else:
            logger.info(f"Retrieved {len(ma_deals)} M&A deals")
            
            # Calculate basic M&A statistics
            if 'deal_value' in ma_deals.columns:
                # Convert deal value to numeric
                ma_deals['deal_value'] = pd.to_numeric(ma_deals['deal_value'], errors='coerce')
                
                # Calculate total deal value
                total_deal_value = ma_deals['deal_value'].sum()
                logger.info(f"Total deal value: ${total_deal_value:,.2f} million")
                
                # Calculate average deal value
                avg_deal_value = ma_deals['deal_value'].mean()
                logger.info(f"Average deal value: ${avg_deal_value:,.2f} million")
                
                # Show largest deals
                logger.info("\nLargest deals by value:")
                if 'target' in ma_deals.columns and 'acquirer' in ma_deals.columns:
                    largest_deals = ma_deals.sort_values('deal_value', ascending=False)
                    print(largest_deals[['target', 'acquirer', 'deal_value', 'announcement_date']].head(5))
            
            # Show deal status distribution
            if 'status' in ma_deals.columns:
                status_counts = ma_deals['status'].value_counts()
                logger.info("\nDeals by status:")
                print(status_counts)
        
        # ------------------------------------------------------------
        # 3. Get ticker changes (symbol and name changes)
        # ------------------------------------------------------------
        logger.info("\n\nDEMO 3: Getting ticker and name changes")
        
        # Get ticker changes for the last year
        ticker_change_start = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        ticker_changes = bloomberg.get_ticker_changes(ticker_change_start, end_date)
        
        if ticker_changes.empty:
            logger.warning("No ticker changes found for the specified time period")
        else:
            logger.info(f"Retrieved {len(ticker_changes)} ticker changes")
            
            # Show sample of ticker changes
            logger.info("\nSample of ticker changes:")
            if 'Old Ticker' in ticker_changes.columns and 'New Ticker' in ticker_changes.columns:
                sample_cols = ['Old Ticker', 'New Ticker', 'Old Name', 'New Name', 'Change Date', 'Change Reason']
                sample_cols = [col for col in sample_cols if col in ticker_changes.columns]
                print(ticker_changes[sample_cols].head(10))
            else:
                print(ticker_changes.head(10))
            
            # Show reasons for ticker changes
            if 'Change Reason' in ticker_changes.columns:
                reason_counts = ticker_changes['Change Reason'].value_counts()
                logger.info("\nReasons for ticker changes:")
                print(reason_counts)
            
            # Show ticker changes by country
            if 'Country' in ticker_changes.columns:
                country_counts = ticker_changes['Country'].value_counts()
                logger.info("\nTicker changes by country:")
                print(country_counts.head(10))
    
    except Exception as e:
        logger.error(f"Error during demo: {e}")
    finally:
        # Always stop the session when done
        bloomberg.stop_session()
        logger.info("Bloomberg session closed")

if __name__ == "__main__":
    main() 