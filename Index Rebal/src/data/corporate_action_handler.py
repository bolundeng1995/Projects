import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime, timedelta
from src.data.bloomberg_client import BloombergClient

class CorporateActionHandler:
    """
    Handles corporate actions including mergers, acquisitions,
    ticker changes, and other events
    """
    
    def __init__(self, database, bloomberg_client: BloombergClient):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        
    def update_corporate_actions(self, lookback_days: int = 30, lookforward_days: int = 90):
        """
        Update the database with recent and upcoming corporate actions
        
        Args:
            lookback_days: Days to look back for past actions
            lookforward_days: Days to look forward for upcoming actions
            
        Returns:
            Number of actions added to the database
        """
        try:
            # Calculate date range
            end_date = (datetime.now() + timedelta(days=lookforward_days)).strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            # Get all unique tickers for monitored indices
            all_tickers = self._get_all_monitored_tickers()
            
            if not all_tickers:
                self.logger.warning("No tickers found to check for corporate actions")
                return 0
                
            # Get corporate actions for these tickers
            bloomberg_tickers = [f"{ticker} Equity" for ticker in all_tickers]
            actions = self.bloomberg.get_corporate_actions(bloomberg_tickers, start_date, end_date)
            
            if actions.empty:
                self.logger.info(f"No corporate actions found for the period {start_date} to {end_date}")
                return 0
                
            # Process and store actions
            action_count = 0
            for _, action in actions.iterrows():
                ticker = action['ticker'].replace(" Equity", "")
                bloomberg_ticker = action['ticker']
                action_type = action['action_type']
                action_date = action['action_date']
                description = action['action_description']
                
                # Determine effective date (if available)
                effective_date = action.get('effective_date', action_date)
                
                # Add to database
                success = self.db.add_corporate_action(
                    ticker=ticker,
                    bloomberg_ticker=bloomberg_ticker,
                    action_type=action_type,
                    ex_date=action_date,
                    effective_date=effective_date,
                    description=description
                )
                
                if success:
                    action_count += 1
                    
            self.logger.info(f"Added {action_count} corporate actions to the database")
            return action_count
            
        except Exception as e:
            self.logger.error(f"Error updating corporate actions: {e}")
            return 0
            
    def update_mergers_acquisitions(self, lookback_days: int = 30, lookforward_days: int = 90):
        """
        Update the database with recent and upcoming M&A activity
        
        Args:
            lookback_days: Days to look back for past events
            lookforward_days: Days to look forward for upcoming events
            
        Returns:
            Number of M&A events added to the database
        """
        try:
            # Calculate date range
            end_date = (datetime.now() + timedelta(days=lookforward_days)).strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            # Get M&A data from Bloomberg
            ma_data = self.bloomberg.get_mergers_acquisitions(start_date, end_date)
            
            if ma_data.empty:
                self.logger.info(f"No M&A events found for the period {start_date} to {end_date}")
                return 0
                
            # Process and store M&A events
            event_count = 0
            for _, deal in ma_data.iterrows():
                # Check if target is in our monitored universe
                target_ticker = deal.get('TargetTicker', '').replace(" Equity", "")
                if not target_ticker:
                    continue
                    
                # Check if this target is in our monitored universe
                if not self._is_ticker_monitored(target_ticker):
                    continue
                    
                # Format the description
                acquirer_name = deal.get('AcquirerName', 'Unknown Acquirer')
                target_name = deal.get('TargetName', 'Unknown Target')
                deal_value = deal.get('DealValue', 'Undisclosed')
                currency = deal.get('DealValueCurrency', '')
                description = f"{acquirer_name} acquiring {target_name} for {deal_value} {currency}"
                
                # Add to database as a corporate action
                success = self.db.add_corporate_action(
                    ticker=target_ticker,
                    bloomberg_ticker=f"{target_ticker} Equity",
                    action_type="ACQUISITION",
                    ex_date=deal.get('AnnouncementDate', ''),
                    effective_date=deal.get('ExpectedCompletionDate', ''),
                    description=description,
                    details=deal.to_json()
                )
                
                if success:
                    event_count += 1
                    
            self.logger.info(f"Added {event_count} M&A events to the database")
            return event_count
            
        except Exception as e:
            self.logger.error(f"Error updating M&A events: {e}")
            return 0
            
    def update_ticker_changes(self, lookback_days: int = 30, lookforward_days: int = 30):
        """
        Update the database with recent and upcoming ticker changes
        
        Args:
            lookback_days: Days to look back for past changes
            lookforward_days: Days to look forward for upcoming changes
            
        Returns:
            Number of ticker changes added to the database
        """
        try:
            # Calculate date range
            end_date = (datetime.now() + timedelta(days=lookforward_days)).strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            # Get ticker changes from Bloomberg
            ticker_changes = self.bloomberg.get_ticker_changes(start_date, end_date)
            
            if ticker_changes.empty:
                self.logger.info(f"No ticker changes found for the period {start_date} to {end_date}")
                return 0
                
            # Process and store ticker changes
            change_count = 0
            for _, change in ticker_changes.iterrows():
                old_ticker = change.get('Old Ticker', '').replace(" Equity", "")
                new_ticker = change.get('New Ticker', '').replace(" Equity", "")
                
                # Check if the old ticker is in our monitored universe
                if not self._is_ticker_monitored(old_ticker):
                    continue
                    
                # Format the description
                old_name = change.get('Old Name', '')
                new_name = change.get('New Name', '')
                change_reason = change.get('Change Reason', '')
                description = f"Ticker change from {old_ticker} ({old_name}) to {new_ticker} ({new_name}). Reason: {change_reason}"
                
                # Add to database as a corporate action
                success = self.db.add_corporate_action(
                    ticker=old_ticker,
                    bloomberg_ticker=f"{old_ticker} Equity",
                    action_type="TICKER_CHANGE",
                    ex_date=change.get('Change Date', ''),
                    effective_date=change.get('Change Date', ''),
                    description=description,
                    details=f"New Ticker: {new_ticker}"
                )
                
                if success:
                    change_count += 1
                    
                    # Update any index constituent entries
                    self._update_ticker_in_constituents(old_ticker, new_ticker, new_name)
                    
            self.logger.info(f"Added {change_count} ticker changes to the database")
            return change_count
            
        except Exception as e:
            self.logger.error(f"Error updating ticker changes: {e}")
            return 0
            
    def _get_all_monitored_tickers(self) -> List[str]:
        """Get all tickers currently being monitored in the database"""
        try:
            query = "SELECT DISTINCT ticker FROM current_constituents"
            result = pd.read_sql_query(query, self.db.conn)
            
            if result.empty:
                return []
                
            return result['ticker'].tolist()
            
        except Exception as e:
            self.logger.error(f"Error retrieving monitored tickers: {e}")
            return []
            
    def _is_ticker_monitored(self, ticker: str) -> bool:
        """Check if a ticker is currently monitored in the database"""
        try:
            query = "SELECT COUNT(*) FROM current_constituents WHERE ticker = ?"
            result = self.db.conn.execute(query, (ticker,)).fetchone()
            
            return result[0] > 0
            
        except Exception as e:
            self.logger.error(f"Error checking if ticker {ticker} is monitored: {e}")
            return False
            
    def _update_ticker_in_constituents(self, old_ticker: str, new_ticker: str, new_name: str = ""):
        """Update ticker references in the database when a ticker changes"""
        try:
            # Get all indices that contain the old ticker
            query = "SELECT index_id, weight, sector, industry, market_cap FROM current_constituents WHERE ticker = ?"
            indices = pd.read_sql_query(query, self.db.conn, params=(old_ticker,))
            
            if indices.empty:
                return
                
            # Update each index constituent
            for _, row in indices.iterrows():
                index_id = row['index_id']
                
                # Remove the old ticker
                self.db.conn.execute(
                    "DELETE FROM current_constituents WHERE index_id = ? AND ticker = ?",
                    (index_id, old_ticker)
                )
                
                # Add the new ticker
                self.db.add_constituent(
                    index_id=index_id,
                    ticker=new_ticker,
                    bloomberg_ticker=f"{new_ticker} Equity",
                    data={
                        'company_name': new_name,
                        'weight': row['weight'],
                        'sector': row['sector'],
                        'industry': row['industry'],
                        'market_cap': row['market_cap'],
                        'last_updated': datetime.now().date()
                    }
                )
                
                self.logger.info(f"Updated ticker from {old_ticker} to {new_ticker} in index {index_id}")
                
            self.db.conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error updating ticker references for {old_ticker} to {new_ticker}: {e}")
            self.db.conn.rollback() 