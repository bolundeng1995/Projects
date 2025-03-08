import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
from src.data.bloomberg_client import BloombergClient

class IndexConstituentAnalyzer:
    """
    Analyzes index constituent changes, detects announcements,
    and identifies additions/deletions
    """
    
    def __init__(self, database, bloomberg_client: BloombergClient):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        
    def detect_constituent_changes(self, index_id: str, lookback_days: int = 30) -> pd.DataFrame:
        """
        Detect changes in index constituents by comparing current constituents
        with previous records
        
        Args:
            index_id: Internal index identifier
            lookback_days: Days to look back for comparison
            
        Returns:
            DataFrame with detected changes
        """
        try:
            # Get current constituents
            current = self.db.get_current_constituents(index_id)
            
            if current.empty:
                self.logger.warning(f"No current constituents found for {index_id}")
                return pd.DataFrame()
                
            # Get constituents from lookback_days ago
            previous_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            previous = self.db.get_historical_constituents(index_id, as_of_date=previous_date)
            
            if previous.empty:
                self.logger.warning(f"No historical constituents found for {index_id} as of {previous_date}")
                return pd.DataFrame()
                
            # Identify additions (in current but not in previous)
            current_tickers = set(current['ticker'])
            previous_tickers = set(previous['ticker'])
            
            additions = current_tickers - previous_tickers
            deletions = previous_tickers - current_tickers
            
            # Create a DataFrame of changes
            changes = []
            
            for ticker in additions:
                row = current[current['ticker'] == ticker].iloc[0]
                changes.append({
                    'index_id': index_id,
                    'ticker': ticker,
                    'bloomberg_ticker': row['bloomberg_ticker'],
                    'event_type': 'ADDITION',
                    'weight': row['weight'],
                    'sector': row['sector'],
                    'detection_date': datetime.now().strftime('%Y-%m-%d')
                })
                
            for ticker in deletions:
                row = previous[previous['ticker'] == ticker].iloc[0]
                changes.append({
                    'index_id': index_id,
                    'ticker': ticker,
                    'bloomberg_ticker': row['bloomberg_ticker'],
                    'event_type': 'DELETION',
                    'weight': row['weight'],
                    'sector': row['sector'],
                    'detection_date': datetime.now().strftime('%Y-%m-%d')
                })
                
            return pd.DataFrame(changes)
            
        except Exception as e:
            self.logger.error(f"Error detecting constituent changes for {index_id}: {e}")
            return pd.DataFrame()
            
    def fetch_announcement_dates(self, index_id: str) -> Dict[str, str]:
        """
        Fetch announcement dates for index rebalances from Bloomberg
        
        Args:
            index_id: Internal index identifier
            
        Returns:
            Dictionary mapping event types to announcement dates
        """
        try:
            # Get Bloomberg ticker for the index
            query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
            result = pd.read_sql_query(query, self.db.conn, params=(index_id,))
            
            if result.empty:
                self.logger.error(f"Index {index_id} not found in metadata")
                return {}
                
            bb_ticker = result["bloomberg_ticker"].iloc[0]
            
            # Different indices have different announcement schedules
            # We need to check Bloomberg for this information
            
            # For S&P indices, use INDX_REBALANCE_ANNOUNCE_DT field
            if index_id.startswith("SP"):
                fields = ["INDX_REBALANCE_ANNOUNCE_DT", "INDX_REBALANCE_EFF_DT"]
                dates = self.bloomberg.get_security_info([bb_ticker], fields)
                
                if dates.empty:
                    self.logger.warning(f"No rebalance dates found for {index_id}")
                    return {}
                    
                announce_date = dates.get("INDX_REBALANCE_ANNOUNCE_DT", [None])[0]
                effective_date = dates.get("INDX_REBALANCE_EFF_DT", [None])[0]
                
                return {
                    "QUARTERLY_REBALANCE": {
                        "announcement_date": announce_date,
                        "implementation_date": effective_date
                    }
                }
                
            # For Russell indices, use separate fields for reconstitution
            elif index_id.startswith("RUSSELL"):
                fields = [
                    "RECONSTITUTION_PRELIM_DATE", 
                    "RECONSTITUTION_ANNOUNCE_DT", 
                    "RECONSTITUTION_EFFECT_DATE"
                ]
                dates = self.bloomberg.get_security_info([bb_ticker], fields)
                
                if dates.empty:
                    self.logger.warning(f"No reconstitution dates found for {index_id}")
                    return {}
                
                prelim_date = dates.get("RECONSTITUTION_PRELIM_DATE", [None])[0]
                announce_date = dates.get("RECONSTITUTION_ANNOUNCE_DT", [None])[0]
                effect_date = dates.get("RECONSTITUTION_EFFECT_DATE", [None])[0]
                
                return {
                    "ANNUAL_RECONSTITUTION": {
                        "preliminary_date": prelim_date,
                        "announcement_date": announce_date,
                        "implementation_date": effect_date
                    }
                }
                
            # For Nasdaq indices
            elif index_id.startswith("NASDAQ"):
                # Nasdaq has different rebalance schedule and fields
                fields = ["INDX_REBALANCE_ANNOUNCE_DT", "INDX_REBALANCE_EFF_DT"]
                dates = self.bloomberg.get_security_info([bb_ticker], fields)
                
                if dates.empty:
                    self.logger.warning(f"No rebalance dates found for {index_id}")
                    return {}
                    
                announce_date = dates.get("INDX_REBALANCE_ANNOUNCE_DT", [None])[0]
                effective_date = dates.get("INDX_REBALANCE_EFF_DT", [None])[0]
                
                return {
                    "QUARTERLY_REBALANCE": {
                        "announcement_date": announce_date,
                        "implementation_date": effective_date
                    }
                }
                
            return {}
            
        except Exception as e:
            self.logger.error(f"Error fetching announcement dates for {index_id}: {e}")
            return {}
            
    def analyze_historical_patterns(self, index_id: str) -> Dict[str, Any]:
        """
        Analyze historical patterns of constituent changes
        
        Args:
            index_id: Internal index identifier
            
        Returns:
            Dictionary with analysis results
        """
        try:
            # Get historical constituent changes
            changes = self.db.get_historical_changes(index_id)
            
            if changes.empty:
                self.logger.warning(f"No historical changes found for {index_id}")
                return {}
                
            # Analyze by event type
            additions = changes[changes['event_type'] == 'ADDITION']
            deletions = changes[changes['event_type'] == 'DELETION']
            
            # Calculate time between announcement and implementation
            if 'announcement_date' in changes.columns and 'implementation_date' in changes.columns:
                changes['announcement_date'] = pd.to_datetime(changes['announcement_date'])
                changes['implementation_date'] = pd.to_datetime(changes['implementation_date'])
                changes['days_between'] = (changes['implementation_date'] - changes['announcement_date']).dt.days
                
                avg_days = changes['days_between'].mean()
                median_days = changes['days_between'].median()
            else:
                avg_days = None
                median_days = None
                
            # Analyze by sector
            if 'sector' in changes.columns:
                sector_counts = changes.groupby(['event_type', 'sector']).size().reset_index(name='count')
                sector_analysis = sector_counts.pivot(index='sector', columns='event_type', values='count').fillna(0)
            else:
                sector_analysis = pd.DataFrame()
                
            return {
                'total_changes': len(changes),
                'addition_count': len(additions),
                'deletion_count': len(deletions),
                'avg_days_to_implementation': avg_days,
                'median_days_to_implementation': median_days,
                'sector_analysis': sector_analysis.to_dict() if not sector_analysis.empty else {}
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing historical patterns for {index_id}: {e}")
            return {} 