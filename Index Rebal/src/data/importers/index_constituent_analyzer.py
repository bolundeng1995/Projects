import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
from src.data.bloomberg_client import BloombergClient
from src.data.importers.index_constituents import IndexConstituentImporter

class IndexConstituentAnalyzer:
    """
    Analyzes index constituent changes, detects announcements,
    and identifies additions/deletions
    """
    
    def __init__(self, database, bloomberg_client):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        
        # Create constituent importer to access providers
        self.constituent_importer = IndexConstituentImporter(database, bloomberg_client)
        
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
            
    def fetch_announcement_dates(self, index_id: str) -> Dict[str, Dict[str, str]]:
        """
        Fetch upcoming rebalance announcement dates for an index
        
        Args:
            index_id: Index identifier
            
        Returns:
            Dict with announcement types and dates
        """
        # Get index metadata
        index_metadata = self.db.get_index_metadata(index_id)
        
        if not index_metadata:
            self.logger.error(f"No metadata found for index {index_id}")
            return {}
        
        # Get the appropriate provider using the importer
        provider = self.constituent_importer._get_provider(index_id)
        bloomberg_ticker = index_metadata.get('bloomberg_ticker')
        
        # Delegate to the provider
        return provider.get_announcement_dates(bloomberg_ticker)
            
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