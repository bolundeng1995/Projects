import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
import os
import glob

class IndexConstituentAnalyzer:
    """
    Analyzes index constituent changes, detects patterns,
    and generates insights from constituent data
    """
    
    def __init__(self, database, data_folder: str = 'data/constituents'):
        self.db = database
        self.logger = logging.getLogger(__name__)
        self.data_folder = data_folder
        
    def detect_constituent_changes(self, index_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Detect changes in index constituents by comparing two dates
        
        Args:
            index_id: Internal index identifier
            start_date: Starting date in YYYY-MM-DD format
            end_date: Ending date in YYYY-MM-DD format
            
        Returns:
            DataFrame with detected changes
        """
        try:
            # Get constituents for both dates
            start_constituents = self.db.get_index_constituents(index_id, start_date)
            end_constituents = self.db.get_index_constituents(index_id, end_date)
            
            if start_constituents.empty or end_constituents.empty:
                self.logger.warning(f"Missing data for comparison between {start_date} and {end_date}")
                return pd.DataFrame()
            
            # Extract symbols from both dates
            start_symbols = set(start_constituents['symbol'].unique())
            end_symbols = set(end_constituents['symbol'].unique())
            
            # Find additions and deletions
            additions = end_symbols - start_symbols
            deletions = start_symbols - end_symbols
            
            # Find weight changes for symbols in both sets
            common_symbols = start_symbols & end_symbols
            weight_changes = []
            
            for symbol in common_symbols:
                start_weight = start_constituents[start_constituents['symbol'] == symbol]['index_weight'].values[0]
                end_weight = end_constituents[end_constituents['symbol'] == symbol]['index_weight'].values[0]
                
                if abs(end_weight - start_weight) > 0.0001:  # Use a small threshold to account for floating point errors
                    weight_changes.append({
                        'symbol': symbol,
                        'old_weight': start_weight,
                        'new_weight': end_weight,
                        'change': end_weight - start_weight
                    })
            
            # Create a DataFrame with all changes
            changes = []
            
            # Add additions
            for symbol in additions:
                row = end_constituents[end_constituents['symbol'] == symbol].iloc[0]
                changes.append({
                    'index_id': index_id,
                    'symbol': symbol,
                    'event_type': 'ADDITION',
                    'start_date': start_date,
                    'end_date': end_date,
                    'old_weight': 0,
                    'new_weight': row['index_weight'],
                    'change': row['index_weight']
                })
            
            # Add deletions
            for symbol in deletions:
                row = start_constituents[start_constituents['symbol'] == symbol].iloc[0]
                changes.append({
                    'index_id': index_id,
                    'symbol': symbol,
                    'event_type': 'DELETION',
                    'start_date': start_date,
                    'end_date': end_date,
                    'old_weight': row['index_weight'],
                    'new_weight': 0,
                    'change': -row['index_weight']
                })
            
            # Add weight changes
            for change in weight_changes:
                changes.append({
                    'index_id': index_id,
                    'symbol': change['symbol'],
                    'event_type': 'WEIGHT_CHANGE',
                    'start_date': start_date,
                    'end_date': end_date,
                    'old_weight': change['old_weight'],
                    'new_weight': change['new_weight'],
                    'change': change['change']
                })
            
            return pd.DataFrame(changes)
                
        except Exception as e:
            self.logger.error(f"Error detecting constituent changes for {index_id}: {e}")
            return pd.DataFrame()
    
    def find_available_reference_dates(self, index_id: str) -> List[str]:
        """
        Find all available reference dates for an index
        
        Args:
            index_id: Index identifier
            
        Returns:
            List of available reference dates in descending order
        """
        try:
            # Query the database for all reference dates for this index
            query = '''
                SELECT DISTINCT reference_date 
                FROM index_constituents 
                WHERE index_id = ?
                ORDER BY reference_date DESC
            '''
            
            df = pd.read_sql_query(query, self.db.conn, params=(index_id,))
            
            if df.empty:
                return []
            
            return df['reference_date'].tolist()
            
        except Exception as e:
            self.logger.error(f"Error finding reference dates for {index_id}: {e}")
            return []
    
    def analyze_historical_patterns(self, index_id: str, lookback_periods: int = 4) -> Dict[str, Any]:
        """
        Analyze historical patterns of constituent changes
        
        Args:
            index_id: Internal index identifier
            lookback_periods: Number of rebalance periods to analyze
            
        Returns:
            Dictionary with analysis results
        """
        try:
            # Get available reference dates
            dates = self.find_available_reference_dates(index_id)
            
            if len(dates) < 2:
                self.logger.warning(f"Insufficient historical data for {index_id}")
                return {}
            
            # Limit to the lookback periods
            dates = dates[:min(lookback_periods+1, len(dates))]
            
            # Analyze changes between each pair of consecutive dates
            all_changes = []
            
            for i in range(len(dates)-1):
                end_date = dates[i]
                start_date = dates[i+1]
                
                period_changes = self.detect_constituent_changes(index_id, start_date, end_date)
                
                if not period_changes.empty:
                    all_changes.append(period_changes)
            
            if not all_changes:
                return {}
                
            # Combine all changes into one DataFrame
            combined_changes = pd.concat(all_changes, ignore_index=True)
            
            # Calculate statistics
            additions = combined_changes[combined_changes['event_type'] == 'ADDITION']
            deletions = combined_changes[combined_changes['event_type'] == 'DELETION']
            weight_changes = combined_changes[combined_changes['event_type'] == 'WEIGHT_CHANGE']
            
            avg_additions_per_period = len(additions) / (len(dates)-1)
            avg_deletions_per_period = len(deletions) / (len(dates)-1)
            avg_weight_changes_per_period = len(weight_changes) / (len(dates)-1)
            
            # Calculate turnover metrics
            if not additions.empty and not deletions.empty:
                avg_addition_weight = additions['new_weight'].mean()
                avg_deletion_weight = deletions['old_weight'].mean()
                avg_weight_change = weight_changes['change'].abs().mean() if not weight_changes.empty else 0
                
                # Calculate one-way turnover (sum of absolute weight changes)
                total_weight_change = (
                    additions['new_weight'].sum() +
                    deletions['old_weight'].sum() +
                    weight_changes['change'].abs().sum() if not weight_changes.empty else 0
                )
                
                avg_turnover_per_period = total_weight_change / (len(dates)-1)
            else:
                avg_addition_weight = 0
                avg_deletion_weight = 0
                avg_weight_change = 0
                avg_turnover_per_period = 0
            
            return {
                'periods_analyzed': len(dates)-1,
                'reference_dates': dates,
                'avg_additions_per_period': avg_additions_per_period,
                'avg_deletions_per_period': avg_deletions_per_period,
                'avg_weight_changes_per_period': avg_weight_changes_per_period,
                'avg_addition_weight': avg_addition_weight,
                'avg_deletion_weight': avg_deletion_weight,
                'avg_weight_change': avg_weight_change,
                'avg_turnover_per_period': avg_turnover_per_period,
                'total_additions': len(additions),
                'total_deletions': len(deletions),
                'total_weight_changes': len(weight_changes)
            }
                
        except Exception as e:
            self.logger.error(f"Error analyzing historical patterns for {index_id}: {e}")
            return {} 