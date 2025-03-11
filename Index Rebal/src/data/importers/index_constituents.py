import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

class BaseConstituentProvider:
    """Base class for all constituent providers with common functionality"""
    
    def __init__(self, bloomberg_client):
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
    
    def get_constituents(self, index_ticker: str) -> pd.DataFrame:
        """
        Get current constituents for an index
        
        Args:
            index_ticker: Bloomberg ticker for the index
            
        Returns:
            DataFrame with constituent information
        """
        constituents = self.bloomberg.get_index_member_weights(index_ticker)
        
        if constituents.empty:
            self.logger.warning(f"No constituents found for {index_ticker}")
            return pd.DataFrame()
            
        # Add Bloomberg tickers
        constituents['bloomberg_ticker'] = constituents['member_ticker'].apply(
            lambda x: f"{x} Equity" if not pd.isna(x) else None
        )
        
        # Standardize column names
        constituents = constituents.rename(columns={
            'member_ticker': 'ticker',
            'weight': 'weight',
            'member_name': 'company_name'
        })
        
        # Make sure weight is numeric
        constituents['weight'] = pd.to_numeric(constituents['weight'], errors='coerce')
        
        # Add as_of_date
        constituents['as_of_date'] = datetime.now().strftime('%Y-%m-%d')
        
        return constituents
    
    def get_historical_changes(self, index_ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get historical constituent changes for an index
        
        Args:
            index_ticker: Bloomberg ticker for the index
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with changes information
        """
        changes = self.bloomberg.get_index_changes(index_ticker, start_date, end_date)
        
        if changes.empty:
            self.logger.warning(f"No changes found for {index_ticker} from {start_date} to {end_date}")
            return pd.DataFrame()
        
        # Custom processing if needed (can be overridden by subclasses)
        return self._process_changes(changes)
    
    def _process_changes(self, changes: pd.DataFrame) -> pd.DataFrame:
        """
        Process raw changes data (can be overridden by subclasses)
        
        Args:
            changes: Raw changes DataFrame from Bloomberg
            
        Returns:
            Processed changes DataFrame
        """
        # Default implementation - return as is
        return changes
    
    def get_announcement_dates(self, index_ticker: str) -> Dict[str, Dict[str, str]]:
        """
        Get upcoming announcement dates for index rebalance
        
        Args:
            index_ticker: Bloomberg ticker for the index
            
        Returns:
            Dict with announcement and implementation dates
        """
        # This is a stub implementation - subclasses should override
        self.logger.warning(f"get_announcement_dates not implemented for {index_ticker}")
        return {}


class SPConstituentProvider(BaseConstituentProvider):
    """Provider for S&P indices"""
    
    def get_announcement_dates(self, index_ticker: str) -> Dict[str, Dict[str, str]]:
        """
        Get upcoming S&P index rebalance dates
        
        Args:
            index_ticker: Bloomberg ticker for S&P index
            
        Returns:
            Dict with announcement and implementation dates
        """
        # S&P specific implementation
        self.logger.info(f"Getting announcement dates for S&P index: {index_ticker}")
        
        # S&P-specific logic here...
        # For example, using a calendar service or Bloomberg API
        
        # Return data in format:
        # {'quarterly_rebalance': {'announcement_date': 'YYYY-MM-DD', 'implementation_date': 'YYYY-MM-DD'}}
        
        # Example implementation - this would be replaced with actual API calls
        return {'quarterly_rebalance': {
            'announcement_date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
            'implementation_date': (datetime.now() + timedelta(days=37)).strftime('%Y-%m-%d')
        }}
    
    def _process_changes(self, changes: pd.DataFrame) -> pd.DataFrame:
        """S&P-specific changes processing"""
        # If needed, add S&P-specific transformations here
        return changes


class RussellConstituentProvider(BaseConstituentProvider):
    """Provider for Russell indices"""
    
    def get_announcement_dates(self, index_ticker: str) -> Dict[str, Dict[str, str]]:
        """
        Get upcoming Russell index rebalance dates
        
        Returns:
            Dict with announcement and implementation dates
        """
        # Russell specific implementation
        self.logger.info(f"Getting announcement dates for Russell index: {index_ticker}")
        
        # Russell uses an annual reconstitution in June
        # Logic to determine the exact date based on current date
        
        return {'annual_reconstitution': {
            'announcement_date': '2023-06-09',  # Would be calculated dynamically
            'implementation_date': '2023-06-26'  # Would be calculated dynamically
        }}
    
    def _process_changes(self, changes: pd.DataFrame) -> pd.DataFrame:
        """Russell-specific changes processing"""
        # If needed, add Russell-specific transformations here
        return changes


class NasdaqConstituentProvider(BaseConstituentProvider):
    """Provider for Nasdaq indices"""
    
    def get_announcement_dates(self, index_ticker: str) -> Dict[str, Dict[str, str]]:
        """
        Get upcoming Nasdaq index rebalance dates
        
        Returns:
            Dict with announcement and implementation dates
        """
        # Nasdaq specific implementation
        self.logger.info(f"Getting announcement dates for Nasdaq index: {index_ticker}")
        
        # Nasdaq-specific logic...
        
        return {'quarterly_rebalance': {
            'announcement_date': (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d'),
            'implementation_date': (datetime.now() + timedelta(days=52)).strftime('%Y-%m-%d')
        }}
        
    def _process_changes(self, changes: pd.DataFrame) -> pd.DataFrame:
        """Nasdaq-specific changes processing"""
        # If needed, add Nasdaq-specific transformations here
        return changes


class MSCIConstituentProvider(BaseConstituentProvider):
    """Provider for MSCI indices"""
    
    def get_announcement_dates(self, index_ticker: str) -> Dict[str, Dict[str, str]]:
        """
        Get upcoming MSCI index rebalance dates
        
        Returns:
            Dict with announcement and implementation dates
        """
        # MSCI specific implementation
        self.logger.info(f"Getting announcement dates for MSCI index: {index_ticker}")
        
        # MSCI has quarterly reviews in February, May, August, and November
        # Implementation is usually on the last business day of the month
        
        return {'quarterly_review': {
            'announcement_date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
            'implementation_date': (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d')
        }}
    
    def _process_changes(self, changes: pd.DataFrame) -> pd.DataFrame:
        """MSCI-specific changes processing"""
        # If needed, add MSCI-specific transformations here
        return changes


class IndexConstituentImporter:
    def __init__(self, database, bloomberg_client):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        self.providers = {
            'sp': SPConstituentProvider(bloomberg_client),
            'russell': RussellConstituentProvider(bloomberg_client),
            'nasdaq': NasdaqConstituentProvider(bloomberg_client),
            'msci': MSCIConstituentProvider(bloomberg_client)
        }
    
    def import_current_constituents(self, index_id: str):
        """Import current constituents for a specific index"""
        # Get Bloomberg ticker for the index
        query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
        result = pd.read_sql_query(query, self.db.conn, params=(index_id,))
        
        if result.empty:
            raise ValueError(f"No Bloomberg ticker found for index: {index_id}")
        
        bloomberg_ticker = result.iloc[0]['bloomberg_ticker']
        
        # Get the appropriate provider and fetch constituents
        provider = self._get_provider(index_id)
        constituents = provider.get_constituents(bloomberg_ticker)
        
        # Add constituents to database
        for ticker, data in constituents.iterrows():
            self.db.add_constituent(index_id, ticker, data['bloomberg_ticker'], data.to_dict())
        
        return len(constituents)
    
    def _get_provider(self, index_id: str) -> BaseConstituentProvider:
        """Get the appropriate provider for an index ID"""
        if index_id.startswith('SP'):
            return self.providers['sp']
        elif index_id.startswith('RUSSELL'):
            return self.providers['russell']
        elif index_id.startswith('NASDAQ'):
            return self.providers['nasdaq']
        elif index_id.startswith('MSCI'):
            return self.providers['msci']
        else:
            self.logger.warning(f"No provider found for {index_id}, using default")
            # Return any provider as fallback - they all share base functionality
            return self.providers['sp']

    def import_historical_changes(self, index_id: str, lookback_days: int = 365):
        """
        Import historical constituent changes for an index
        
        Args:
            index_id: Index identifier
            lookback_days: Number of days to look back for changes
            
        Returns:
            Number of changes imported
        """
        self.logger.info(f"Importing historical constituent changes for {index_id}")
        
        # Get index metadata
        indices = self.db.get_all_indices()
        index_row = indices[indices['index_id'] == index_id]
        
        if index_row.empty:
            self.logger.error(f"Index {index_id} not found in database")
            return 0
            
        bloomberg_ticker = index_row.iloc[0]['bloomberg_ticker']
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        try:
            # Get historical changes from Bloomberg
            # Note: This is a placeholder - actual implementation would depend on the
            # specific Bloomberg API endpoint to retrieve historical changes
            provider = self._get_provider(index_id)
            changes = provider.get_historical_changes(bloomberg_ticker, start_date_str, end_date_str)
            
            if changes.empty:
                self.logger.info(f"No historical changes found for {index_id} in the specified period")
                return 0
                
            # Process and store changes
            count = 0
            for _, row in changes.iterrows():
                # Map Bloomberg data to our schema
                event_type = self._map_event_type(row.get('change_type', ''))
                ticker = row.get('ticker', '')
                bloomberg_ticker = row.get('bloomberg_ticker', '')
                
                if not ticker or not event_type:
                    continue
                    
                announcement_date = row.get('announcement_date', None)
                implementation_date = row.get('effective_date', None)
                old_weight = row.get('old_weight', 0.0)
                new_weight = row.get('new_weight', 0.0)
                reason = row.get('reason', '')
                
                # Store in database
                if self.db.add_constituent_change(
                    index_id=index_id,
                    ticker=ticker,
                    bloomberg_ticker=bloomberg_ticker,
                    event_type=event_type,
                    announcement_date=announcement_date,
                    implementation_date=implementation_date,
                    old_weight=old_weight,
                    new_weight=new_weight,
                    reason=reason
                ):
                    count += 1
                    
            self.logger.info(f"Imported {count} historical changes for {index_id}")
            return count
            
        except Exception as e:
            self.logger.error(f"Error importing historical changes for {index_id}: {e}")
            return 0
        
    def _map_event_type(self, bloomberg_event_type: str) -> str:
        """Map Bloomberg event types to our internal types"""
        event_map = {
            'ADD': 'ADD',
            'DELETE': 'DELETE',
            'ADDITION': 'ADD',
            'DELETION': 'DELETE',
            'REMOVAL': 'DELETE',
            'WEIGHT_INCREASE': 'WEIGHT_CHANGE',
            'WEIGHT_DECREASE': 'WEIGHT_CHANGE'
        }
        
        return event_map.get(bloomberg_event_type.upper(), 'OTHER') 