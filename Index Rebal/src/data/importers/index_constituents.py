import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

class IndexConstituentImporter:
    def __init__(self, database, bloomberg_client):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        self.providers = {
            'sp': SPConstituentProvider(bloomberg_client),
            'russell': RussellConstituentProvider(bloomberg_client),
            'nasdaq': NasdaqConstituentProvider(bloomberg_client)
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
        provider_key = self._get_provider_for_index(index_id)
        constituents = self.providers[provider_key].get_current_constituents(index_id, bloomberg_ticker)
        
        # Add constituents to database
        for ticker, data in constituents.items():
            bloomberg_ticker = f"{ticker} Equity"
            self.db.add_constituent(index_id, ticker, bloomberg_ticker, data)
        
        return len(constituents)
    
    def _get_provider_for_index(self, index_id: str) -> str:
        """Determine which provider to use based on index_id"""
        if index_id.startswith('SP'):
            return 'sp'
        elif index_id.startswith('RUSSELL'):
            return 'russell'
        elif index_id.startswith('NASDAQ'):
            return 'nasdaq'
        else:
            raise ValueError(f"Unknown index provider for {index_id}")

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
            changes = self.bloomberg.get_index_changes(
                index_ticker=bloomberg_ticker,
                start_date=start_date_str,
                end_date=end_date_str
            )
            
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

class SPConstituentProvider:
    def __init__(self, bloomberg_client):
        self.bloomberg = bloomberg_client
    
    def get_current_constituents(self, index_id: str, bloomberg_ticker: str) -> Dict[str, Dict[str, Any]]:
        """Fetch current S&P index constituents using Bloomberg API"""
        constituents = {}
        
        # Get member weights from Bloomberg
        member_weights = self.bloomberg.get_index_member_weights(bloomberg_ticker)
        
        if member_weights.empty:
            return constituents
            
        # Get additional security info for each constituent
        all_tickers = member_weights['member_ticker'].unique().tolist()
        bloomberg_equity_tickers = [f"{ticker} Equity" for ticker in all_tickers]
        
        # Get sector, industry, market cap info
        fields = ["GICS_SECTOR_NAME", "GICS_INDUSTRY_NAME", "CUR_MKT_CAP"]
        security_info = self.bloomberg.get_security_info(bloomberg_equity_tickers, fields)
        
        # Create a dictionary mapping tickers to their info
        info_dict = {}
        for _, row in security_info.iterrows():
            ticker = row['ticker'].replace(" Equity", "")
            info_dict[ticker] = {
                'sector': row.get('GICS_SECTOR_NAME', ''),
                'industry': row.get('GICS_INDUSTRY_NAME', ''),
                'market_cap': row.get('CUR_MKT_CAP', 0.0)
            }
        
        # Build the final constituent dict
        for _, row in member_weights.iterrows():
            ticker = row['member_ticker']
            weight = row['weight'] 
            
            # Get additional info if available
            additional_info = info_dict.get(ticker, {})
            
            constituents[ticker] = {
                'company_name': row.get('member_name', ''),
                'weight': weight,
                'sector': additional_info.get('sector', ''),
                'industry': additional_info.get('industry', ''),
                'market_cap': additional_info.get('market_cap', 0.0),
                'last_updated': pd.Timestamp.now().strftime('%Y-%m-%d')
            }
            
        return constituents

class RussellConstituentProvider:
    def __init__(self, bloomberg_client):
        self.bloomberg = bloomberg_client
    
    def get_current_constituents(self, index_id: str, bloomberg_ticker: str) -> Dict[str, Dict[str, Any]]:
        """Fetch current Russell index constituents using Bloomberg API"""
        # Implementation similar to SPConstituentProvider but with any Russell-specific logic
        constituents = {}
        
        # Get member weights from Bloomberg
        member_weights = self.bloomberg.get_index_member_weights(bloomberg_ticker)
        
        if member_weights.empty:
            return constituents
            
        # Get additional security info for each constituent
        all_tickers = member_weights['member_ticker'].unique().tolist()
        bloomberg_equity_tickers = [f"{ticker} Equity" for ticker in all_tickers]
        
        # Get sector, industry, market cap info
        fields = ["GICS_SECTOR_NAME", "GICS_INDUSTRY_NAME", "CUR_MKT_CAP"]
        security_info = self.bloomberg.get_security_info(bloomberg_equity_tickers, fields)
        
        # Create a dictionary mapping tickers to their info
        info_dict = {}
        for _, row in security_info.iterrows():
            ticker = row['ticker'].replace(" Equity", "")
            info_dict[ticker] = {
                'sector': row.get('GICS_SECTOR_NAME', ''),
                'industry': row.get('GICS_INDUSTRY_NAME', ''),
                'market_cap': row.get('CUR_MKT_CAP', 0.0)
            }
        
        # Build the final constituent dict
        for _, row in member_weights.iterrows():
            ticker = row['member_ticker']
            weight = row['weight']
            
            # Get additional info if available
            additional_info = info_dict.get(ticker, {})
            
            constituents[ticker] = {
                'company_name': row.get('member_name', ''),
                'weight': weight,
                'sector': additional_info.get('sector', ''),
                'industry': additional_info.get('industry', ''),
                'market_cap': additional_info.get('market_cap', 0.0),
                'last_updated': pd.Timestamp.now().strftime('%Y-%m-%d')
            }
            
        return constituents

class NasdaqConstituentProvider:
    def __init__(self, bloomberg_client):
        self.bloomberg = bloomberg_client
    
    def get_current_constituents(self, index_id: str, bloomberg_ticker: str) -> Dict[str, Dict[str, Any]]:
        """Fetch current Nasdaq index constituents using Bloomberg API"""
        # Implementation similar to others but with any Nasdaq-specific logic
        constituents = {}
        
        # Get member weights from Bloomberg
        member_weights = self.bloomberg.get_index_member_weights(bloomberg_ticker)
        
        if member_weights.empty:
            return constituents
            
        # Get additional security info for each constituent
        all_tickers = member_weights['member_ticker'].unique().tolist()
        bloomberg_equity_tickers = [f"{ticker} Equity" for ticker in all_tickers]
        
        # Get sector, industry, market cap info
        fields = ["GICS_SECTOR_NAME", "GICS_INDUSTRY_NAME", "CUR_MKT_CAP"]
        security_info = self.bloomberg.get_security_info(bloomberg_equity_tickers, fields)
        
        # Create a dictionary mapping tickers to their info
        info_dict = {}
        for _, row in security_info.iterrows():
            ticker = row['ticker'].replace(" Equity", "")
            info_dict[ticker] = {
                'sector': row.get('GICS_SECTOR_NAME', ''),
                'industry': row.get('GICS_INDUSTRY_NAME', ''),
                'market_cap': row.get('CUR_MKT_CAP', 0.0)
            }
        
        # Build the final constituent dict
        for _, row in member_weights.iterrows():
            ticker = row['member_ticker']
            weight = row['weight']
            
            # Get additional info if available
            additional_info = info_dict.get(ticker, {})
            
            constituents[ticker] = {
                'company_name': row.get('member_name', ''),
                'weight': weight,
                'sector': additional_info.get('sector', ''),
                'industry': additional_info.get('industry', ''),
                'market_cap': additional_info.get('market_cap', 0.0),
                'last_updated': pd.Timestamp.now().strftime('%Y-%m-%d')
            }
            
        return constituents 