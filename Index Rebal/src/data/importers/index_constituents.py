import pandas as pd
from typing import List, Dict, Any, Optional

class IndexConstituentImporter:
    def __init__(self, database, bloomberg_client):
        self.db = database
        self.bloomberg = bloomberg_client
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