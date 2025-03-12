import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime, timedelta
from src.data.bloomberg_client import BloombergClient

class PriceDataImporter:
    def __init__(self, database, bloomberg_client: BloombergClient):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        
    def fetch_daily_ohlcv(self, tickers: List[str], 
                          start_date: str, 
                          end_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Fetch daily OHLCV data for a list of tickers from Bloomberg
        
        Args:
            tickers: List of ticker symbols
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD (defaults to today)
            
        Returns:
            Dictionary mapping tickers to DataFrames with price data
        """
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        # Convert tickers to Bloomberg format (append " Equity" to each)
        bloomberg_tickers = [f"{ticker} Equity" for ticker in tickers]
        
        # Define fields to request
        fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "VOLUME", "TOT_RETURN_INDEX_GROSS_DVDS"]
        
        # Get data from Bloomberg
        bloomberg_data = self.bloomberg.get_historical_data(
            bloomberg_tickers, fields, start_date, end_date)
        
        if not bloomberg_data:
            self.logger.error("Failed to retrieve price data from Bloomberg")
            return {}
        
        # Process and rename the Bloomberg data to match our schema
        result = {}
        for bb_ticker, data in bloomberg_data.items():
            ticker = bb_ticker.replace(" Equity", "")
            
            if data.empty:
                self.logger.warning(f"No data retrieved for {ticker}")
                continue
                
            # Rename columns to match our schema
            renamed_data = data.rename(columns={
                "PX_OPEN": "open",
                "PX_HIGH": "high",
                "PX_LOW": "low",
                "PX_LAST": "close",
                "VOLUME": "volume"
            })
            
                
            # Store in result dictionary
            result[ticker] = renamed_data
            
            # Also store in database
            self.db.add_price_data(ticker, renamed_data)
            
        return result
    
    def update_all_constituent_prices(self, lookback_days: int = 7):
        """
        Update prices for all index constituents in the database
        
        Args:
            lookback_days: Number of days to look back (default: 7)
        """
        try:
            # Get all current index IDs
            query = "SELECT DISTINCT index_id FROM index_metadata"
            index_ids = pd.read_sql_query(query, self.db.conn)["index_id"].tolist()
            
            if not index_ids:
                self.logger.warning("No indices found in database")
                return
            
            # Get all unique tickers across all indices
            all_tickers = set()
            for index_id in index_ids:
                constituents = self.db.get_current_constituents(index_id)
                if not constituents.empty:
                    all_tickers.update(constituents["ticker"].tolist())
            
            if not all_tickers:
                self.logger.warning("No constituents found in database")
                return
                
            # Calculate date range
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            # Fetch and store price data
            self.fetch_daily_ohlcv(list(all_tickers), start_date, end_date)
            
            self.logger.info(f"Updated prices for {len(all_tickers)} constituents")
            
        except Exception as e:
            self.logger.error(f"Error updating constituent prices: {e}")
    
    def update_index_prices(self, index_ids: List[str], lookback_days: int = 30):
        """
        Update prices for specified indices
        
        Args:
            index_ids: List of index IDs to update
            lookback_days: Number of days to look back (default: 30)
        """
        try:
            # Get Bloomberg tickers for the indices
            query = "SELECT index_id, bloomberg_ticker FROM index_metadata WHERE index_id IN ({})".format(
                ",".join(["?"] * len(index_ids)))
            indices = pd.read_sql_query(query, self.db.conn, params=index_ids)
            
            if indices.empty:
                self.logger.warning(f"No indices found for IDs: {index_ids}")
                return
                
            # Calculate date range
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            for _, row in indices.iterrows():
                index_id = row["index_id"]
                bb_ticker = row["bloomberg_ticker"]
                
                # Define fields to request including daily return
                fields = ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "VOLUME", "DAY_TO_DAY_TOTAL_RETURN_GROSS_DVDS"]
                
                # Get data from Bloomberg
                bb_data = self.bloomberg.get_historical_data(
                    [bb_ticker], fields, start_date, end_date)
                
                if not bb_data or bb_ticker not in bb_data:
                    self.logger.warning(f"No data retrieved for index: {index_id} ({bb_ticker})")
                    continue
                    
                # Rename columns to match our schema
                data = bb_data[bb_ticker].rename(columns={
                    "PX_OPEN": "open",
                    "PX_HIGH": "high",
                    "PX_LOW": "low",
                    "PX_LAST": "close",
                    "VOLUME": "volume",
                    "DAY_TO_DAY_TOTAL_RETURN_GROSS_DVDS": "return"
                })
                
                # Store in database
                self.db.add_price_data(index_id, data)
                
                self.logger.info(f"Updated prices for index: {index_id}")
                
        except Exception as e:
            self.logger.error(f"Error updating index prices: {e}")
    
    def update_constituent_prices(self, ticker: str, lookback_days: int = 365):
        """
        Update price data for a specific constituent
        
        Args:
            ticker: Stock ticker symbol
            lookback_days: Number of days to fetch price history for
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Updating price data for {ticker}")
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        try:
            # Get price data from Bloomberg
            fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_VOLUME']
            equity_ticker = f"{ticker} Equity"  # Convert to Bloomberg equity format
            
            price_data = self.bloomberg.get_historical_data(
                securities=[equity_ticker],
                fields=fields,
                start_date=start_date_str,
                end_date=end_date_str
            )
            
            if not price_data or equity_ticker not in price_data:
                self.logger.warning(f"No price data returned for {ticker}")
                return False
                
            # Get the data frame for this ticker
            df = price_data[equity_ticker].copy()
            
            # Rename columns to match database schema
            column_map = {
                'PX_OPEN': 'open',
                'PX_HIGH': 'high',
                'PX_LOW': 'low',
                'PX_LAST': 'close',
                'PX_VOLUME': 'volume'
            }
            
            df.rename(columns=column_map, inplace=True)
            
            
            # Store in database
            return self.db.add_price_data(ticker, df)
            
        except Exception as e:
            self.logger.error(f"Error updating price data for {ticker}: {e}")
            return False 