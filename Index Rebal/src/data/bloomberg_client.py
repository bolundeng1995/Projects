#!/usr/bin/env python
"""
Bloomberg Client Module

A client for interacting with the Bloomberg API to fetch price data.
Handles connection, data retrieval, and proper error handling.
Constituents data will be fetched from local files instead of Bloomberg.
"""

import logging
import pandas as pd
from datetime import datetime
import sqlite3
import hashlib
import json
import pickle

logger = logging.getLogger(__name__)

class BloombergClient:
    """
    A client for the Bloomberg API that handles price data retrieval and caching.
    """
    
    def __init__(self, use_cached_data=False, cache_db_path='index_data.db'):
        """
        Initialize the Bloomberg client
        
        Args:
            use_cached_data: Whether to use cache for Bloomberg requests
            cache_db_path: Path to the cache database
        """
        self.use_cached_data = use_cached_data
        self.cache_db_path = cache_db_path
        self.conn = None
        
        logger.info("Initializing Bloomberg client")
        
        # Initialize connection to Bloomberg here
        try:
            # Import the Bloomberg API
            import pdblp
            self.bbg = pdblp.BCon(timeout=5000)
            self.bbg.start()
            logger.info("Bloomberg API connection successful")
            
        except ImportError:
            logger.error("Bloomberg API library (pdblp) not available. Please install it with: pip install pdblp")
            raise
        except Exception as e:
            logger.error(f"Error connecting to Bloomberg API: {e}")
            raise
        
        # For caching
        if self.use_cached_data:
            try:
                self.conn = sqlite3.connect(self.cache_db_path)
                self.cursor = self.conn.cursor()
                self._check_cache_table()
                logger.info(f"Bloomberg cache enabled: {self.cache_db_path}")
            except Exception as e:
                logger.error(f"Error initializing cache: {e}")
                logger.warning("Continuing without cache")
                self.use_cached_data = False
        
    def _check_cache_table(self):
        """Ensure the cache table exists"""
        if not self.conn:
            return
            
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS cached_responses (
            request_hash TEXT PRIMARY KEY,
            request_type TEXT NOT NULL,
            request_params TEXT NOT NULL,
            response_data BLOB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        self.conn.commit()
        
    def _cache_key(self, request_type, **params):
        """Generate a cache key for a request"""
        if not self.use_cached_data:
            return None
            
        # Create a normalized JSON string of params for hashing
        param_str = json.dumps(params, sort_keys=True)
        key = f"{request_type}:{param_str}"
        return hashlib.md5(key.encode()).hexdigest()
        
    def _get_from_cache(self, request_type, **params):
        """Get data from cache if available"""
        if not self.use_cached_data or not self.conn:
            return None
            
        cache_key = self._cache_key(request_type, **params)
        
        try:
            query = "SELECT response_data FROM cached_responses WHERE request_hash = ?"
            result = self.cursor.execute(query, (cache_key,)).fetchone()
            
            if result:
                logger.info(f"Cache hit for {request_type}")
                return pickle.loads(result[0])
            else:
                logger.info(f"Cache miss for {request_type}")
                return None
                
        except Exception as e:
            logger.warning(f"Error retrieving from cache: {e}")
            return None
            
    def _save_to_cache(self, request_type, data, **params):
        """Save data to cache"""
        if not self.use_cached_data or not self.conn:
            return
            
        cache_key = self._cache_key(request_type, **params)
        
        try:
            # Serialize the data
            pickled_data = pickle.dumps(data)
            
            # Store in the database
            query = '''
            INSERT OR REPLACE INTO cached_responses 
            (request_hash, request_type, request_params, response_data, timestamp)
            VALUES (?, ?, ?, ?, ?)
            '''
            
            self.cursor.execute(query, (
                cache_key,
                request_type,
                json.dumps(params, sort_keys=True),
                pickled_data,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            self.conn.commit()
            logger.info(f"Saved {request_type} data to cache")
            
        except Exception as e:
            logger.warning(f"Error saving to cache: {e}")
    
    def get_historical_data(self, tickers, fields, start_date, end_date):
        """
        Get historical price data for the specified tickers and fields.
        
        Args:
            tickers: List of tickers to get data for
            fields: List of fields to retrieve (OPEN, HIGH, LOW, PX_LAST, etc.)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with historical data in the format expected by import_bloomberg_prices.py
            Columns: ticker, date, OPEN, HIGH, LOW, PX_LAST, VOLUME, DAY_TO_DAY_TOT_RETURN_GROSS_DVDS
        """
        logger.info(f"Getting historical price data for {len(tickers)} tickers from {start_date} to {end_date}")
        
        # Check cache first
        cache_params = {
            'tickers': tickers,
            'fields': fields,
            'start_date': start_date,
            'end_date': end_date
        }
        
        cached_data = self._get_from_cache('historical_data', **cache_params)
        if cached_data is not None:
            return cached_data
        
        # Get data from the Bloomberg API
        try:
            # Convert fields list to comma-separated string if necessary
            if isinstance(fields, list):
                fields_str = ','.join(fields)
            else:
                fields_str = fields
                fields = fields_str.split(',')
            
            # Request data from Bloomberg
            data = self.bbg.bdh(
                tickers=tickers,
                flds=fields,
                start_date=start_date,
                end_date=end_date
            )
            
            if data.empty:
                logger.warning(f"No data returned from Bloomberg for price request")
                return pd.DataFrame()
            
            # Process the data to ensure consistent format
            processed_data = self._process_bloomberg_data(data, tickers)
            
            # Save to cache
            self._save_to_cache('historical_data', processed_data, **cache_params)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error fetching historical price data from Bloomberg: {e}")
            raise
    
    def _process_bloomberg_data(self, data, tickers):
        """
        Process Bloomberg data to ensure consistent format for database storage.
        
        Args:
            data: Raw data from Bloomberg API
            tickers: List of tickers requested
            
        Returns:
            Processed DataFrame with consistent columns and format
        """
        # Handle different response formats from Bloomberg API
        if isinstance(data.index, pd.MultiIndex):
            # Multi-index format (multiple tickers or fields)
            processed = data.reset_index()
        else:
            # Single index format (usually for single ticker)
            processed = data.reset_index()
            if len(tickers) == 1 and 'ticker' not in processed.columns:
                processed['ticker'] = tickers[0]
        
        # Ensure date column exists and has proper format
        if 'date' not in processed.columns:
            date_col = next((col for col in processed.columns if 'date' in col.lower()), None)
            if date_col:
                processed.rename(columns={date_col: 'date'}, inplace=True)
            else:
                logger.error("No date column found in Bloomberg data")
                return pd.DataFrame()
        
        # Convert date to string format YYYY-MM-DD if it's not already
        if 'date' in processed.columns and not isinstance(processed['date'].iloc[0], str):
            processed['date'] = processed['date'].dt.strftime('%Y-%m-%d')
        
        # Make sure we have a ticker column
        if 'ticker' not in processed.columns and 'security' in processed.columns:
            processed.rename(columns={'security': 'ticker'}, inplace=True)
        elif 'ticker' not in processed.columns:
            logger.error("No ticker column found in Bloomberg data")
            return pd.DataFrame()
        
        # Check for required price fields
        required_fields = ['OPEN', 'HIGH', 'LOW', 'PX_LAST', 'VOLUME']
        missing_fields = [field for field in required_fields if field not in processed.columns]
        
        if missing_fields:
            logger.error(f"Missing required price fields in Bloomberg data: {', '.join(missing_fields)}")
            # We could return an empty DataFrame here, but let's let the caller decide what to do
        
        logger.debug(f"Processed Bloomberg data format: {processed.columns.tolist()}")
        return processed
    
    def close(self):
        """Close the Bloomberg connection and any database connections"""
        logger.info("Closing Bloomberg client connections")
        
        # Close Bloomberg connection
        if hasattr(self, 'bbg'):
            self.bbg.stop()
        
        # Close cache database connection
        if self.conn:
            self.conn.close()
            self.conn = None 

    def execute_eqs_query(self, query):
        """
        Execute an Equity Screen (EQS) query on Bloomberg using pdblp
        
        Args:
            query: EQS query string (e.g. "CNTRY_OF_DOMICILE='US' AND MARKET_CAP>1000000000")
            
        Returns:
            List of Bloomberg securities that match the query
        """
        logger.info(f"Executing EQS query: {query}")
        
        # Check cache first
        cache_params = {'query': query}
        cached_data = self._get_from_cache('eqs_query', **cache_params)
        if cached_data is not None:
            return cached_data
        
        try:
            # Format the query for Bloomberg EQS
            eqs_ticker = f"EQS|{query}"  # Bloomberg EQS format
            
            # Execute the screen using pdblp's ref() function
            screen_result = self.bbg.ref(
                tickers=[eqs_ticker],
                flds=["SECURITIES"]  # This field returns the matching securities
            )
            
            tickers = []
            if not screen_result.empty and "SECURITIES" in screen_result.columns:
                securities_data = screen_result["SECURITIES"][0]
                
                # Process the returned securities data
                if isinstance(securities_data, str):
                    # Sometimes returned as a single string with delimiters
                    tickers = securities_data.split()
                elif isinstance(securities_data, list):
                    # Sometimes returned as a list
                    tickers = securities_data
            
            logger.info(f"EQS query returned {len(tickers)} results")
            
            # Save to cache
            self._save_to_cache('eqs_query', tickers, **cache_params)
            
            return tickers
        
        except Exception as e:
            logger.error(f"Error executing EQS query: {e}")
            # Return empty list on error
            return [] 