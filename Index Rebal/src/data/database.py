import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

class IndexDatabase:
    """
    SQLite database for storing index constituent data and related information
    """
    
    def __init__(self, db_path: str = 'index_data.db'):
        """Initialize the database connection and create tables if they don't exist"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Enable foreign keys
        self.cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create tables if they don't exist
        self._create_tables()
        
    def _create_tables(self):
        """Create the necessary tables if they don't exist"""
        # Index metadata table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_metadata (
            index_id TEXT PRIMARY KEY,
            index_name TEXT NOT NULL,
            bloomberg_ticker TEXT,
            rebalance_frequency TEXT,
            description TEXT
        )
        ''')
        
        # Index constituents table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_id TEXT,
            ticker TEXT,
            name TEXT,
            weight REAL,
            sector TEXT,
            as_of_date TEXT,
            PRIMARY KEY (index_id, ticker, as_of_date),
            FOREIGN KEY (index_id) REFERENCES index_metadata(index_id)
        )
        ''')
        
        # Price data table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_data (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            return REAL,
            PRIMARY KEY (ticker, date)
        )
        ''')
        
        # Create indices for better query performance
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_ticker ON price_data(ticker)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_date ON price_data(date)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_constituents_index_id ON index_constituents(index_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_constituents_ticker ON index_constituents(ticker)')
        
        self.conn.commit()
        
    def add_index(self, index_id: str, index_name: str, bloomberg_ticker: str, 
                  rebalance_frequency: str = None, description: str = None) -> bool:
        """
        Add or update an index in the database
        
        Args:
            index_id: Unique identifier for the index
            index_name: Human-readable name
            bloomberg_ticker: Bloomberg ticker symbol (e.g., "SPX Index")
            rebalance_frequency: How often the index rebalances
            description: Additional information about the index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO index_metadata 
            (index_id, index_name, bloomberg_ticker, rebalance_frequency, description)
            VALUES (?, ?, ?, ?, ?)
            ''', (index_id, index_name, bloomberg_ticker, rebalance_frequency, description))
            
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error adding index: {e}")
            return False
            
    def add_constituent(self, index_id: str, ticker: str, bloomberg_ticker: str, data: Dict) -> bool:
        """
        Add or update a constituent for an index
        
        Args:
            index_id: Index identifier
            ticker: Constituent ticker symbol
            bloomberg_ticker: Bloomberg ticker for the constituent
            data: Dictionary with constituent data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract data fields
            company_name = data.get('company_name', '')
            weight = data.get('weight', 0.0)
            sector = data.get('sector', '')
            as_of_date = data.get('as_of_date', datetime.now().strftime('%Y-%m-%d'))
            import_date = datetime.now().strftime('%Y-%m-%d')
            
            # Insert or replace in current_constituents table (latest data)
            self.cursor.execute('''
            INSERT OR REPLACE INTO current_constituents
            (index_id, ticker, bloomberg_ticker, company_name, weight, sector, as_of_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, ticker, bloomberg_ticker, company_name, weight, sector, as_of_date))
            
            # Also record in historical constituents table with reference_date
            self.cursor.execute('''
            INSERT OR REPLACE INTO constituents
            (index_id, ticker, bloomberg_ticker, company_name, weight, sector, reference_date, import_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, ticker, bloomberg_ticker, company_name, weight, sector, as_of_date, import_date))
            
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding constituent: {e}")
            return False
        
    def remove_constituent(self, index_id: str, ticker: str):
        """Remove a constituent from an index"""
        try:
            self.conn.execute('''
                DELETE FROM current_constituents
                WHERE index_id = ? AND ticker = ?
            ''', (index_id, ticker))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error removing constituent {ticker} from {index_id}: {e}")
            self.conn.rollback()
            return False
    
    def get_current_constituents(self, index_id: str) -> pd.DataFrame:
        """Get current constituents for a specific index"""
        try:
            query = '''
                SELECT * FROM current_constituents
                WHERE index_id = ?
                ORDER BY weight DESC
            '''
            return pd.read_sql_query(query, self.conn, params=(index_id,))
        except Exception as e:
            self.logger.error(f"Error getting constituents for {index_id}: {e}")
            return pd.DataFrame()
        
    def get_historical_changes(self, index_id: str, 
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None) -> pd.DataFrame:
        """Get historical constituent changes for a specific index"""
        try:
            query = '''
                SELECT * FROM constituent_changes
                WHERE index_id = ?
            '''
            params = [index_id]
            
            if start_date:
                query += " AND implementation_date >= ?"
                params.append(start_date)
                
            if end_date:
                query += " AND implementation_date <= ?"
                params.append(end_date)
                
            query += " ORDER BY implementation_date DESC"
            
            return pd.read_sql_query(query, self.conn, params=tuple(params))
        except Exception as e:
            self.logger.error(f"Error getting historical changes for {index_id}: {e}")
            return pd.DataFrame()
    
    def add_price_data(self, ticker: str, price_data: pd.DataFrame):
        """Add price data for a ticker"""
        if price_data.empty:
            return False
            
        try:
            # Ensure the DataFrame has the required columns
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            
            df = price_data.copy()
            
            # Convert column names to lowercase
            df.columns = [col.lower() for col in df.columns]
            
            # Check all required columns are present
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                self.logger.warning(f"Missing required price columns for {ticker}: {missing_columns}")
                return False
                
            # Make sure the index is the date
            if not isinstance(df.index, pd.DatetimeIndex):
                # If there's a 'date' column, set it as index
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                else:
                    self.logger.warning(f"No date index or column for {ticker}")
                    return False
                
            # Reset index to get date as a column
            df = df.reset_index()
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Add ticker column if it doesn't exist
            if 'ticker' not in df.columns:
                df['ticker'] = ticker
            
            # Build columns list for SQL insert
            columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'return']
            
            df_subset = df[columns]
            
            # Convert to list of tuples for efficient sqlite insert
            data_tuples = [tuple(x) for x in df_subset.to_numpy()]
            
            # Build SQL statement dynamically based on columns
            sql = f'''
                INSERT OR REPLACE INTO price_data
                ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            '''
            
            self.conn.executemany(sql, data_tuples)
            
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding price data for {ticker}: {e}")
            self.conn.rollback()
            return False
    
    def get_price_data(self, ticker: str, 
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get price data for a specific ticker
        
        Args:
            ticker: Ticker symbol
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            limit: Optional limit of rows to return
            
        Returns:
            DataFrame containing price data
        """
        try:
            query = "SELECT * FROM price_data WHERE ticker = ?"
            params = [ticker]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
                
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
                
            query += " ORDER BY date DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            return pd.read_sql_query(query, self.conn, params=tuple(params))
        except Exception as e:
            self.logger.error(f"Error getting price data for {ticker}: {e}")
            return pd.DataFrame()
    
    def add_rebalance_event(self, index_id: str, event_type: str,
                          announcement_date: str, implementation_date: str,
                          description: str = "", status: str = "scheduled", notes: str = ""):
        """Add a rebalance event to the calendar"""
        try:
            self.conn.execute('''
                INSERT INTO rebalance_events
                (index_id, event_type, announcement_date, implementation_date, description, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, event_type, announcement_date, implementation_date, description, status, notes))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding rebalance event for {index_id}: {e}")
            self.conn.rollback()
            return False
    
    def get_upcoming_rebalance_events(self, days_ahead: int = 30) -> pd.DataFrame:
        """Get upcoming rebalance events"""
        try:
            today = datetime.now().date()
            end_date = (today + pd.Timedelta(days=days_ahead)).strftime('%Y-%m-%d')
            today_str = today.strftime('%Y-%m-%d')
            
            query = '''
                SELECT r.*, i.index_name
                FROM rebalance_events r
                JOIN index_metadata i ON r.index_id = i.index_id
                WHERE (r.announcement_date BETWEEN ? AND ?) OR (r.implementation_date BETWEEN ? AND ?)
                ORDER BY 
                    CASE 
                        WHEN r.announcement_date >= ? THEN r.announcement_date 
                        ELSE r.implementation_date 
                    END
            '''
            
            params = (today_str, end_date, today_str, end_date, today_str)
            
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            self.logger.error(f"Error getting upcoming rebalance events: {e}")
            return pd.DataFrame()
    
    def add_constituent_change(self, index_id: str, ticker: str, bloomberg_ticker: str,
                             event_type: str, announcement_date: str, implementation_date: str,
                             old_weight: float = 0.0, new_weight: float = 0.0,
                             reason: str = "", notes: str = ""):
        """Record a constituent change event"""
        try:
            self.conn.execute('''
                INSERT INTO constituent_changes
                (index_id, ticker, bloomberg_ticker, event_type, announcement_date, implementation_date,
                old_weight, new_weight, reason, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, ticker, bloomberg_ticker, event_type, announcement_date, implementation_date,
                old_weight, new_weight, reason, notes))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding constituent change for {ticker} in {index_id}: {e}")
            self.conn.rollback()
            return False
    
    def add_corporate_action(self, ticker: str, bloomberg_ticker: str, action_type: str,
                           ex_date: str, effective_date: str, description: str, details: str = ""):
        """Record a corporate action"""
        try:
            self.conn.execute('''
                INSERT INTO corporate_actions
                (ticker, bloomberg_ticker, action_type, ex_date, effective_date, description, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (ticker, bloomberg_ticker, action_type, ex_date, effective_date, description, details))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding corporate action for {ticker}: {e}")
            self.conn.rollback()
            return False
    
    def get_corporate_actions(self, ticker: Optional[str] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> pd.DataFrame:
        """Get corporate actions within a date range"""
        try:
            query = '''
                SELECT * FROM corporate_actions
                WHERE 1=1
            '''
            params = []
            
            if ticker:
                query += " AND ticker = ?"
                params.append(ticker)
                
            if start_date:
                query += " AND effective_date >= ?"
                params.append(start_date)
                
            if end_date:
                query += " AND effective_date <= ?"
                params.append(end_date)
                
            query += " ORDER BY effective_date DESC"
            
            return pd.read_sql_query(query, self.conn, params=tuple(params))
        except Exception as e:
            self.logger.error(f"Error getting corporate actions: {e}")
            return pd.DataFrame()
    
    def get_all_indices(self) -> pd.DataFrame:
        """
        Get all indices stored in the database
        
        Returns:
            DataFrame containing all indices
        """
        try:
            query = "SELECT * FROM index_metadata"
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error retrieving indices: {e}")
            return pd.DataFrame()
    
    def delete_index(self, index_id: str) -> bool:
        """
        Delete an index and all its associated data
        
        Args:
            index_id: Internal index identifier to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Start a transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Delete from index_metadata
            self.conn.execute(
                "DELETE FROM index_metadata WHERE index_id = ?", 
                (index_id,)
            )
            
            # Delete associated data
            self.conn.execute(
                "DELETE FROM current_constituents WHERE index_id = ?", 
                (index_id,)
            )
            self.conn.execute(
                "DELETE FROM constituent_changes WHERE index_id = ?", 
                (index_id,)
            )
            self.conn.execute(
                "DELETE FROM rebalance_events WHERE index_id = ?", 
                (index_id,)
            )
            
            # Commit transaction
            self.conn.execute("COMMIT")
            self.logger.info(f"Deleted index {index_id} and associated data")
            return True
            
        except Exception as e:
            # Rollback in case of error
            self.conn.execute("ROLLBACK")
            self.logger.error(f"Error deleting index {index_id}: {e}")
            return False
    
    def get_historical_constituents(self, index_id: str, as_of_date: str) -> pd.DataFrame:
        """
        Get index constituents as they were on a specific date
        
        Args:
            index_id: Index identifier
            as_of_date: Date in format YYYY-MM-DD to get constituents for
            
        Returns:
            DataFrame containing constituents as of the specified date
        """
        try:
            # Find the closest reference_date before or equal to the requested date
            date_query = '''
                SELECT MAX(reference_date) as closest_date
                FROM constituents
                WHERE index_id = ? AND reference_date <= ?
            '''
            
            closest_date = pd.read_sql_query(date_query, self.conn, 
                                         params=(index_id, as_of_date)).iloc[0]['closest_date']
            
            if not closest_date:
                self.logger.warning(f"No historical constituent data for {index_id} before {as_of_date}")
                return pd.DataFrame()
            
            # Get all constituents from that reference date
            query = '''
                SELECT * FROM constituents
                WHERE index_id = ? AND reference_date = ?
                ORDER BY weight DESC
            '''
            
            result = pd.read_sql_query(query, self.conn, params=(index_id, closest_date))
            
            if result.empty:
                self.logger.warning(f"No historical constituents found for {index_id} on {closest_date}")
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error getting historical constituents for {index_id} as of {as_of_date}: {e}")
            return pd.DataFrame()
    
    def add_index_constituent(self, data: Dict) -> bool:
        """
        Add or update an index constituent
        
        Args:
            data: Dictionary with constituent data including:
                - index_id: Index identifier
                - symbol: Constituent symbol
                - index_shares: Number of shares in the index
                - index_weight: Weight in the index
                - closing_price: Closing price
                - market_value: Market value
                - sedol: SEDOL identifier
                - cusip: CUSIP identifier
                - isin: ISIN identifier
                - reference_date: Date this data is valid for
                
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract required fields
            index_id = data.get('index_id')
            symbol = data.get('symbol')
            reference_date = data.get('reference_date')
            
            if not all([index_id, symbol, reference_date]):
                self.logger.error("Missing required fields for constituent: index_id, symbol, reference_date")
                return False
            
            # Extract optional fields with defaults
            index_shares = data.get('index_shares', 0)
            index_weight = data.get('index_weight', 0)
            closing_price = data.get('closing_price', 0)
            market_value = data.get('market_value', 0)
            sedol = data.get('sedol', '')
            cusip = data.get('cusip', '')
            isin = data.get('isin', '')
            import_date = datetime.now().strftime('%Y-%m-%d')
            
            # Insert or replace in index_constituents table
            self.cursor.execute('''
            INSERT OR REPLACE INTO index_constituents
            (index_id, ticker, name, weight, sector, as_of_date, sedol, cusip, isin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, symbol, symbol, index_weight, '', as_of_date, sedol, cusip, isin))
            
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding index constituent: {e}")
            self.conn.rollback()
            return False

    def get_index_constituents(self, index_id, as_of_date=None):
        """
        Get constituents for an index as of a specific date
        
        Args:
            index_id: ID of the index
            as_of_date: Date to get constituents for (default: latest available)
            
        Returns:
            DataFrame with constituent data
        """
        query = """
        SELECT ticker, name, weight, sector, as_of_date
        FROM index_constituents
        WHERE index_id = ?
        """
        
        params = [index_id]
        
        if as_of_date:
            query += " AND as_of_date = ?"
            params.append(as_of_date)
        else:
            # Get the latest date for each ticker
            query += " AND as_of_date = (SELECT MAX(as_of_date) FROM index_constituents WHERE index_id = ?)"
            params.append(index_id)
        
        return pd.read_sql_query(query, self.conn, params=params)

    def get_index_bloomberg_ticker(self, index_id):
        """
        Get the Bloomberg ticker for an index
        
        Args:
            index_id: ID of the index
            
        Returns:
            Bloomberg ticker as string or None if not found
        """
        query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
        result = self.cursor.execute(query, (index_id,)).fetchone()
        
        if result:
            return result[0]
        return None 