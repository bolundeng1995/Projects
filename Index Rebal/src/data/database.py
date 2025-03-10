import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

class IndexDatabase:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.logger = logging.getLogger(__name__)
        self._initialize_tables()
    
    def _initialize_tables(self):
        """Create necessary tables if they don't exist"""
        # Tables for constituents, historical changes, corporate actions, etc.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS index_metadata (
                index_id TEXT PRIMARY KEY,
                index_name TEXT,
                bloomberg_ticker TEXT,
                rebalance_frequency TEXT,
                description TEXT,
                last_updated DATE
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS current_constituents (
                index_id TEXT,
                ticker TEXT,
                bloomberg_ticker TEXT,
                company_name TEXT,
                weight REAL,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                last_updated DATE,
                PRIMARY KEY (index_id, ticker)
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS constituent_changes (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_id TEXT,
                ticker TEXT,
                bloomberg_ticker TEXT,
                event_type TEXT,
                announcement_date DATE,
                implementation_date DATE,
                old_weight REAL,
                new_weight REAL,
                reason TEXT,
                notes TEXT
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS price_data (
                ticker TEXT,
                date DATE,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                adj_close REAL,
                PRIMARY KEY (ticker, date)
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rebalance_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_id TEXT,
                event_type TEXT,
                announcement_date DATE,
                implementation_date DATE,
                description TEXT,
                status TEXT,
                notes TEXT
            )
        ''')
        
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS corporate_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                bloomberg_ticker TEXT,
                action_type TEXT,
                ex_date DATE,
                effective_date DATE,
                description TEXT,
                details TEXT
            )
        ''')
        
    def add_index(self, index_id: str, index_name: str, bloomberg_ticker: str,
                rebalance_frequency: str, description: str = ""):
        """Add or update an index in the metadata table"""
        try:
            self.conn.execute('''
                INSERT OR REPLACE INTO index_metadata
                (index_id, index_name, bloomberg_ticker, rebalance_frequency, description, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (index_id, index_name, bloomberg_ticker, rebalance_frequency, description, datetime.now().date()))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding index {index_id}: {e}")
            self.conn.rollback()
            return False
        
    def add_constituent(self, index_id: str, ticker: str, bloomberg_ticker: str, data: Dict[str, Any]):
        """Add a constituent to an index"""
        try:
            company_name = data.get('company_name', '')
            weight = data.get('weight', 0.0)
            sector = data.get('sector', '')
            industry = data.get('industry', '')
            market_cap = data.get('market_cap', 0.0)
            last_updated = data.get('last_updated', datetime.now().date())
            
            self.conn.execute('''
                INSERT OR REPLACE INTO current_constituents
                (index_id, ticker, bloomberg_ticker, company_name, weight, sector, industry, market_cap, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (index_id, ticker, bloomberg_ticker, company_name, weight, sector, industry, market_cap, last_updated))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding constituent {ticker} to {index_id}: {e}")
            self.conn.rollback()
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
            
            # Add adj_close if not present
            if 'adj_close' not in df.columns and 'close' in df.columns:
                df['adj_close'] = df['close']
                
            # Check all required columns are present
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                self.logger.error(f"Missing required price columns for {ticker}: {missing_columns}")
                return False
                
            # Check for datetime index and handle properly
            if df.index.name == 'date' or df.index.name == 'Date':
                # Explicitly convert index to DatetimeIndex
                df.index = pd.DatetimeIndex(df.index)
                df = df.reset_index()
            elif isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                df.rename(columns={'index': 'date'}, inplace=True)
            else:
                # If index is not a DatetimeIndex, ensure there's a date column
                if 'date' not in df.columns:
                    self.logger.error(f"No date column in price data for {ticker}")
                    return False
            
            # Ensure date is properly formatted
            df['date'] = pd.to_datetime(df['date'])
                
            # Add ticker column
            df['ticker'] = ticker
            
            # Convert date to string format for SQLite storage
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            # Select only the required columns
            cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']
            df = df[cols]
            
            # Instead of using to_sql which doesn't handle conflicts well
            # Prepare the data in a format suitable for executemany
            # Convert DataFrame to list of tuples
            records = df.to_records(index=False)
            data_tuples = list(records)
            
            # Use INSERT OR REPLACE to handle existing records
            self.conn.executemany('''
                INSERT OR REPLACE INTO price_data
                (ticker, date, open, high, low, close, volume, adj_close)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error adding price data for {ticker}: {e}")
            self.conn.rollback()
            return False
    
    def get_price_data(self, ticker: str, start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> pd.DataFrame:
        """Get price data for a ticker within a date range"""
        try:
            query = '''
                SELECT date, open, high, low, close, volume, adj_close
                FROM price_data
                WHERE ticker = ?
            '''
            params = [ticker]
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
                
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
                
            query += " ORDER BY date"
            
            df = pd.read_sql_query(query, self.conn, params=tuple(params))
            
            # Convert date column to datetime and set as index
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            return df
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