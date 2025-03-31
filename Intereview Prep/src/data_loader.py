import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

class DataLoader:
    """
    Handles data acquisition and preprocessing for S&P 500 stocks.
    """
    
    def __init__(self, config):
        """
        Initialize the data loader with configuration parameters.
        
        Parameters:
        -----------
        config : dict
            Configuration dictionary containing parameters like start_date, end_date, etc.
        """
        self.config = config
        self.start_date = config.get('start_date', '2018-01-01')
        self.end_date = config.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        self.data_dir = config.get('data_dir', 'data/')
        
    def get_sp500_tickers(self):
        """
        Get the current list of S&P 500 tickers.
        
        Returns:
        --------
        list
            List of S&P 500 ticker symbols.
        """
        # For a complete implementation, use a proper source for S&P 500 constituents
        # This is a simplified version that can be enhanced
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(sp500_url)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        return tickers
    
    def load_price_data(self, tickers=None, force_download=False):
        """
        Load price data for the specified tickers.
        
        Parameters:
        -----------
        tickers : list, optional
            List of tickers to load data for. If None, uses S&P 500 tickers.
        force_download : bool, optional
            If True, forces re-download of data even if cached version exists.
            
        Returns:
        --------
        pandas.DataFrame
            Multi-index DataFrame with ticker and date as indices and OHLCV data.
        """
        if tickers is None:
            tickers = self.get_sp500_tickers()
        
        cache_file = f"{self.data_dir}/raw/price_data_{self.start_date}_{self.end_date}.pkl"
        
        # Check if cached data exists
        try:
            if not force_download:
                return pd.read_pickle(cache_file)
        except FileNotFoundError:
            pass
        
        # Download data for each ticker
        all_data = []
        for ticker in tickers:
            try:
                print(f"Downloading data for {ticker}...")
                ticker_data = yf.download(ticker, start=self.start_date, end=self.end_date)
                ticker_data = ticker_data.droplevel(1, axis=1)
                ticker_data['Ticker'] = ticker
                all_data.append(ticker_data)
            except Exception as e:
                print(f"Error downloading data for {ticker}: {e}")
        
        # Combine all data into a multi-index DataFrame
        if not all_data:
            raise ValueError("No data was downloaded. Please check your internet connection and ticker list.")
        
        combined_data = pd.concat(all_data)
        combined_data = combined_data.reset_index()
        combined_data = combined_data.set_index(['Ticker', 'Date'])
        
        # Save to cache
        combined_data.to_pickle(cache_file)
        
        return combined_data
    
    def load_fundamental_data(self, tickers=None):
        """
        Load fundamental data for the specified tickers.
        
        Parameters:
        -----------
        tickers : list, optional
            List of tickers to load data for. If None, uses S&P 500 tickers.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame containing fundamental data for analysis.
        """
        if tickers is None:
            tickers = self.get_sp500_tickers()
            
        # In a production environment, you would connect to a data provider API
        # or use a library like yfinance to fetch fundamental data
        # This is a placeholder implementation
        
        # Create a dictionary to store fundamental data
        fundamental_data = {}
        
        for ticker in tickers[:10]:  # Limiting to 10 tickers for demonstration
            try:
                # Get ticker information
                ticker_obj = yf.Ticker(ticker)
                
                # Get financial data
                income_stmt = ticker_obj.income_stmt
                balance_sheet = ticker_obj.balance_sheet
                cash_flow = ticker_obj.cashflow
                
                # Extract key metrics
                if not income_stmt.empty and not balance_sheet.empty:
                    # Calculate fundamental ratios
                    try:
                        net_income = income_stmt.loc['Net Income']
                        total_assets = balance_sheet.loc['Total Assets']
                        total_equity = balance_sheet.loc['Total Stockholder Equity']
                        
                        # Calculate ROA and ROE
                        roa = net_income / total_assets
                        roe = net_income / total_equity
                        
                        fundamental_data[ticker] = {
                            'ROA': roa,
                            'ROE': roe,
                            # Add more fundamental metrics as needed
                        }
                    except (KeyError, TypeError) as e:
                        print(f"Error processing fundamental data for {ticker}: {e}")
            except Exception as e:
                print(f"Error fetching fundamental data for {ticker}: {e}")
        
        # Convert to DataFrame
        fundamental_df = pd.DataFrame.from_dict(fundamental_data, orient='index')
        
        return fundamental_df
    
    def prepare_factor_data(self):
        """
        Prepare and combine price and fundamental data for factor creation.
        
        Returns:
        --------
        tuple
            (price_data, fundamental_data) for factor calculation.
        """
        price_data = self.load_price_data()
        fundamental_data = self.load_fundamental_data(tickers=price_data.index.get_level_values('Ticker').unique())
        
        return price_data, fundamental_data 