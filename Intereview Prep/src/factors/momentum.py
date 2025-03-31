import pandas as pd
import numpy as np

class MomentumFactors:
    """
    Implements various momentum-based factors.
    """
    
    def __init__(self, price_data):
        """
        Initialize the momentum factors calculator.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        """
        self.price_data = price_data
        
    def calculate_price_momentum(self, lookback_months=12, skip_months=1):
        """
        Calculate price momentum over a lookback period, excluding the most recent months.
        
        Parameters:
        -----------
        lookback_months : int, optional
            Total lookback period in months (default 12).
        skip_months : int, optional
            Number of most recent months to exclude (default 1).
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with price momentum values for each ticker and date.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Convert to monthly prices for cleaner month-to-month calculations
        monthly_prices = close_prices.resample('ME').last()
        
        # Initialize a DataFrame to store momentum returns
        momentum_df = pd.DataFrame(index=close_prices.index, columns=close_prices.columns)
        
        # Calculate the momentum for each date in the original price series
        for date in close_prices.index:
            # Find the most recent month-end before this date
            month_end = pd.Timestamp(date.year, date.month, 1) - pd.Timedelta(days=1)
            
            # Skip back the required number of months
            skip_month = month_end - pd.DateOffset(months=skip_months)
            lookback_month = month_end - pd.DateOffset(months=lookback_months)
            
            # Find the closest available dates in our monthly data
            try:
                # Find nearest available dates
                skip_date = monthly_prices.index[monthly_prices.index <= skip_month][-1]
                lookback_date = monthly_prices.index[monthly_prices.index <= lookback_month][-1]
                
                # Calculate momentum returns
                if skip_date > lookback_date:  # Ensure we have enough history
                    momentum_returns = monthly_prices.loc[skip_date] / monthly_prices.loc[lookback_date] - 1
                    momentum_df.loc[date] = momentum_returns
            except (IndexError, KeyError):
                # Not enough history
                pass
        
        # Drop rows with all NaNs (dates with insufficient history)
        momentum_df = momentum_df.dropna(how='all')
        
        # Rank stocks cross-sectionally for each date
        momentum_factor = momentum_df.rank(axis=1, pct=True)
        
        return momentum_factor
    
    def calculate_relative_strength_index(self, window=14):
        """
        Calculate Relative Strength Index (RSI) momentum factor.
        
        Parameters:
        -----------
        window : int, optional
            Lookback window for RSI calculation.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with RSI values for each ticker and date.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Calculate daily returns
        daily_returns = close_prices.pct_change()
        
        # Separate upward and downward movements
        up_returns = daily_returns.copy()
        down_returns = daily_returns.copy()
        
        up_returns[up_returns < 0] = 0
        down_returns[down_returns > 0] = 0
        down_returns = down_returns.abs()
        
        # Calculate rolling averages of up and down movements
        avg_up = up_returns.rolling(window=window).mean()
        avg_down = down_returns.rolling(window=window).mean()
        
        # Calculate RSI
        rs = avg_up / avg_down
        rsi = 100 - (100 / (1 + rs))
        
        # Rank stocks cross-sectionally
        rsi_factor = rsi.rank(axis=1, pct=True)
        
        return rsi_factor
    
    def calculate_mean_reversion(self, window=20):
        """
        Calculate mean reversion factor as a negative short-term momentum.
        
        Parameters:
        -----------
        window : int, optional
            Lookback window for mean reversion calculation.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with mean reversion values for each ticker and date.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Calculate short-term returns
        short_returns = close_prices.pct_change(window)
        
        # Mean reversion is the negative of short-term momentum
        mean_reversion_factor = -short_returns.rank(axis=1, pct=True)
        
        return mean_reversion_factor
    
    def combine_momentum_factors(self, weights=None):
        """
        Combine multiple momentum factors into a single composite factor.
        
        Parameters:
        -----------
        weights : dict, optional
            Dictionary with factor names as keys and weights as values.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with combined momentum factor for each ticker and date.
        """
        if weights is None:
            weights = {
                'price_momentum': 0.5,
                'rsi': 0.3,
                'mean_reversion': 0.2
            }
        
        price_momentum = self.calculate_price_momentum(lookback_months=12, skip_months=1)
        rsi = self.calculate_relative_strength_index()
        mean_reversion = self.calculate_mean_reversion()
        
        # Combine factors using specified weights
        combined_factor = (
            weights['price_momentum'] * price_momentum +
            weights['rsi'] * rsi +
            weights['mean_reversion'] * mean_reversion
        )
        
        return combined_factor 