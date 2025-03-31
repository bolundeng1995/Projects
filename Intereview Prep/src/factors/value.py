import pandas as pd
import numpy as np

class ValueFactors:
    """
    Implements various value-based factors.
    """
    
    def __init__(self, price_data, fundamental_data):
        """
        Initialize the value factors calculator.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        fundamental_data : pandas.DataFrame
            DataFrame with fundamental data for tickers.
        """
        self.price_data = price_data
        self.fundamental_data = fundamental_data
        
    def calculate_price_to_book(self):
        """
        Calculate Price-to-Book ratio factor.
        
        Returns:
        --------
        pandas.DataFrame
            DataFrame with P/B values for each ticker and date.
        """
        # In a real implementation, you would extract book value per share
        # from the fundamental data and calculate P/B ratio
        # This is a simplified version
        
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Create a dummy P/B ratio for demonstration
        # In practice, you would calculate this using actual book values
        pb_ratios = close_prices.copy()
        
        # Simulate P/B ratio calculation (random values for demonstration)
        for ticker in pb_ratios.columns:
            # In reality, you would use actual book values here
            # This is just simulating the effect with random numbers
            book_value = np.random.uniform(10, 100)
            pb_ratios[ticker] = close_prices[ticker] / book_value
        
        # Calculate the cross-sectional factor (negative since lower P/B typically means higher value)
        pb_factor = -pb_ratios.rank(axis=1, pct=True)
        
        return pb_factor
    
    def calculate_price_to_earnings(self):
        """
        Calculate Price-to-Earnings ratio factor.
        
        Returns:
        --------
        pandas.DataFrame
            DataFrame with P/E values for each ticker and date.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Create a dummy P/E ratio for demonstration
        pe_ratios = close_prices.copy()
        
        # Simulate P/E ratio calculation
        for ticker in pe_ratios.columns:
            # In reality, you would use actual earnings here
            earnings_per_share = np.random.uniform(0.5, 10)
            pe_ratios[ticker] = close_prices[ticker] / earnings_per_share
        
        # Calculate the cross-sectional factor (negative since lower P/E typically means higher value)
        pe_factor = -pe_ratios.rank(axis=1, pct=True)
        
        return pe_factor
    
    def calculate_ev_to_ebitda(self):
        """
        Calculate Enterprise Value to EBITDA ratio factor.
        
        Returns:
        --------
        pandas.DataFrame
            DataFrame with EV/EBITDA values for each ticker and date.
        """
        # This would require enterprise value and EBITDA data
        # For now, we'll create a placeholder similar to above
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Create a dummy EV/EBITDA ratio
        ev_ebitda_ratios = close_prices.copy()
        
        # Simulate EV/EBITDA calculation
        for ticker in ev_ebitda_ratios.columns:
            # In reality, you would calculate this properly
            ebitda = np.random.uniform(1, 20)
            ev_factor = np.random.uniform(1.5, 3.0)  # Simulating enterprise value as a multiple of price
            ev_ebitda_ratios[ticker] = close_prices[ticker] * ev_factor / ebitda
        
        # Calculate the cross-sectional factor (negative since lower EV/EBITDA typically means higher value)
        ev_ebitda_factor = -ev_ebitda_ratios.rank(axis=1, pct=True)
        
        return ev_ebitda_factor
    
    def combine_value_factors(self, weights=None):
        """
        Combine multiple value factors into a single composite factor.
        
        Parameters:
        -----------
        weights : dict, optional
            Dictionary with factor names as keys and weights as values.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with combined value factor for each ticker and date.
        """
        if weights is None:
            weights = {
                'price_to_book': 0.4,
                'price_to_earnings': 0.4,
                'ev_to_ebitda': 0.2
            }
        
        pb_factor = self.calculate_price_to_book()
        pe_factor = self.calculate_price_to_earnings()
        ev_ebitda_factor = self.calculate_ev_to_ebitda()
        
        # Combine factors using specified weights
        combined_factor = (
            weights['price_to_book'] * pb_factor +
            weights['price_to_earnings'] * pe_factor +
            weights['ev_to_ebitda'] * ev_ebitda_factor
        )
        
        return combined_factor 