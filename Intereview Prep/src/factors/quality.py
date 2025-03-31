import pandas as pd
import numpy as np

class QualityFactors:
    """
    Implements various quality-based factors.
    """
    
    def __init__(self, price_data, fundamental_data):
        """
        Initialize the quality factors calculator.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        fundamental_data : pandas.DataFrame
            DataFrame with fundamental data for tickers.
        """
        self.price_data = price_data
        self.fundamental_data = fundamental_data
        
    def calculate_return_on_equity(self):
        """
        Calculate Return on Equity (ROE) quality factor.
        
        Returns:
        --------
        pandas.DataFrame
            DataFrame with ROE values for each ticker and date.
        """
        # In a real implementation, you would extract ROE from fundamental data
        # This is a simplified version for demonstration
        
        # Get tickers
        tickers = self.price_data.index.get_level_values('Ticker').unique()
        dates = self.price_data.index.get_level_values('Date').unique()
        
        # Create a DataFrame to store ROE values
        roe_values = pd.DataFrame(index=dates, columns=tickers)
        
        # Fill with simulated ROE values
        for ticker in tickers:
            # In reality, you would use actual ROE data
            base_roe = np.random.uniform(0.05, 0.30)  # Random baseline ROE between 5% and 30%
            
            # Add some time variation to make it realistic
            trend = np.linspace(-0.05, 0.05, len(dates))  # Slight trend
            noise = np.random.normal(0, 0.02, len(dates))  # Small random variations
            
            roe_series = base_roe + trend + noise
            roe_values[ticker] = roe_series
        
        # Rank stocks cross-sectionally based on ROE
        roe_factor = roe_values.rank(axis=1, pct=True)
        
        return roe_factor
    
    def calculate_earnings_stability(self, window=8):
        """
        Calculate earnings stability factor based on earnings volatility.
        
        Parameters:
        -----------
        window : int, optional
            Number of quarters to consider for stability calculation.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with earnings stability values for each ticker and date.
        """
        # This would require quarterly earnings data
        # For demonstration, we'll simulate earnings data
        
        # Get tickers and dates
        tickers = self.price_data.index.get_level_values('Ticker').unique()
        dates = self.price_data.index.get_level_values('Date').unique()
        
        # Create a DataFrame to store earnings volatility values
        earnings_volatility = pd.DataFrame(index=dates, columns=tickers)
        
        # Fill with simulated earnings volatility values
        for ticker in tickers:
            # Simulate earnings volatility - lower is better for quality
            base_volatility = np.random.uniform(0.1, 0.5)
            
            # Add some time variation
            trend = np.linspace(-0.05, 0.05, len(dates))
            noise = np.random.normal(0, 0.05, len(dates))
            
            volatility_series = base_volatility + trend + noise
            volatility_series = np.maximum(0.01, volatility_series)  # Ensure positive values
            
            earnings_volatility[ticker] = volatility_series
        
        # Rank stocks cross-sectionally based on earnings stability (negative of volatility)
        earnings_stability_factor = -earnings_volatility.rank(axis=1, pct=True)
        
        return earnings_stability_factor
    
    def calculate_debt_to_equity(self):
        """
        Calculate Debt-to-Equity ratio quality factor.
        
        Returns:
        --------
        pandas.DataFrame
            DataFrame with D/E values for each ticker and date.
        """
        # In a real implementation, you would extract D/E from fundamental data
        # This is a simplified version for demonstration
        
        # Get tickers and dates
        tickers = self.price_data.index.get_level_values('Ticker').unique()
        dates = self.price_data.index.get_level_values('Date').unique()
        
        # Create a DataFrame to store D/E values
        de_values = pd.DataFrame(index=dates, columns=tickers)
        
        # Fill with simulated D/E values
        for ticker in tickers:
            # In reality, you would use actual D/E data
            base_de = np.random.uniform(0.2, 2.0)  # Random baseline D/E
            
            # Add some time variation to make it realistic
            trend = np.linspace(-0.1, 0.1, len(dates))
            noise = np.random.normal(0, 0.05, len(dates))
            
            de_series = base_de + trend + noise
            de_series = np.maximum(0, de_series)  # Ensure non-negative values
            
            de_values[ticker] = de_series
        
        # Rank stocks cross-sectionally based on D/E (negative since lower D/E is generally better)
        de_factor = -de_values.rank(axis=1, pct=True)
        
        return de_factor
    
    def combine_quality_factors(self, weights=None):
        """
        Combine multiple quality factors into a single composite factor.
        
        Parameters:
        -----------
        weights : dict, optional
            Dictionary with factor names as keys and weights as values.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with combined quality factor for each ticker and date.
        """
        if weights is None:
            weights = {
                'return_on_equity': 0.4,
                'earnings_stability': 0.4,
                'debt_to_equity': 0.2
            }
        
        roe_factor = self.calculate_return_on_equity()
        earnings_stability_factor = self.calculate_earnings_stability()
        de_factor = self.calculate_debt_to_equity()
        
        # Combine factors using specified weights
        combined_factor = (
            weights['return_on_equity'] * roe_factor +
            weights['earnings_stability'] * earnings_stability_factor +
            weights['debt_to_equity'] * de_factor
        )
        
        return combined_factor 