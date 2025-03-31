import pandas as pd
import numpy as np

class PortfolioConstructor:
    """
    Handles portfolio construction based on factor scores.
    """
    
    def __init__(self, price_data, config):
        """
        Initialize the portfolio constructor.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        config : dict
            Configuration dictionary with portfolio construction parameters.
        """
        self.price_data = price_data
        self.config = config
        self.market_neutral = config.get('market_neutral', True)
        self.position_limits = config.get('position_limits', {'max_position': 0.05})
        self.sector_neutral = config.get('sector_neutral', False)
        self.sector_data = None
        
    def set_sector_data(self, sector_data):
        """
        Set sector classification data for sector-neutral construction.
        
        Parameters:
        -----------
        sector_data : pandas.Series
            Series with sector classifications for each ticker.
        """
        self.sector_data = sector_data
        
    def construct_portfolio(self, factor_data, date, risk_model=None):
        """
        Construct a portfolio based on factor scores for a given date.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        date : datetime
            Date for which to construct the portfolio.
        risk_model : object, optional
            Risk model for incorporating risk constraints.
            
        Returns:
        --------
        pandas.Series
            Series with portfolio weights for each ticker.
        """
        # Get factor values for the current date
        try:
            current_factors = factor_data.loc[date].dropna()
        except KeyError:
            raise ValueError(f"No factor data available for date: {date}")
            
        # Apply sector neutrality if specified
        if self.sector_neutral and self.sector_data is not None:
            weights = self._construct_sector_neutral_portfolio(current_factors)
        else:
            weights = self._construct_simple_portfolio(current_factors)
            
        # Apply position limits
        weights = self._apply_position_limits(weights)
        
        return weights
    
    def _construct_simple_portfolio(self, factor_values):
        """
        Construct a simple portfolio based on factor values.
        
        Parameters:
        -----------
        factor_values : pandas.Series
            Series with factor values for each ticker.
            
        Returns:
        --------
        pandas.Series
            Series with portfolio weights for each ticker.
        """
        # Rank stocks based on factor values
        ranks = factor_values.rank(method='first')
        
        if self.market_neutral:
            # Separate into longs and shorts
            median_rank = ranks.median()
            long_stocks = ranks[ranks > median_rank].index
            short_stocks = ranks[ranks <= median_rank].index
            
            # Calculate weights proportional to factor ranks
            long_weights = ranks[long_stocks] / ranks[long_stocks].sum()
            short_weights = -ranks[short_stocks] / ranks[short_stocks].sum()
            
            # Normalize to ensure long weights sum to 1 and short weights sum to -1
            long_weights = long_weights / long_weights.sum()
            short_weights = short_weights / abs(short_weights.sum()) * -1.0
            
            # Combine weights
            weights = pd.concat([long_weights, short_weights])
        else:
            # Long-only portfolio
            weights = ranks / ranks.sum()
        
        return weights
    
    def _construct_sector_neutral_portfolio(self, factor_values):
        """
        Construct a sector-neutral portfolio based on factor values.
        
        Parameters:
        -----------
        factor_values : pandas.Series
            Series with factor values for each ticker.
            
        Returns:
        --------
        pandas.Series
            Series with sector-neutral portfolio weights.
        """
        # Get sectors for stocks in factor_values
        sectors = self.sector_data[factor_values.index].dropna()
        common_stocks = factor_values.index.intersection(sectors.index)
        
        # Filter to common stocks
        factor_subset = factor_values[common_stocks]
        sector_subset = sectors[common_stocks]
        
        # Initialize weights
        weights = pd.Series(0.0, index=factor_subset.index)
        
        # Process each sector
        for sector in sector_subset.unique():
            sector_stocks = sector_subset[sector_subset == sector].index
            sector_factors = factor_subset[sector_stocks]
            
            if self.market_neutral:
                # Split each sector into longs and shorts
                sector_ranks = sector_factors.rank(method='first')
                sector_median = sector_ranks.median()
                
                sector_long = sector_ranks[sector_ranks > sector_median].index
                sector_short = sector_ranks[sector_ranks <= sector_median].index
                
                if len(sector_long) > 0 and len(sector_short) > 0:
                    # Calculate weights within sector
                    long_weights = sector_ranks[sector_long] / sector_ranks[sector_long].sum()
                    short_weights = -sector_ranks[sector_short] / sector_ranks[sector_short].sum()
                    
                    # Normalize within sector
                    long_weights = long_weights / long_weights.sum()
                    short_weights = short_weights / abs(short_weights.sum()) * -1.0
                    
                    # Scale by sector weight (equal sector allocation)
                    sector_weight = 1.0 / len(sector_subset.unique())
                    weights[sector_long] = long_weights * sector_weight * 0.5
                    weights[sector_short] = short_weights * sector_weight * 0.5
            else:
                # Long-only sector allocation
                sector_ranks = sector_factors.rank(method='first')
                sector_weights = sector_ranks / sector_ranks.sum()
                
                # Scale by sector weight (equal sector allocation)
                sector_weight = 1.0 / len(sector_subset.unique())
                weights[sector_stocks] = sector_weights * sector_weight
        
        return weights
    
    def _apply_position_limits(self, weights):
        """
        Apply position limits to portfolio weights.
        
        Parameters:
        -----------
        weights : pandas.Series
            Series with portfolio weights for each ticker.
            
        Returns:
        --------
        pandas.Series
            Series with adjusted portfolio weights.
        """
        # Apply maximum position size limit
        if 'max_position' in self.position_limits:
            max_pos = self.position_limits['max_position']
            weights[weights > max_pos] = max_pos
            weights[weights < -max_pos] = -max_pos
        
        # Normalize weights after applying limits
        if self.market_neutral:
            # Separate long and short positions
            long_weights = weights[weights > 0]
            short_weights = weights[weights < 0]
            
            # If we have both long and short positions, normalize them
            if len(long_weights) > 0 and len(short_weights) > 0:
                # Scale long positions to sum to 1.0
                long_weights = long_weights / long_weights.sum()
                
                # Scale short positions to sum to -1.0
                short_weights = short_weights / abs(short_weights.sum()) * -1.0
                
                # Combine weights
                weights = pd.concat([long_weights, short_weights])
            else:
                # If we only have long or short positions (edge case)
                weights = weights / abs(weights.sum())
        else:
            # Long-only normalization
            if weights.sum() > 0:
                weights = weights / weights.sum()
        
        return weights 