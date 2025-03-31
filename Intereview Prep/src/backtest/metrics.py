import pandas as pd
import numpy as np
from scipy import stats

class PerformanceMetrics:
    """
    Calculate various performance metrics for factor evaluation.
    """
    
    def __init__(self, price_data):
        """
        Initialize the performance metrics calculator.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        """
        self.price_data = price_data
        
    def calculate_factor_returns(self, factor_data, n_quantiles=5, holdings_period=20):
        """
        Calculate factor returns based on quantile portfolios.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        n_quantiles : int, optional
            Number of quantiles to divide stocks into.
        holdings_period : int, optional
            Number of days to hold each portfolio.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with returns for each quantile portfolio.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Calculate forward returns
        forward_returns = close_prices.pct_change(holdings_period).shift(-holdings_period)
        
        # Initialize quantile returns DataFrame
        quantile_returns = pd.DataFrame(index=factor_data.index, 
                                        columns=[f'Q{i+1}' for i in range(n_quantiles)])
        
        # Calculate returns for each date
        for date in factor_data.index:
            if date not in forward_returns.index:
                continue
                
            # Get factor values and forward returns for current date
            current_factors = factor_data.loc[date].dropna()
            current_returns = forward_returns.loc[date, current_factors.index]
            
            # Divide into quantiles
            quantiles = pd.qcut(current_factors, n_quantiles, labels=False, duplicates='drop')
            
            # Calculate returns for each quantile
            for i in range(n_quantiles):
                stocks_in_quantile = current_factors.index[quantiles == i]
                if len(stocks_in_quantile) > 0:
                    quantile_returns.loc[date, f'Q{i+1}'] = current_returns[stocks_in_quantile].mean()
        
        # Calculate long-short returns (top quantile - bottom quantile)
        quantile_returns['Long_Short'] = quantile_returns[f'Q{n_quantiles}'] - quantile_returns['Q1']
        
        return quantile_returns
    
    def calculate_information_coefficient(self, factor_data, forward_period=20):
        """
        Calculate Information Coefficient (IC) for a factor.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        forward_period : int, optional
            Number of days for forward returns calculation.
            
        Returns:
        --------
        pandas.Series
            Series with IC values for each date.
        """
        # Get closing prices
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Calculate forward returns
        forward_returns = close_prices.pct_change(forward_period).shift(-forward_period)
        
        # Initialize IC series
        ic_series = pd.Series(index=factor_data.index)
        
        # Calculate IC for each date
        for date in factor_data.index:
            if date not in forward_returns.index:
                continue
                
            # Get factor values and forward returns for current date
            current_factors = factor_data.loc[date].dropna()
            current_returns = forward_returns.loc[date, current_factors.index].dropna()
            
            # Align data
            common_stocks = current_factors.index.intersection(current_returns.index)
            if len(common_stocks) < 10:  # Require at least 10 stocks for meaningful correlation
                continue
                
            # Calculate rank correlation (Spearman's)
            factor_ranks = current_factors[common_stocks].rank()
            return_ranks = current_returns[common_stocks].rank()
            ic = factor_ranks.corr(return_ranks, method='spearman')
            
            ic_series[date] = ic
        
        return ic_series.dropna()
    
    def calculate_factor_decay(self, factor_data, max_periods=60, step=5):
        """
        Calculate how a factor's predictive power decays over time.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        max_periods : int, optional
            Maximum number of days to test factor decay.
        step : int, optional
            Step size (in days) for decay calculation.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with IC values for different forward periods.
        """
        # Initialize DataFrame for IC at different horizons
        periods = range(step, max_periods + step, step)
        decay_df = pd.DataFrame(index=periods, columns=['IC_Mean', 'IC_Std', 'IC_t_stat', 'p_value'])
        
        # Calculate IC for different forward periods
        for period in periods:
            ic_series = self.calculate_information_coefficient(factor_data, forward_period=period)
            
            # Calculate statistics
            ic_mean = ic_series.mean()
            ic_std = ic_series.std()
            t_stat = ic_mean / (ic_std / np.sqrt(len(ic_series)))
            p_value = 2 * (1 - stats.t.cdf(abs(t_stat), len(ic_series) - 1))
            
            decay_df.loc[period] = [ic_mean, ic_std, t_stat, p_value]
        
        return decay_df
    
    def calculate_factor_exposures(self, factor_data, risk_factors):
        """
        Calculate factor exposures to common risk factors.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        risk_factors : pandas.DataFrame
            DataFrame with risk factor values for stocks.
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with factor exposures to risk factors.
        """
        # Initialize DataFrame for exposures
        exposures = pd.DataFrame(index=factor_data.index, 
                                columns=risk_factors.columns)
        
        # Calculate exposures for each date
        for date in factor_data.index:
            if date not in risk_factors.index:
                continue
                
            # Get factor values and risk factors for current date
            current_factors = factor_data.loc[date].dropna()
            current_risks = risk_factors.loc[date]
            
            # Align data
            common_stocks = current_factors.index.intersection(current_risks.index)
            if len(common_stocks) < 10:
                continue
                
            # Calculate rank correlations with each risk factor
            for risk_factor in current_risks.columns:
                exposure = current_factors[common_stocks].corr(
                    current_risks.loc[common_stocks, risk_factor], method='spearman')
                exposures.loc[date, risk_factor] = exposure
        
        return exposures.dropna() 