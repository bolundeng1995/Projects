import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class BacktestEngine:
    """
    Engine for backtesting factor-based trading strategies.
    """
    
    def __init__(self, price_data, config):
        """
        Initialize the backtest engine.
        
        Parameters:
        -----------
        price_data : pandas.DataFrame
            Multi-index DataFrame with ticker and date indices and OHLCV data.
        config : dict
            Configuration dictionary with backtest parameters.
        """
        self.price_data = price_data
        self.config = config
        self.rebalance_frequency = config.get('rebalance_frequency', 'monthly')
        self.transaction_cost = config.get('transaction_cost', 0.0005)  # 5 bps per trade
        self.positions = None
        self.portfolio_value = None
        self.performance_metrics = None
        
    def generate_rebalance_dates(self, start_date, end_date):
        """
        Generate rebalance dates based on the specified frequency.
        
        Parameters:
        -----------
        start_date : str or datetime
            Start date for the backtest.
        end_date : str or datetime
            End date for the backtest.
            
        Returns:
        --------
        list
            List of rebalance dates.
        """
        if isinstance(start_date, str):
            start_date = pd.Timestamp(start_date)
        if isinstance(end_date, str):
            end_date = pd.Timestamp(end_date)
            
        all_dates = self.price_data.index.get_level_values('Date').unique()
        all_dates = all_dates.sort_values()
        
        # Filter dates within the backtest range
        valid_dates = all_dates[(all_dates >= start_date) & (all_dates <= end_date)]
        
        # Convert to pandas Series for date operations
        dates_series = pd.Series(valid_dates)
        
        if self.rebalance_frequency == 'daily':
            return valid_dates
        elif self.rebalance_frequency == 'weekly':
            # Get the first trading day of each week
            return valid_dates[dates_series.dt.dayofweek == 0].values
        elif self.rebalance_frequency == 'monthly':
            # Get dates with day <= 7 (first week of month)
            first_week_dates = dates_series[dates_series.dt.day <= 7]
            # Create year-month string for groupby
            year_month = first_week_dates.dt.strftime('%Y-%m')
            # Group by year-month and get first date in each group
            return first_week_dates.groupby(year_month).first().values
        elif self.rebalance_frequency == 'quarterly':
            # Get dates with day <= 7 (first week of month)
            first_week_dates = dates_series[dates_series.dt.day <= 7]
            # Create year-quarter string for groupby
            year_quarter = first_week_dates.dt.strftime('%Y-Q') + ((first_week_dates.dt.month - 1) // 3 + 1).astype(str)
            # Group by year-quarter and get first date in each group
            return first_week_dates.groupby(year_quarter).first().values
        else:
            raise ValueError(f"Invalid rebalance frequency: {self.rebalance_frequency}")
    
    def run_backtest(self, factor_data, start_date, end_date, market_neutral=True, 
                     risk_model=None, position_limits=None):
        """
        Run a backtest of a factor-based strategy.
        
        Parameters:
        -----------
        factor_data : pandas.DataFrame
            DataFrame with factor values for each ticker and date.
        start_date : str or datetime
            Start date for the backtest.
        end_date : str or datetime
            End date for the backtest.
        market_neutral : bool, optional
            Whether to construct a market-neutral portfolio.
        risk_model : object, optional
            Risk model for portfolio optimization.
        position_limits : dict, optional
            Dictionary with position limit constraints.
            
        Returns:
        --------
        dict
            Dictionary containing backtest results.
        """
        # Generate rebalance dates
        rebalance_dates = self.generate_rebalance_dates(start_date, end_date)
        
        # Get closing prices for the backtest period
        close_prices = self.price_data['Close'].unstack('Ticker')
        
        # Find the first date with sufficient factor data
        valid_factor_dates = factor_data.dropna(how='all').index
        first_valid_date = valid_factor_dates.min()
        
        # Adjust start date if needed to ensure we have factor data
        if first_valid_date > pd.Timestamp(start_date):
            print(f"Adjusting backtest start date from {start_date} to {first_valid_date} to ensure sufficient factor data")
            # Find the first rebalance date after we have factor data
            rebalance_dates = rebalance_dates[rebalance_dates >= first_valid_date]
            if len(rebalance_dates) < 2:
                raise ValueError("Insufficient rebalance dates after adjusting for factor data availability")
        
        # Find the actual first trading day
        first_rebalance = rebalance_dates[0]
        first_trading_day = close_prices.index[close_prices.index >= first_rebalance][0]
        
        # Get all trading days for the backtest period (starting at first rebalance)
        backtest_dates = close_prices.index[close_prices.index >= first_trading_day]
        
        # Initialize portfolio values and positions only for relevant dates
        portfolio_values = pd.Series(index=backtest_dates)
        positions = pd.DataFrame(0.0, index=backtest_dates, columns=close_prices.columns)
        
        # Set initial portfolio value
        portfolio_values.iloc[0] = 1.0  # Initialize with $1
        
        # Track positions for performance attribution
        position_history = []
        
        # Run backtest
        for i, rebalance_date in enumerate(rebalance_dates):
            # Find the next trading day after the rebalance date
            trading_day = close_prices.index[close_prices.index >= rebalance_date][0]
            
            # Find the next rebalance date or use end_date for the last rebalance
            if i < len(rebalance_dates) - 1:
                next_rebalance_date = rebalance_dates[i+1]
            else:
                # For the last rebalance date, hold until the end of the backtest
                next_rebalance_date = pd.Timestamp(end_date) + pd.Timedelta(days=1)
            
            next_trading_dates = close_prices.index[(close_prices.index >= trading_day) & 
                                                   (close_prices.index < next_rebalance_date)]
            
            # Find the previous trading day (if not the first rebalance)
            if i > 0:
                prev_trading_days = close_prices.index[close_prices.index < trading_day]
                if len(prev_trading_days) > 0:
                    prev_trading_day = prev_trading_days[-1]  # Get the most recent previous trading day
                    old_positions = positions.loc[prev_trading_day]
                    prev_portfolio_value = portfolio_values.loc[prev_trading_day]
                else:
                    old_positions = 0
                    prev_portfolio_value = 1.0
            else:
                old_positions = 0
                prev_portfolio_value = 1.0
            
            # Get factor values for the current rebalance date
            current_factors = factor_data.loc[trading_day]
            
            # Filter out stocks with missing data
            valid_stocks = ~current_factors.isna()
            valid_factors = current_factors[valid_stocks]
            
            # Calculate weights based on factors
            weights = self._calculate_weights(valid_factors, market_neutral)
            
            # Apply position limits if specified
            if position_limits:
                weights = self._apply_position_limits(weights, position_limits)
            
            # Calculate effective weights of current positions before rebalancing
            if isinstance(old_positions, pd.Series) and prev_portfolio_value > 0:
                # Calculate current weights based on current position values 
                raw_old_weights = old_positions / prev_portfolio_value
                
                # For market-neutral portfolios, normalize old weights to ensure they sum to zero
                if market_neutral and abs(raw_old_weights.sum()) > 1e-10:
                    # Separate long and short positions
                    long_weights = raw_old_weights[raw_old_weights > 0]
                    short_weights = raw_old_weights[raw_old_weights < 0]
                    
                    # If we have both long and short positions, normalize them separately
                    if len(long_weights) > 0 and len(short_weights) > 0:
                        # Scale long positions to sum to 1.0
                        long_weights = long_weights / long_weights.sum()
                        
                        # Scale short positions to sum to -1.0
                        short_weights = short_weights / abs(short_weights.sum()) * -1.0
                        
                        # Combine weights
                        old_weights = pd.concat([long_weights, short_weights])
                    else:
                        # If we've somehow lost either all long or all short positions
                        old_weights = raw_old_weights
                else:
                    # Non market-neutral or zero-sum portfolio
                    old_weights = raw_old_weights
            else:
                # No previous positions
                old_weights = pd.Series(0.0, index=positions.columns)
            
            # Calculate turnover properly
            # 1. Ensure both weight sets are defined on the same universe of stocks
            combined_index = weights.index.union(old_weights.index)
            weights_aligned = weights.reindex(combined_index, fill_value=0)
            old_weights_aligned = old_weights.reindex(combined_index, fill_value=0)
            
            # 2. Calculate absolute changes in weights (one-way turnover)
            weight_changes = (weights_aligned - old_weights_aligned).abs()
            
            # 3. Compute total turnover - should never exceed 2.0 for market-neutral
            weight_turnover = weight_changes.sum() / 2
            
            # Calculate transaction costs based on weight turnover
            transaction_costs = weight_turnover * self.transaction_cost * prev_portfolio_value
            
            # Update positions with new target values
            new_positions = pd.Series(0.0, index=positions.columns)
            new_positions[weights.index] = weights.values * prev_portfolio_value
            
            # Update portfolio value accounting for transaction costs
            positions.loc[trading_day] = new_positions
            portfolio_values.loc[trading_day] = prev_portfolio_value - transaction_costs
            
            # Store position details for attribution
            position_history.append({
                'date': trading_day,
                'weights': weights,
                'turnover': weight_turnover,
                'transaction_costs': transaction_costs
            })
            
            # Calculate returns for each day until the next rebalance
            for j in range(len(next_trading_dates) - 1):
                current_date = next_trading_dates[j]
                next_date = next_trading_dates[j+1]
                
                # Calculate stock returns
                stock_returns = close_prices.loc[next_date] / close_prices.loc[current_date] - 1
                
                # Calculate portfolio return
                portfolio_return = (positions.loc[current_date] * stock_returns).sum()
                
                # Update positions and portfolio value
                positions.loc[next_date] = positions.loc[current_date] * (1 + stock_returns)
                portfolio_values.loc[next_date] = portfolio_values.loc[current_date] * (1 + portfolio_return)
        
        # Calculate performance metrics
        self.positions = positions
        self.portfolio_value = portfolio_values
        self.performance_metrics = self._calculate_performance_metrics(portfolio_values)
        
        # Prepare results
        results = {
            'portfolio_value': portfolio_values,
            'positions': positions,
            'performance_metrics': self.performance_metrics,
            'position_history': position_history
        }
        
        return results
    
    def _calculate_weights(self, factor_values, market_neutral=True):
        """
        Calculate portfolio weights based on factor values.
        
        For market-neutral portfolios, this implementation:
        - Takes long positions only in the top 20% of stocks
        - Takes short positions only in the bottom 20% of stocks
        - Ensures equal dollar allocation to both sides
        
        Parameters:
        -----------
        factor_values : pandas.Series
            Series with factor values for each ticker.
        market_neutral : bool, optional
            Whether to construct a market-neutral portfolio.
            
        Returns:
        --------
        pandas.Series
            Series with portfolio weights for each ticker.
        """
        # Sort stocks by factor value
        sorted_factors = factor_values.sort_values(ascending=False)
        
        # Calculate the number of stocks in each 20% group (instead of 10%)
        num_stocks = len(sorted_factors)
        quintile_size = max(int(num_stocks * 0.2), 1)
        
        # Initialize weights
        weights = pd.Series(0.0, index=sorted_factors.index)
        
        if market_neutral:
            # Select top and bottom 20% of stocks
            top_stocks = sorted_factors.iloc[:quintile_size].index
            bottom_stocks = sorted_factors.iloc[-quintile_size:].index
            
            # Equal weight within the top and bottom groups
            weights[top_stocks] = 1.0 / quintile_size   # Long positions
            weights[bottom_stocks] = -1.0 / quintile_size  # Short positions
        else:
            # For long-only portfolio, use all stocks but focus on top performers
            # Apply equal weight to top stocks
            top_stocks = sorted_factors.iloc[:quintile_size].index
            weights[top_stocks] = 1.0 / quintile_size
        
        return weights
    
    def _apply_position_limits(self, weights, position_limits):
        """
        Apply position limits to portfolio weights.
        
        Parameters:
        -----------
        weights : pandas.Series
            Series with portfolio weights for each ticker.
        position_limits : dict
            Dictionary with position limit constraints.
            
        Returns:
        --------
        pandas.Series
            Series with adjusted portfolio weights.
        """
        # Apply maximum position size limit
        if 'max_position' in position_limits:
            max_pos = position_limits['max_position']
            weights[weights > max_pos] = max_pos
            weights[weights < -max_pos] = -max_pos
        
        # Normalize weights to ensure they sum to zero for market-neutral portfolio
        if abs(weights.sum()) > 1e-10:
            # Separate long and short positions
            long_weights = weights[weights > 0]
            short_weights = weights[weights < 0]
            
            # If we have both long and short positions, normalize them to ensure dollar neutrality
            if len(long_weights) > 0 and len(short_weights) > 0:
                # Scale long positions to sum to 1.0
                long_weights = long_weights / long_weights.sum()
                
                # Scale short positions to sum to -1.0
                short_weights = short_weights / abs(short_weights.sum()) * -1.0
                
                # Combine weights
                weights = pd.concat([long_weights, short_weights])
            else:
                # If we only have long or short positions (which shouldn't happen in a properly
                # constructed market-neutral strategy), just normalize to 1 or -1
                weights = weights / abs(weights.sum())
        
        return weights
    
    def _calculate_performance_metrics(self, portfolio_values):
        """
        Calculate performance metrics for the backtest.
        
        Parameters:
        -----------
        portfolio_values : pandas.Series
            Series with portfolio values over time.
            
        Returns:
        --------
        dict
            Dictionary with performance metrics.
        """
        # Calculate daily returns
        daily_returns = portfolio_values.pct_change().dropna()
        
        # Calculate performance metrics
        total_return = portfolio_values.iloc[-1] / portfolio_values.iloc[0] - 1
        annual_return = (1 + total_return) ** (252 / len(daily_returns)) - 1
        annual_volatility = daily_returns.std() * np.sqrt(252)
        sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else 0
        
        # Calculate drawdowns
        cumulative_returns = (1 + daily_returns).cumprod()
        running_max = cumulative_returns.cummax()
        drawdowns = (cumulative_returns / running_max) - 1
        max_drawdown = drawdowns.min()
        
        # Calculate additional metrics
        win_rate = (daily_returns > 0).mean()
        profit_loss_ratio = abs(daily_returns[daily_returns > 0].mean() / daily_returns[daily_returns < 0].mean()) if len(daily_returns[daily_returns < 0]) > 0 else float('inf')
        
        metrics = {
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio
        }
        
        return metrics 