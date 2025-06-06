import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import itertools
import seaborn as sns

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Position:
    """A class to track and manage individual trading positions.
    
    This class maintains the state of a single trading position, including its size,
    entry price, and position management logic. It handles position doubling and
    maintains the average entry price for proper PnL calculation.

    Attributes:
        size (int): Current position size (positive for long, negative for short, 0 for no position)
        entry_price (float): Initial entry price of the position
        avg_entry_price (float): Average entry price, updated when position is doubled
        entry_date (Optional[pd.Timestamp]): Date when the position was entered
        position_size (int): Position size multiplier (1 for normal, 2 for doubled position)
    """

    size: int = 0
    entry_price: float = 0.0
    avg_entry_price: float = 0.0
    entry_date: Optional[pd.Timestamp] = None
    position_size: int = 1

    def reset(self) -> None:
        """Reset the position to its initial state.
        
        This method is called when a position is closed or when a stop-loss is triggered.
        It resets all position attributes to their default values.
        """
        self.size = 0
        self.entry_price = 0.0
        self.avg_entry_price = 0.0
        self.entry_date = None
        self.position_size = 1

    def double_down(self, price: float) -> None:
        """Double the position size and update the average entry price.
        
        Args:
            price (float): Current price at which the position is being doubled
        """
        self.position_size = 2
        self.avg_entry_price = (self.avg_entry_price + price) / 2
        self.size *= 2

class TradeTracker:
    """A class to track trading performance and maintain trade records.
    
    This class handles all aspects of trade tracking, including PnL calculation,
    position updates, and performance metrics. It maintains a detailed record of
    all trades and their outcomes.

    Attributes:
        initial_capital (float): Starting capital for the strategy
        base_capital (float): Current capital level, updated with each trade
        cumulative_pnl (float): Total realized PnL across all trades
        positions (pd.DataFrame): DataFrame containing detailed trade records
        metrics (Dict): Dictionary containing performance metrics
    """

    def __init__(self, initial_capital: float):
        """Initialize the TradeTracker with initial capital.
        
        Args:
            initial_capital (float): Starting capital for the strategy
        """
        self.initial_capital = initial_capital
        self.base_capital = initial_capital
        self.cumulative_pnl = 0.0
        self.positions = None
        self.metrics = {}

    def initialize_tracking(self, index: pd.Index) -> None:
        """Initialize the positions DataFrame with the required columns.
        
        Args:
            index (pd.Index): Index for the positions DataFrame (typically dates)
        """
        self.positions = pd.DataFrame(index=index)
        self.positions['position'] = 0
        self.positions['capital'] = self.initial_capital
        self.positions['entry_price'] = 0.0
        self.positions['exit_price'] = 0.0
        self.positions['pnl'] = 0.0
        self.positions['unrealized_pnl'] = 0.0
        self.positions['cumulative_pnl'] = 0.0
        self.positions['trade_duration'] = 0
        self.positions['enter_reason'] = ''
        self.positions['exit_reason'] = ''

    def update_daily_pnl(self, idx: int, position: Position, price: float, prev_price: float) -> None:
        """Update daily PnL for an open position.
        
        Args:
            idx (int): Index of the current trading day
            position (Position): Current position object
            price (float): Current price
            prev_price (float): Previous day's price
        """
        if position.size != 0 and prev_price is not None:
            daily_pnl = position.size * (price - prev_price)
            self.base_capital += daily_pnl
            self.positions.iloc[idx, self.positions.columns.get_loc('position')] = position.size
            self.positions.iloc[idx, self.positions.columns.get_loc('unrealized_pnl')] = daily_pnl
            self.positions.iloc[idx, self.positions.columns.get_loc('capital')] = self.base_capital

    def record_trade_exit(self, idx: int, position: Position, price: float, exit_reason: str) -> None:
        """Record the details of a trade exit.
        
        Args:
            idx (int): Index of the current trading day
            position (Position): Position being exited
            price (float): Exit price
            exit_reason (str): Reason for exit (e.g., 'signal', 'stop_loss')
        """
        pnl = position.size * (price - position.avg_entry_price)
        self.cumulative_pnl += pnl
        self.positions.iloc[idx, self.positions.columns.get_loc('exit_price')] = price
        self.positions.iloc[idx, self.positions.columns.get_loc('pnl')] = pnl
        self.positions.iloc[idx, self.positions.columns.get_loc('cumulative_pnl')] = self.cumulative_pnl
        self.positions.iloc[idx, self.positions.columns.get_loc('exit_reason')] = exit_reason
        self.positions.iloc[idx, self.positions.columns.get_loc('trade_duration')] = (
            self.positions.index[idx] - position.entry_date
        ).days

    def record_position_update(self, idx: int, position: Position, price: float, 
                              transaction_cost: float, enter_reason: str = '') -> None:
        """Record position update details including transaction costs.
        
        Args:
            idx (int): Index of the current trading day
            position (Position): Updated position object
            price (float): Current price
            enter_reason (str): Reason for entering the position
        """
        self.positions.iloc[idx, self.positions.columns.get_loc('position')] = position.size
        self.positions.iloc[idx, self.positions.columns.get_loc('entry_price')] = price
        self.positions.iloc[idx, self.positions.columns.get_loc('enter_reason')] = enter_reason
        cost = abs(position.size) * price * transaction_cost
        self.base_capital -= cost
        self.positions.iloc[idx, self.positions.columns.get_loc('capital')] = self.base_capital

class COFTradingStrategy:
    """A trading strategy based on Cost of Funds (COF) analysis.
    
    This strategy trades based on deviations between actual and predicted COF levels,
    with position management including doubling down on extreme deviations and
    stop-loss protection.

    Attributes:
        cof_data (pd.DataFrame): DataFrame containing COF analysis results
        liquidity_data (pd.DataFrame): DataFrame containing liquidity indicators
        initial_capital (float): Starting capital for the strategy
        trade_tracker (TradeTracker): Object to track trading performance
        position (Position): Object to manage current position
    """

    def __init__(self, cof_data: pd.DataFrame, liquidity_data: pd.DataFrame, initial_capital: float = 0, cof_term: str = "cof"):
        """Initialize the COF trading strategy.
        
        Args:
            cof_data (pd.DataFrame): DataFrame containing COF analysis results
            liquidity_data (pd.DataFrame): DataFrame containing liquidity indicators
            initial_capital (float): Starting capital for backtesting
            cof_term (str): The COF term to analyze (e.g., "1Y COF", "3M COF", etc.)
        """
        self.cof_data = cof_data
        self.liquidity_data = liquidity_data
        self.initial_capital = initial_capital
        self.trade_tracker = TradeTracker(initial_capital)
        self.position = Position()
        self.cof_term = cof_term  # The value to predict or train
        self.lst_window_size = [13, 52, 104]
        
    def calculate_liquidity_stress(self) -> None:
        """Calculate a composite liquidity stress indicator.
        
        This method calculates a normalized composite stress indicator using
        rolling 1-year windows for each liquidity metric. The composite indicator
        is the average of the normalized individual indicators.
        """
        try:
            normalized_indicators = pd.DataFrame()
            window_size = 52  # 1 year of trading weeks
            
            for col in self.liquidity_data.columns:
                rolling_mean = self.liquidity_data[col].rolling(window=window_size, min_periods=1).mean()
                rolling_std = self.liquidity_data[col].rolling(window=window_size, min_periods=1).std()
                normalized_indicators[col] = (
                    self.liquidity_data[col] - rolling_mean
                ) / rolling_std
            
            self.liquidity_data['liquidity_stress'] = normalized_indicators.mean(axis=1)
            logger.info("Liquidity stress indicator calculated successfully")
            
        except Exception as e:
            logger.error(f"Error calculating liquidity stress: {str(e)}")
            raise
    
    def generate_signals(self, entry_threshold: float = 2.0, exit_threshold: float = 0.5, 
                        liquidity_threshold: Optional[float] = None) -> None:
        """Generate trading signals based on COF mispricing and liquidity indicators.
        
        Args:
            entry_threshold (float): Z-score threshold for entering positions
            exit_threshold (float): Z-score threshold for exiting positions
            liquidity_threshold (Optional[float]): Threshold for liquidity stress
        """
        try:
            self._calculate_cof_deviation()
            self._apply_signal_logic(entry_threshold, exit_threshold, liquidity_threshold)
            
            # Store thresholds in metrics
            self.trade_tracker.metrics['entry_threshold'] = entry_threshold
            self.trade_tracker.metrics['exit_threshold'] = exit_threshold
            
            logger.info("Trading signals generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating signals: {str(e)}")
            raise

    def _calculate_cof_deviation(self) -> None:
        """Calculate COF deviation and its z-score.
        
        This method calculates the deviation between actual and predicted COF levels
        and computes the rolling z-score of this deviation.
        """
        self.cof_data[f'{self.cof_term}_deviation'] = (
            self.cof_data[f'{self.cof_term}_actual'] - self.cof_data[f'{self.cof_term}_predicted']
            )
            
        # window_size = 52
        # rolling_mean = self.cof_data[f'{self.cof_term}_deviation'].rolling(window=window_size, min_periods=10).mean()
        # rolling_std = self.cof_data[f'{self.cof_term}_deviation'].rolling(window=window_size, min_periods=10).std()
        # self.cof_data[f'{self.cof_term}_deviation_zscore'] = (
        #     self.cof_data[f'{self.cof_term}_deviation'] - rolling_mean
        # ) / rolling_std
        
        # # Fill NaN values with 0
        # self.cof_data[f'{self.cof_term}_deviation_zscore'] = self.cof_data[f'{self.cof_term}_deviation_zscore'].fillna(0)

        for window_size in self.lst_window_size:
            rolling_mean = self.cof_data[f'{self.cof_term}_deviation'].rolling(window=window_size, min_periods=10).mean()
            rolling_std = self.cof_data[f'{self.cof_term}_deviation'].rolling(window=window_size, min_periods=10).std()
            self.cof_data[f'{self.cof_term}_deviation_zscore_{window_size}'] = (
                self.cof_data[f'{self.cof_term}_deviation'] - rolling_mean
            ) / rolling_std
            self.cof_data[f'{self.cof_term}_deviation_mean_{window_size}'] = rolling_mean
            self.cof_data[f'{self.cof_term}_deviation_std_{window_size}'] = rolling_std
            
            # Fill NaN values with 0
            self.cof_data[f'{self.cof_term}_deviation_zscore_{window_size}'] = self.cof_data[f'{self.cof_term}_deviation_zscore_{window_size}'].fillna(0)

            # get the aggregated zscore
            self.cof_data[f'{self.cof_term}_deviation_zscore'] = self.cof_data.loc[:, [f'{self.cof_term}_deviation_zscore_{window_size}' for window_size in self.lst_window_size]].mean(axis=1)
            self.cof_data[f'{self.cof_term}_deviation_zscore_std'] = self.cof_data.loc[:, [f'{self.cof_term}_deviation_std_{window_size}' for window_size in self.lst_window_size]].mean(axis=1)
            self.cof_data[f'{self.cof_term}_deviation_zscore_mean'] = self.cof_data.loc[:, [f'{self.cof_term}_deviation_mean_{window_size}' for window_size in self.lst_window_size]].mean(axis=1)
            self.cof_data[f'{self.cof_term}_deviation_zscore_up'] = self.cof_data[f'{self.cof_term}_predicted'] + self.cof_data[f'{self.cof_term}_deviation_zscore_mean'] + self.cof_data[f'{self.cof_term}_deviation_zscore_std']
            self.cof_data[f'{self.cof_term}_deviation_zscore_down'] = self.cof_data[f'{self.cof_term}_predicted'] + self.cof_data[f'{self.cof_term}_deviation_zscore_mean'] - self.cof_data[f'{self.cof_term}_deviation_zscore_std']

    def _apply_signal_logic(self, entry_threshold: float, exit_threshold: float, 
                          liquidity_threshold: Optional[float] = None) -> None:
        """Apply trading signal logic based on thresholds.
        
        Args:
            entry_threshold (float): Z-score threshold for entering positions
            exit_threshold (float): Z-score threshold for exiting positions
            liquidity_threshold (Optional[float]): Threshold for liquidity stress
        """
        self.cof_data['signal'] = 0
        
        # Define deviation thresholds (can be adjusted)
        deviation_entry_threshold = 5  # Example value, adjust based on your needs
        deviation_exit_threshold = 0    # Example value, adjust based on your needs
        
        if liquidity_threshold is None:
            long_condition = (
                (self.cof_data[f'{self.cof_term}_deviation_zscore'] < -entry_threshold) &
                (self.cof_data[f'{self.cof_term}_deviation'] < -deviation_entry_threshold)
            )
            short_condition = (
                (self.cof_data[f'{self.cof_term}_deviation_zscore'] > entry_threshold) &
                (self.cof_data[f'{self.cof_term}_deviation'] > deviation_entry_threshold)
            )
        else:
            long_condition = (
                ((self.cof_data[f'{self.cof_term}_deviation_zscore'] < -entry_threshold) &
                 (self.cof_data[f'{self.cof_term}_deviation'] < -deviation_entry_threshold)) &
                (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
            )
            short_condition = (
                ((self.cof_data[f'{self.cof_term}_deviation_zscore'] > entry_threshold) &
                 (self.cof_data[f'{self.cof_term}_deviation'] > deviation_entry_threshold)) &
                (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
            )
            
        self.cof_data.loc[long_condition, 'signal'] = 1
        self.cof_data.loc[short_condition, 'signal'] = -1
            
        # Apply exit conditions
        long_exit = (
            ((self.cof_data[f'{self.cof_term}_deviation_zscore'] > -exit_threshold) |
            (self.cof_data[f'{self.cof_term}_deviation'] > -deviation_exit_threshold)) &
            (self.cof_data['signal'].shift(1) == 1)
        )
        short_exit = (
            ((self.cof_data[f'{self.cof_term}_deviation_zscore'] < exit_threshold) |
            (self.cof_data[f'{self.cof_term}_deviation'] < deviation_exit_threshold)) &
            (self.cof_data['signal'].shift(1) == -1)
        )
        
        self.cof_data.loc[long_exit, 'signal'] = 0
        self.cof_data.loc[short_exit, 'signal'] = 0
        
        # Maintain positions until exit threshold is crossed
        for i in range(1, len(self.cof_data)):
            if self.cof_data['signal'].iloc[i - 1] == 1:
                if (self.cof_data[f'{self.cof_term}_deviation_zscore'].iloc[i] < -exit_threshold and 
                    self.cof_data[f'{self.cof_term}_deviation'].iloc[i] < -deviation_exit_threshold):
                    self.cof_data['signal'].iloc[i] = 1  # maintain long position
            elif self.cof_data['signal'].iloc[i - 1] == -1:
                if (self.cof_data[f'{self.cof_term}_deviation_zscore'].iloc[i] > exit_threshold and 
                    self.cof_data[f'{self.cof_term}_deviation'].iloc[i] > deviation_exit_threshold):
                    self.cof_data['signal'].iloc[i] = -1  # maintain short position
    
    def backtest(self, transaction_cost: float = 0.0, max_loss: float = 20,
                double_threshold: float = 2.5, max_position_size: int = 2) -> None:
        """Backtest the trading strategy.
        
        Args:
            transaction_cost (float): Transaction cost as a fraction of trade value
            max_loss (float): Maximum loss in absolute price terms
            double_threshold (float): Z-score threshold for doubling down
            max_position_size (int): Maximum allowed position size
        """
        try:
            self.trade_tracker.initialize_tracking(self.cof_data.index)
            prev_price = None
            
            for i in range(1, len(self.cof_data)):
                self._process_trading_day(i, transaction_cost, max_loss, 
                                       double_threshold, max_position_size, prev_price)
                prev_price = self.cof_data[f'{self.cof_term}_actual'].iloc[i]
            
            self._save_results()
            self.calculate_performance_metrics()
            logger.info("Backtesting completed successfully")
            
        except Exception as e:
            logger.error(f"Error in backtesting: {str(e)}")
            raise
    
    def _process_trading_day(self, idx: int, transaction_cost: float, max_loss: float,
                           double_threshold: float, max_position_size: int, 
                           prev_price: Optional[float]) -> None:
        """Process a single trading day.
        
        Args:
            idx (int): Index of the current trading day
            transaction_cost (float): Transaction cost as a fraction of trade value
            max_loss (float): Maximum loss in absolute price terms
            double_threshold (float): Z-score threshold for doubling down
            max_position_size (int): Maximum allowed position size
            prev_price (Optional[float]): Previous day's price
        """
        signal = self.cof_data['signal'].iloc[idx]
        price = self.cof_data[f'{self.cof_term}_actual'].iloc[idx]
        current_date = self.cof_data.index[idx]
        current_zscore = self.cof_data[f'{self.cof_term}_deviation_zscore'].iloc[idx]
        
        if self.position.size != 0:
            self._handle_existing_position(idx, price, prev_price, max_loss, 
                                        double_threshold, max_position_size, 
                                        transaction_cost, current_zscore)
            
        if signal == 0 and self.position.size == 0:
            self.trade_tracker.positions.iloc[idx, self.trade_tracker.positions.columns.get_loc('capital')] = self.trade_tracker.base_capital
        if signal != 0 and self.position.size == 0:
            self._enter_new_position(idx, signal, price, current_date, transaction_cost)
        elif signal == 0 and self.position.size != 0:
            self._exit_position(idx, price)

    def _handle_existing_position(self, idx: int, price: float, prev_price: Optional[float],
                                max_loss: float, double_threshold: float, max_position_size: int,
                                transaction_cost: float, current_zscore: float) -> None:
        """Handle logic for existing positions.
        
        Args:
            idx (int): Index of the current trading day
            price (float): Current price
            prev_price (Optional[float]): Previous day's price
            max_loss (float): Maximum loss in absolute price terms
            double_threshold (float): Z-score threshold for doubling down
            max_position_size (int): Maximum allowed position size
            transaction_cost (float): Transaction cost as a fraction of trade value
            current_zscore (float): Current COF deviation z-score
        """
        # Update daily PnL including transaction costs
        if prev_price is not None:
            daily_pnl = self.position.size * (price - prev_price)
            self.trade_tracker.update_daily_pnl(idx, self.position, price, prev_price)
            
        # Check stop loss using absolute terms
        cumulative_unrealized_pnl = self.position.size * (price - self.position.avg_entry_price)
        if cumulative_unrealized_pnl <= -max_loss:
            self.trade_tracker.record_trade_exit(idx, self.position, price, 'stop_loss')
            self.position.reset()
            return
        
        # Check doubling down
        if self.position.position_size < max_position_size:
            if (self.position.size > 0 and current_zscore < -double_threshold) or \
               (self.position.size < 0 and current_zscore > double_threshold):
                self.position.double_down(price)
                enter_reason = f'doubled_down_zscore_{current_zscore:.2f}'
                self.trade_tracker.record_position_update(idx, self.position, price, transaction_cost, enter_reason)
                logger.info(f"Doubled down position at {self.cof_data.index[idx]} with z-score {current_zscore:.2f}")

    def _enter_new_position(self, idx: int, signal: int, price: float, 
                          current_date: pd.Timestamp, transaction_cost: float) -> None:
        """Enter a new trading position.
        
        Args:
            idx (int): Index of the current trading day
            signal (int): Trading signal (1 for long, -1 for short)
            price (float): Current price
            current_date (pd.Timestamp): Current trading date
            transaction_cost (float): Transaction cost as a fraction of trade value
        """
        self.position.size = signal
        self.position.entry_price = price
        self.position.avg_entry_price = price
        self.position.entry_date = current_date
        
        # Determine entry reason based on signal and z-score
        current_zscore = self.cof_data[f'{self.cof_term}_deviation_zscore'].iloc[idx]
        if signal > 0:
            enter_reason = f'long_signal_zscore_{current_zscore:.2f}'
        else:
            enter_reason = f'short_signal_zscore_{current_zscore:.2f}'
            
        self.trade_tracker.record_position_update(idx, self.position, price, transaction_cost, enter_reason)

    def _exit_position(self, idx: int, price: float) -> None:
        """Exit an existing trading position.
        
        Args:
            idx (int): Index of the current trading day
            price (float): Current price
        """
        self.trade_tracker.record_trade_exit(idx, self.position, price, 'signal')
        self.position.reset()

    def _save_results(self) -> None:
        """Save trading results to CSV file."""
        results_df = self.trade_tracker.positions.copy()
        results_df[f'{self.cof_term}_actual'] = self.cof_data[f'{self.cof_term}_actual']
        results_df[f'{self.cof_term}_predicted'] = self.cof_data[f'{self.cof_term}_predicted']
        results_df[f'{self.cof_term}_deviation'] = self.cof_data[f'{self.cof_term}_deviation']
        results_df[f'{self.cof_term}_deviation_zscore'] = self.cof_data[f'{self.cof_term}_deviation_zscore']
        results_df[f'{self.cof_term}_deviation_zscore_up'] = self.cof_data[f'{self.cof_term}_deviation_zscore_up']
        results_df[f'{self.cof_term}_deviation_zscore_down'] = self.cof_data[f'{self.cof_term}_deviation_zscore_down']
        results_df[f'{self.cof_term}_deviation_zscore_mean'] = self.cof_data[f'{self.cof_term}_deviation_zscore_mean']
        results_df[f'{self.cof_term}_deviation_zscore_std'] = self.cof_data[f'{self.cof_term}_deviation_zscore_std']

        for window_size in self.lst_window_size:
            for stat in ['zscore', 'mean', 'std']:
                results_df[f'{self.cof_term}_deviation_{stat}_{window_size}'] = self.cof_data[f'{self.cof_term}_deviation_{stat}_{window_size}']
        
        # Format float columns to 2 decimal places
        float_columns = ['capital', 'entry_price', 'exit_price', 'pnl', 
                        'unrealized_pnl', 'cumulative_pnl', f'{self.cof_term}_actual', 
                        f'{self.cof_term}_predicted', f'{self.cof_term}_deviation'] + \
                        [f'{self.cof_term}_deviation_{stat}' for stat in ['zscore', 'mean', 'std', 'up', 'down']] + \
                        [f'{self.cof_term}_deviation_{stat}_{window_size}' for stat in ['zscore', 'mean', 'std'] for window_size in self.lst_window_size]
        results_df[float_columns] = results_df[float_columns].round(2)
        
        results_df.to_csv(f'trading_results_{self.cof_term}.csv')
        logger.info(f"Trading results saved to trading_results_{self.cof_term}.csv with entry and exit reasons")

    def calculate_performance_metrics(self) -> None:
        """Calculate strategy performance metrics.
        
        This method calculates various performance metrics including:
        - Total return
        - Sharpe ratio
        - Maximum drawdown
        - Win rate
        - Average win/loss PnL
        - Average trade duration
        """
        try:
            returns = self.trade_tracker.positions['capital'].diff()
            
            trades = self.trade_tracker.positions['pnl'] != 0
            winning_trades = self.trade_tracker.positions['pnl'] > 0
            losing_trades = self.trade_tracker.positions['pnl'] < 0
            
            avg_win_pnl = self.trade_tracker.positions['pnl'][winning_trades].mean()
            avg_loss_pnl = abs(self.trade_tracker.positions['pnl'][losing_trades].mean())
            win_loss_ratio = avg_win_pnl / avg_loss_pnl if avg_loss_pnl != 0 else float('inf')
            
            self.trade_tracker.metrics = {
                "num_trades": trades.sum(),
                'total_return': self.trade_tracker.positions['capital'].iloc[-1],
                'sharpe_ratio': np.sqrt(52) * returns.mean() / returns.std(),
                'max_drawdown': (self.trade_tracker.positions['capital'] - 
                               self.trade_tracker.positions['capital'].cummax()).min(),
                'win_rate': winning_trades.sum() / trades.sum(),
                'avg_win_pnl': avg_win_pnl,
                'avg_loss_pnl': -avg_loss_pnl,  # Store as negative for consistency
                'win_loss_ratio': win_loss_ratio,
                'avg_win_trade_duration': self.trade_tracker.positions['trade_duration'][winning_trades].mean(),
                'avg_loss_trade_duration': self.trade_tracker.positions['trade_duration'][losing_trades].mean(),
                'avg_trade_duration': self.trade_tracker.positions['trade_duration'][trades].mean(),
                'entry_threshold': self.trade_tracker.metrics.get('entry_threshold', 'N/A'),
                'exit_threshold': self.trade_tracker.metrics.get('exit_threshold', 'N/A')
            }
            
            logger.info("Performance metrics calculated successfully")
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {str(e)}")
            raise
    
    def plot_results(self) -> None:
        """Plot backtesting results.
        
        This method creates three plots:
        1. Portfolio value over time with trade markers
        2. Daily mark-to-market performance
        3. Trading positions
        """
        try:
            plt.figure(figsize=(15, 15))
            
            # Calculate total capital including unrealized PnL
            total_capital = self.trade_tracker.positions['capital'].copy()
            
            self._plot_portfolio_value(total_capital)
            self._plot_daily_performance(total_capital)
            self._plot_positions()
            
            plt.tight_layout()
            plt.show()
            
            self._print_metrics()
            
        except Exception as e:
            logger.error(f"Error plotting results: {str(e)}")
            raise

    def _plot_portfolio_value(self, total_capital: pd.Series) -> None:
        """Plot portfolio value over time.
        
        Args:
            total_capital (pd.Series): Series containing portfolio value over time
        """
        plt.subplot(3, 1, 1)
        plt.plot(self.trade_tracker.positions.index, total_capital, 
                label='Portfolio Value (with Unrealized PnL)', color='blue')
        
        for i in range(1, len(self.trade_tracker.positions)):
            if self.trade_tracker.positions['pnl'].iloc[i] != 0:
                color = 'green' if self.trade_tracker.positions['pnl'].iloc[i] > 0 else 'red'
                plt.scatter(self.trade_tracker.positions.index[i], total_capital.iloc[i], 
                          color=color, s=100, alpha=0.5)
        
        plt.title('Strategy Performance')
        plt.legend()
        plt.grid(True)

    def _plot_daily_performance(self, total_capital: pd.Series) -> None:
        """Plot daily mark-to-market performance.
        
        Args:
            total_capital (pd.Series): Series containing portfolio value over time
        """
        plt.subplot(3, 1, 2)
        daily_returns = total_capital.diff()
        
        positive_returns = daily_returns.copy()
        positive_returns[positive_returns < 0] = 0
        negative_returns = daily_returns.copy()
        negative_returns[negative_returns > 0] = 0
        
        plt.bar(self.trade_tracker.positions.index, positive_returns, 
               label='Positive Returns', color='green', alpha=0.6)
        plt.bar(self.trade_tracker.positions.index, negative_returns, 
               label='Negative Returns', color='red', alpha=0.6)
        
        plt.axhline(y=0, color='black', linestyle='--', alpha=0.3)
        plt.title('Daily Mark-to-Market Performance')
        plt.legend()
        plt.grid(True)

    def _plot_positions(self) -> None:
        """Plot trading positions over time."""
        plt.subplot(3, 1, 3)
        plt.plot(self.trade_tracker.positions.index, self.trade_tracker.positions['position'], 
                label='Position')
        plt.title('Trading Positions')
        plt.legend()
        plt.grid(True)

    def _print_metrics(self) -> None:
        """Print strategy performance metrics."""
        print("\nStrategy Performance Metrics:")
        print(f"Entry Threshold: {self.trade_tracker.metrics.get('entry_threshold', 'N/A'):.2f}")
        print(f"Exit Threshold: {self.trade_tracker.metrics.get('exit_threshold', 'N/A'):.2f}")
        print(f"Number of Trades: {self.trade_tracker.metrics['num_trades']}")
        print(f"Total Return: {self.trade_tracker.metrics['total_return']:.2f}")
        print(f"Sharpe Ratio: {self.trade_tracker.metrics['sharpe_ratio']:.2f}")
        print(f"Maximum Drawdown: {self.trade_tracker.metrics['max_drawdown']:.2f}")
        print(f"Win Rate: {self.trade_tracker.metrics['win_rate']:.2%}")
        print(f"Average Win PnL: {self.trade_tracker.metrics['avg_win_pnl']:.2f}")
        print(f"Average Loss PnL: {self.trade_tracker.metrics['avg_loss_pnl']:.2f}")
        print(f"Win/Loss PnL Ratio: {self.trade_tracker.metrics['win_loss_ratio']:.2f}")
        print(f"Average Trade Duration: {self.trade_tracker.metrics['avg_trade_duration']:.2f} days")
        print(f"Average Win Trade Duration: {self.trade_tracker.metrics['avg_win_trade_duration']:.2f} days")
        print(f"Average Loss Trade Duration: {self.trade_tracker.metrics['avg_loss_trade_duration']:.2f} days")

    def reset_strategy(self, cof_data: Optional[pd.DataFrame] = None, liquidity_data: Optional[pd.DataFrame] = None) -> None:
        """Reset the strategy state to its initial condition.
        
        Args:
            cof_data (Optional[pd.DataFrame]): If provided, use this as the new cof_data.
                                              If None, keep the current cof_data.
            liquidity_data (Optional[pd.DataFrame]): If provided, use this as the new liquidity_data.
                                                    If None, keep the current liquidity_data.
        """
        if cof_data is not None:
            self.cof_data = cof_data.copy()
        if liquidity_data is not None:
            self.liquidity_data = liquidity_data.copy()
        self.trade_tracker = TradeTracker(self.initial_capital)
        self.position = Position()
        self.calculate_liquidity_stress()

    def grid_search(self, param_grid: Dict[str, List[float]], 
                   transaction_cost: float = 0.0, max_loss: float = 20,
                   max_position_size: int = 2, double_threshold: float = 2.5) -> pd.DataFrame:
        """Perform grid search over parameter combinations.
        
        Args:
            param_grid (Dict[str, List[float]]): Dictionary of parameters and their values to test
                Example: {
                    'entry_threshold': [1.5, 2.0, 2.5],
                    'exit_threshold': [0.5, 1.0, 1.5]
                }
            transaction_cost (float): Transaction cost as a fraction of trade value
            max_loss (float): Maximum loss in absolute price terms
            max_position_size (int): Maximum allowed position size
            double_threshold (float): Fixed threshold for doubling down
            
        Returns:
            pd.DataFrame: Results of grid search with performance metrics for each combination
        """
        results = []
        
        # Store original data
        original_cof_data = self.cof_data.copy()
        original_liquidity_data = self.liquidity_data.copy()
        
        # Generate parameter combinations where entry_threshold > exit_threshold
        param_combinations = []
        for entry in param_grid['entry_threshold']:
            for exit in param_grid['exit_threshold']:
                if entry > exit:  # Only include combinations where entry > exit
                    param_combinations.append({
                        'entry_threshold': entry,
                        'exit_threshold': exit
                    })
        
        for params in param_combinations:
            try:
                # Reset strategy state
                self.reset_strategy(original_cof_data, original_liquidity_data)
                
                # Generate signals with current parameters
                self.generate_signals(
                    entry_threshold=params['entry_threshold'],
                    exit_threshold=params['exit_threshold']
                )
                
                # Run backtest
                self.backtest()
                
                # Calculate performance metrics
                self.calculate_performance_metrics()
                
                # Store results using metrics directly
                result = {
                    **params,  # Include parameter values
                    **self.trade_tracker.metrics.copy()  # Include all metrics with a copy
                }
                results.append(result)
                
                logger.info(f"Completed parameter combination: {params}")
                
            except Exception as e:
                logger.error(f"Error in parameter combination {params}: {str(e)}")
                continue
        
        # Convert results to DataFrame and sort by Sharpe ratio
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('sharpe_ratio', ascending=False)
        
        # Format float columns to 2 decimal places
        float_columns = ['entry_threshold', 'exit_threshold', 'total_return', 'sharpe_ratio', 
                        'max_drawdown', 'win_rate', 'avg_win_pnl', 'avg_loss_pnl',
                        'win_loss_ratio', 'avg_trade_duration']
        results_df[float_columns] = results_df[float_columns].round(2)
        
        # Save results to CSV
        results_df.to_csv('grid_search_results.csv', float_format='%.2f')
        logger.info("Grid search results saved to grid_search_results.csv")
        
        # Create performance grid visualizations
        self._plot_performance_grids(results_df)
        
        return results_df

    def _plot_performance_grids(self, results_df: pd.DataFrame) -> None:
        """Create grid visualizations of performance metrics.
        
        Args:
            results_df (pd.DataFrame): Results from grid search
        """
        # Create pivot tables for each metric
        metrics = ['sharpe_ratio', 'total_return', 'win_rate', 'max_drawdown', 
                  'win_loss_ratio', 'avg_trade_duration']
        fig, axes = plt.subplots(3, 2, figsize=(15, 18))
        fig.suptitle('Performance Metrics Grid', fontsize=16)
        
        for idx, metric in enumerate(metrics):
            row = idx // 2
            col = idx % 2
            
            # Create pivot table
            pivot = results_df.pivot_table(
                values=metric,
                index='entry_threshold',
                columns='exit_threshold'
            )
            
            # Create mask for invalid combinations (where entry <= exit)
            mask = np.zeros_like(pivot, dtype=bool)
            for i, entry in enumerate(pivot.index):
                for j, exit_val in enumerate(pivot.columns):
                    if entry <= exit_val:
                        mask[i, j] = True
            
            # Plot heatmap with mask
            sns.heatmap(pivot, 
                       annot=True, 
                       fmt='.2f',
                       cmap='RdYlGn' if metric not in ['max_drawdown', 'avg_trade_duration'] else 'RdYlGn_r',
                       mask=mask,
                       ax=axes[row, col])
            
            axes[row, col].set_title(f'{metric.replace("_", " ").title()}')
            axes[row, col].set_xlabel('Exit Threshold')
            axes[row, col].set_ylabel('Entry Threshold')
        
        plt.tight_layout()
        plt.savefig('performance_grids.png')
        plt.show()  # Display the plot in the notebook
        plt.close()

def main():
    """Main function to run the trading strategy.
    
    This function:
    1. Loads data from Excel
    2. Prepares data for the strategy
    3. Initializes and runs the strategy
    4. Displays results
    """
    # Load data from Excel
    data = pd.read_excel('COF_DATA.xlsx', index_col=0)
    
    # Initialize strategy first to get cof_term
    strategy = COFTradingStrategy(pd.DataFrame(), pd.DataFrame(), cof_term="1Y COF")
    
    # Prepare data for strategy
    cof_data = pd.DataFrame({
        f'{strategy.cof_term}_actual': data[strategy.cof_term],
        f'{strategy.cof_term}_predicted': data[f'{strategy.cof_term}_predicted']
    })
    
    liquidity_data = data[['fed_funds_sofr_spread']]
    
    # Reset strategy with actual data
    strategy.reset_strategy(cof_data, liquidity_data)
    strategy.calculate_liquidity_stress()
    
    # Define parameter grid for grid search
    param_grid = {
        'entry_threshold': [1.5, 2.0, 2.5, 3.0],
        'exit_threshold': [0.5, 1.0, 1.5, 2.0]
    }
    
    # Run grid search
    results = strategy.grid_search(param_grid)
    
    # Print top 5 parameter combinations
    print("\nTop 5 Parameter Combinations by Sharpe Ratio:")
    print(results.head().to_string())
    
    # Run strategy with best parameters
    best_params = results.iloc[0]
    
    # Reset strategy state and run with best parameters
    strategy.reset_strategy(cof_data, liquidity_data)
    strategy.generate_signals(
        entry_threshold=best_params['entry_threshold'],
        exit_threshold=best_params['exit_threshold']
    )
    strategy.backtest()
    strategy.calculate_performance_metrics()
    strategy.plot_results()

if __name__ == "__main__":
    main() 