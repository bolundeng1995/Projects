import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class COFTradingStrategy:
    def __init__(self, cof_data, liquidity_data, initial_capital=0):
        """
        Initialize the COF trading strategy
        
        Parameters:
        -----------
        cof_data : pd.DataFrame
            DataFrame containing COF analysis results with columns:
            - cof_actual: Actual COF levels
            - cof_predicted: Model-predicted COF levels
            - futures_price: SPX futures prices
        liquidity_data : pd.DataFrame
            DataFrame containing liquidity indicators with columns:
            - fed_funds_sofr_spread
            - swap_spread
            - jpyusd_basis
        initial_capital : float
            Initial capital for backtesting
        """
        self.cof_data = cof_data
        self.liquidity_data = liquidity_data
        self.initial_capital = initial_capital
        self.positions = None
        self.portfolio_value = None
        
    def calculate_liquidity_stress(self):
        """Calculate a composite liquidity stress indicator using rolling 1-year windows"""
        try:
            # Normalize each liquidity indicator using rolling 1-year windows
            normalized_indicators = pd.DataFrame()
            window_size = 52  # 1 year of trading weeks
            
            for col in self.liquidity_data.columns:
                # Calculate rolling mean and std (using shift to look forward since data is in descending order)
                rolling_mean = self.liquidity_data[col].rolling(window=window_size, min_periods=1).mean()
                rolling_std = self.liquidity_data[col].rolling(window=window_size, min_periods=1).std()
                
                # Calculate z-score using rolling statistics
                normalized_indicators[col] = (
                    self.liquidity_data[col] - rolling_mean
                ) / rolling_std
            
            # Calculate composite stress indicator (equal weights)
            self.liquidity_data['liquidity_stress'] = normalized_indicators.mean(axis=1)
            
            logger.info("Liquidity stress indicator calculated successfully using rolling 1-year windows")
            
        except Exception as e:
            logger.error(f"Error calculating liquidity stress: {str(e)}")
            raise
    
    def generate_signals(self, entry_threshold=2.0, exit_threshold=0.5, liquidity_threshold=0.01):
        """
        Generate trading signals based on COF mispricing and liquidity indicators
        
        Parameters:
        -----------
        entry_threshold : float
            Z-score threshold for entering a position (e.g., 2.0 for 2 standard deviations)
        exit_threshold : float
            Z-score threshold for exiting a position (e.g., 0.5 for 0.5 standard deviations)
        liquidity_threshold : float
            Threshold for liquidity stress to trigger a signal
        """
        try:
            # Calculate COF deviation
            self.cof_data['cof_deviation'] = (
                self.cof_data['cof_actual'] - self.cof_data['cof_predicted']
            )
            
            # Calculate rolling z-score of COF deviation
            window_size = 52  # 1 year of trading weeks
            rolling_mean = self.cof_data['cof_deviation'].rolling(window=window_size, min_periods=10).mean()
            rolling_std = self.cof_data['cof_deviation'].rolling(window=window_size, min_periods=10).std()
            self.cof_data['cof_deviation_zscore'] = (
                self.cof_data['cof_deviation'] - rolling_mean
            ) / rolling_std
            
            # Calculate liquidity stress if not already done
            if 'liquidity_stress' not in self.liquidity_data.columns:
                self.calculate_liquidity_stress()
            
            # Generate signals
            self.cof_data['signal'] = 0

            if liquidity_threshold == None:
                            # Long signal: COF is cheap (negative z-score) and liquidity is normal
                long_condition = (
                    (self.cof_data['cof_deviation_zscore'] < -entry_threshold) 
                )
                
                # Short signal: COF is expensive (positive z-score) and liquidity is normal
                short_condition = (
                    (self.cof_data['cof_deviation_zscore'] > entry_threshold) 
                )
            else:
                # Long signal: COF is cheap (negative z-score) and liquidity is normal
                long_condition = (
                    (self.cof_data['cof_deviation_zscore'] < -entry_threshold) &
                    (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
                )
                
                # Short signal: COF is expensive (positive z-score) and liquidity is normal
                short_condition = (
                    (self.cof_data['cof_deviation_zscore'] > entry_threshold) &
                    (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
                )
            
            # Exit conditions
            long_exit_condition = (
                (self.cof_data['cof_deviation_zscore'] > -exit_threshold) &
                (self.cof_data['signal'].shift(1) == 1)  # Only exit if we were long
            )
            
            short_exit_condition = (
                (self.cof_data['cof_deviation_zscore'] < exit_threshold) &
                (self.cof_data['signal'].shift(1) == -1)  # Only exit if we were short
            )
            
            # Apply signals
            self.cof_data.loc[long_condition, 'signal'] = 1
            self.cof_data.loc[short_condition, 'signal'] = -1
            
            # Maintain positions until exit threshold is crossed
            for i in range(1, len(self.cof_data)):
                if self.cof_data['signal'].iloc[i-1] == 1:  # If we were long
                    if self.cof_data['cof_deviation_zscore'].iloc[i] <= -exit_threshold:  # Only exit if we cross exit threshold
                        self.cof_data.iloc[i, self.cof_data.columns.get_loc('signal')] = 1
                elif self.cof_data['signal'].iloc[i-1] == -1:  # If we were short
                    if self.cof_data['cof_deviation_zscore'].iloc[i] >= exit_threshold:  # Only exit if we cross exit threshold
                        self.cof_data.iloc[i, self.cof_data.columns.get_loc('signal')] = -1
            
            # Apply exit signals
            self.cof_data.loc[long_exit_condition, 'signal'] = 0
            self.cof_data.loc[short_exit_condition, 'signal'] = 0
            
            logger.info("Trading signals generated successfully using z-scores")
            
        except Exception as e:
            logger.error(f"Error generating signals: {str(e)}")
            raise
    
    def backtest(self, transaction_cost=0.0001, max_loss=50):
        """
        Backtest the trading strategy
        
        Parameters:
        -----------
        transaction_cost : float
            Transaction cost as a fraction of trade value
        max_loss : float
            Maximum loss in absolute price terms (e.g., 50 points)
        """
        try:
            # Initialize portfolio tracking
            self.positions = pd.DataFrame(index=self.cof_data.index)
            self.positions['position'] = 0
            self.positions['capital'] = self.initial_capital
            self.positions['entry_price'] = 0.0
            self.positions['exit_price'] = 0.0
            self.positions['pnl'] = 0.0
            self.positions['unrealized_pnl'] = 0.0
            self.positions['cumulative_pnl'] = 0.0
            self.positions['trade_duration'] = 0
            self.positions['exit_reason'] = ''
            
            # Track trades
            current_position = 0
            entry_price = 0
            entry_date = None
            cumulative_pnl = 0
            base_capital = self.initial_capital
            prev_price = None
            
            for i in range(1, len(self.cof_data)):
                signal = self.cof_data['signal'].iloc[i]
                price = self.cof_data['cof_actual'].iloc[i]
                current_date = self.cof_data.index[i]
                
                # Calculate daily unrealized PnL if in a position
                if current_position != 0:
                    if prev_price is not None:
                        # Calculate daily change in PnL
                        daily_pnl = current_position * (price - prev_price)
                        self.positions.iloc[i, self.positions.columns.get_loc('unrealized_pnl')] = daily_pnl
                        base_capital += daily_pnl
                    
                    # Check for stop loss using cumulative PnL
                    cumulative_unrealized_pnl = current_position * (price - entry_price)
                    if cumulative_unrealized_pnl <= -max_loss:
                        # Exit position due to stop loss
                        self.positions.iloc[i, self.positions.columns.get_loc('capital')] = base_capital
                        self.positions.iloc[i, self.positions.columns.get_loc('exit_price')] = price
                        self.positions.iloc[i, self.positions.columns.get_loc('pnl')] = cumulative_unrealized_pnl
                        cumulative_pnl += cumulative_unrealized_pnl
                        self.positions.iloc[i, self.positions.columns.get_loc('cumulative_pnl')] = cumulative_pnl
                        self.positions.iloc[i, self.positions.columns.get_loc('exit_reason')] = 'stop_loss'
                        self.positions.iloc[i, self.positions.columns.get_loc('trade_duration')] = (
                            current_date - entry_date
                        ).days
                        current_position = 0
                        entry_price = 0
                        entry_date = None
                        continue
                
                # Update position
                if signal != 0 and current_position == 0:
                    # Enter new position
                    current_position = signal
                    entry_price = price
                    entry_date = current_date
                    self.positions.iloc[i, self.positions.columns.get_loc('position')] = current_position
                    self.positions.iloc[i, self.positions.columns.get_loc('entry_price')] = entry_price
                    
                    # Apply transaction cost
                    cost = abs(current_position) * price * transaction_cost
                    base_capital -= cost
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = base_capital
                    
                elif signal == 0 and current_position != 0:
                    # Exit position
                    pnl = current_position * (price - entry_price)
                    base_capital += pnl
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = base_capital
                    self.positions.iloc[i, self.positions.columns.get_loc('exit_price')] = price
                    self.positions.iloc[i, self.positions.columns.get_loc('pnl')] = pnl
                    cumulative_pnl += pnl
                    self.positions.iloc[i, self.positions.columns.get_loc('cumulative_pnl')] = cumulative_pnl
                    self.positions.iloc[i, self.positions.columns.get_loc('exit_reason')] = 'signal'
                    self.positions.iloc[i, self.positions.columns.get_loc('trade_duration')] = (
                        current_date - entry_date
                    ).days
                    current_position = 0
                    entry_price = 0
                    entry_date = None
                    
                else:
                    # Maintain current position
                    self.positions.iloc[i, self.positions.columns.get_loc('position')] = current_position
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = base_capital
                
                prev_price = price
            
            # Save detailed trading information to CSV
            results_df = self.positions.copy()
            results_df['cof_actual'] = self.cof_data['cof_actual']
            results_df['cof_predicted'] = self.cof_data['cof_predicted']
            results_df['cof_deviation'] = self.cof_data['cof_deviation']
            results_df['cof_deviation_zscore'] = self.cof_data['cof_deviation_zscore']
            results_df.to_csv('trading_results.csv')
            logger.info("Trading results saved to trading_results.csv")
            
            # Calculate performance metrics
            self.calculate_performance_metrics()
            
            logger.info("Backtesting completed successfully")
            
        except Exception as e:
            logger.error(f"Error in backtesting: {str(e)}")
            raise
    
    def calculate_performance_metrics(self):
        """Calculate strategy performance metrics"""
        try:
            # Calculate net change in capital
            returns = self.positions['capital'].diff()
            
            # Calculate metrics
            total_return = self.positions['capital'].iloc[-1]
            sharpe_ratio = np.sqrt(52) * returns.mean() / returns.std()
            max_drawdown = (self.positions['capital'] - self.positions['capital'].cummax()).min()
            
            # Calculate win rate
            trades = self.positions['pnl'] != 0
            winning_trades = self.positions['pnl'] > 0
            losing_trades = self.positions['pnl'] < 0
            win_rate = winning_trades.sum() / trades.sum()
            
            # Store metrics
            self.metrics = {
                "num_trades": trades.sum(),
                'total_return': total_return,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate,
                'avg_win_pnl': self.positions['pnl'][winning_trades].mean(),
                'avg_loss_pnl': self.positions['pnl'][losing_trades].mean(),
                'avg_win_trade_duration': self.positions['trade_duration'][winning_trades].mean(),
                'avg_loss_trade_duration': self.positions['trade_duration'][losing_trades].mean()
            }
            
            logger.info("Performance metrics calculated successfully")
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {str(e)}")
            raise
    
    def plot_results(self):
        """Plot backtesting results"""
        try:
            plt.figure(figsize=(15, 15))
            
            # Calculate total capital including unrealized PnL
            total_capital = self.positions['capital'].copy()
            for i in range(1, len(self.positions)):
                if self.positions['position'].iloc[i] != 0:  # If in a position
                    total_capital.iloc[i] += self.positions['unrealized_pnl'].iloc[i]
            
            # Plot portfolio value
            plt.subplot(3, 1, 1)
            plt.plot(self.positions.index, total_capital, label='Portfolio Value (with Unrealized PnL)', color='blue')
            
            # Highlight trades
            for i in range(1, len(self.positions)):
                if self.positions['pnl'].iloc[i] != 0:  # If there's a trade
                    color = 'green' if self.positions['pnl'].iloc[i] > 0 else 'red'
                    plt.scatter(self.positions.index[i], total_capital.iloc[i], 
                              color=color, s=100, alpha=0.5)
            
            plt.title('Strategy Performance')
            plt.legend()
            plt.grid(True)
            
            # Plot daily mark-to-market performance
            plt.subplot(3, 1, 2)
            daily_returns = total_capital.diff()
            
            # Create separate series for positive and negative returns
            positive_returns = daily_returns.copy()
            positive_returns[positive_returns < 0] = 0
            negative_returns = daily_returns.copy()
            negative_returns[negative_returns > 0] = 0
            
            # Plot positive and negative returns separately
            plt.bar(self.positions.index, positive_returns, 
                   label='Positive Returns', color='green', alpha=0.6)
            plt.bar(self.positions.index, negative_returns, 
                   label='Negative Returns', color='red', alpha=0.6)
            
            plt.axhline(y=0, color='black', linestyle='--', alpha=0.3)
            plt.title('Daily Mark-to-Market Performance')
            plt.legend()
            plt.grid(True)
            
            # Plot positions
            plt.subplot(3, 1, 3)
            plt.plot(self.positions.index, self.positions['position'], label='Position')
            plt.title('Trading Positions')
            plt.legend()
            plt.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Print metrics
            print("\nStrategy Performance Metrics:")
            print(f"Number of Trades: {self.metrics['num_trades']}")
            print(f"Total Return: {self.metrics['total_return']:.2f}")
            print(f"Sharpe Ratio: {self.metrics['sharpe_ratio']:.2f}")
            print(f"Maximum Drawdown: {self.metrics['max_drawdown']:.2%}")
            print(f"Win Rate: {self.metrics['win_rate']:.2%}")
            print(f"Average Win PnL: {self.metrics['avg_win_pnl']:.2f}")
            print(f"Average Loss PnL: {self.metrics['avg_loss_pnl']:.2f}")
            print(f"Average Win Trade Duration: {self.metrics['avg_win_trade_duration']:.2f} days")
            print(f"Average Loss Trade Duration: {self.metrics['avg_loss_trade_duration']:.2f} days")
            
        except Exception as e:
            logger.error(f"Error plotting results: {str(e)}")
            raise

def main():
    # Load data from Excel
    data = pd.read_excel('COF_DATA.xlsx', index_col=0)
    
    # Prepare data for strategy
    cof_data = pd.DataFrame({
        'cof_actual': data['cof'],
        'cof_predicted': data['cof_predicted']
    })
    
    liquidity_data = data[['fed_funds_sofr_spread']]
    
    # Initialize and run strategy
    strategy = COFTradingStrategy(cof_data, liquidity_data)
    strategy.calculate_liquidity_stress()
    strategy.generate_signals()
    strategy.backtest()
    strategy.plot_results()

if __name__ == "__main__":
    main() 