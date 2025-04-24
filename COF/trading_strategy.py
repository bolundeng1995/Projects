import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class COFTradingStrategy:
    def __init__(self, cof_data, liquidity_data, initial_capital=1000000):
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
    
    def generate_signals(self, cof_threshold=0.02, liquidity_threshold=0.01):
        """
        Generate trading signals based on COF mispricing and liquidity indicators
        
        Parameters:
        -----------
        cof_threshold : float
            Threshold for COF deviation to trigger a signal
        liquidity_threshold : float
            Threshold for liquidity stress to trigger a signal
        """
        try:
            # Calculate COF deviation
            self.cof_data['cof_deviation'] = (
                self.cof_data['cof_actual'] - self.cof_data['cof_predicted']
            )
            
            # Calculate liquidity stress if not already done
            if 'liquidity_stress' not in self.liquidity_data.columns:
                self.calculate_liquidity_stress()
            
            # Generate signals
            self.cof_data['signal'] = 0
            
            # Long signal: COF is cheap (negative deviation) and liquidity is normal
            long_condition = (
                (self.cof_data['cof_deviation'] < -cof_threshold) &
                (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
            )
            
            # Short signal: COF is expensive (positive deviation) and liquidity is normal
            short_condition = (
                (self.cof_data['cof_deviation'] > cof_threshold) &
                (self.liquidity_data['liquidity_stress'] < liquidity_threshold)
            )
            
            self.cof_data.loc[long_condition, 'signal'] = 1
            self.cof_data.loc[short_condition, 'signal'] = -1
            
            logger.info("Trading signals generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating signals: {str(e)}")
            raise
    
    def backtest(self, transaction_cost=0.0001):
        """
        Backtest the trading strategy
        
        Parameters:
        -----------
        transaction_cost : float
            Transaction cost as a fraction of trade value
        """
        try:
            # Initialize portfolio tracking
            self.positions = pd.DataFrame(index=self.cof_data.index)
            self.positions['position'] = 0
            self.positions['capital'] = self.initial_capital
            
            # Track trades
            current_position = 0
            entry_price = 0
            
            for i in range(1, len(self.cof_data)):
                signal = self.cof_data['signal'].iloc[i]
                price = self.cof_data['futures_price'].iloc[i]
                
                # Update position
                if signal != 0 and current_position == 0:
                    # Enter new position
                    current_position = signal
                    entry_price = price
                    self.positions.iloc[i, self.positions.columns.get_loc('position')] = current_position
                    
                    # Apply transaction cost
                    cost = abs(current_position) * price * transaction_cost
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = (
                        self.positions['capital'].iloc[i-1] - cost
                    )
                    
                elif signal == 0 and current_position != 0:
                    # Exit position
                    pnl = current_position * (price - entry_price)
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = (
                        self.positions['capital'].iloc[i-1] + pnl
                    )
                    current_position = 0
                    entry_price = 0
                    
                else:
                    # Maintain current position
                    self.positions.iloc[i, self.positions.columns.get_loc('position')] = current_position
                    self.positions.iloc[i, self.positions.columns.get_loc('capital')] = (
                        self.positions['capital'].iloc[i-1]
                    )
            
            # Calculate performance metrics
            self.calculate_performance_metrics()
            
            logger.info("Backtesting completed successfully")
            
        except Exception as e:
            logger.error(f"Error in backtesting: {str(e)}")
            raise
    
    def calculate_performance_metrics(self):
        """Calculate strategy performance metrics"""
        try:
            # Calculate returns
            returns = self.positions['capital'].pct_change()
            
            # Calculate metrics
            total_return = (self.positions['capital'].iloc[-1] / self.initial_capital) - 1
            sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
            max_drawdown = (self.positions['capital'] / self.positions['capital'].cummax() - 1).min()
            
            # Calculate win rate
            trades = self.positions['position'].diff().abs() > 0
            winning_trades = (self.positions['capital'].diff() > 0) & trades
            win_rate = winning_trades.sum() / trades.sum()
            
            # Store metrics
            self.metrics = {
                'total_return': total_return,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate
            }
            
            logger.info("Performance metrics calculated successfully")
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {str(e)}")
            raise
    
    def plot_results(self):
        """Plot backtesting results"""
        try:
            plt.figure(figsize=(15, 10))
            
            # Plot portfolio value
            plt.subplot(2, 1, 1)
            plt.plot(self.positions.index, self.positions['capital'], label='Portfolio Value')
            plt.title('Strategy Performance')
            plt.legend()
            plt.grid(True)
            
            # Plot positions
            plt.subplot(2, 1, 2)
            plt.plot(self.positions.index, self.positions['position'], label='Position')
            plt.title('Trading Positions')
            plt.legend()
            plt.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Print metrics
            print("\nStrategy Performance Metrics:")
            print(f"Total Return: {self.metrics['total_return']:.2%}")
            print(f"Sharpe Ratio: {self.metrics['sharpe_ratio']:.2f}")
            print(f"Maximum Drawdown: {self.metrics['max_drawdown']:.2%}")
            print(f"Win Rate: {self.metrics['win_rate']:.2%}")
            
        except Exception as e:
            logger.error(f"Error plotting results: {str(e)}")
            raise

def main():
    # Load data from Excel
    data = pd.read_excel('COF_DATA.xlsx', index_col=0)
    
    # Prepare data for strategy
    cof_data = pd.DataFrame({
        'cof_actual': data['cof'],
        'cof_predicted': data['cof_predicted'],
        'futures_price': data['futures_price']
    })
    
    liquidity_data = data[['fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']]
    
    # Initialize and run strategy
    strategy = COFTradingStrategy(cof_data, liquidity_data)
    strategy.calculate_liquidity_stress()
    strategy.generate_signals()
    strategy.backtest()
    strategy.plot_results()

if __name__ == "__main__":
    main() 