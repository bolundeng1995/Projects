import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Optional, Tuple
import logging
from spx_cof_analysis import SPXCOFAnalyzer
from trading_strategy import COFTradingStrategy

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class COFPortfolioAnalyzer:
    """A class to analyze multiple COF terms and their portfolio combination."""
    
    def __init__(self, cof_terms: List[str] = ["1Y COF", "YE COF", "1Y2Y COF"]):
        """Initialize the portfolio analyzer.
        
        Args:
            cof_terms (List[str]): List of COF terms to analyze
        """
        self.cof_terms = cof_terms
        self.analyzers = {}
        self.strategies = {}
        self.performance_metrics = {}
        self.portfolio_metrics = None
        
    def load_data(self, file_path: str = 'COF_DATA.xlsx') -> None:
        """Load data and initialize analyzers for each COF term.
        
        Args:
            file_path (str): Path to the Excel file containing the data
        """
        try:
            # Load data from Excel
            data = pd.read_excel(file_path, sheet_name="Data", index_col=0)
            data = data.ffill()
            
            # Multiply fed_funds_sofr_spread by -1
            data['fed_funds_sofr_spread'] = data['fed_funds_sofr_spread'] * -1
            
            # Sort data
            data = data.sort_index()
            
            # Initialize analyzers and strategies for each COF term
            for cof_term in self.cof_terms:
                # Initialize analyzer
                analyzer = SPXCOFAnalyzer(cof_term=cof_term)
                analyzer.data = data.copy()
                analyzer.train_model()
                self.analyzers[cof_term] = analyzer
                
                # Initialize strategy
                cof_data = pd.DataFrame({
                    f'{cof_term}_actual': data[cof_term],
                    f'{cof_term}_predicted': analyzer.model_results['cof_predicted']
                })
                
                liquidity_data = data[['fed_funds_sofr_spread']]
                
                strategy = COFTradingStrategy(
                    cof_data=cof_data,
                    liquidity_data=liquidity_data,
                    cof_term=cof_term
                )
                strategy.calculate_liquidity_stress()
                self.strategies[cof_term] = strategy
                
            logger.info("Data loaded successfully for all COF terms")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
            
    def analyze_strategies(self, param_grid: Dict[str, List[float]]) -> pd.DataFrame:
        """Analyze performance of individual strategies.
        
        Args:
            param_grid (Dict[str, List[float]]): Parameter grid for optimization
            
        Returns:
            pd.DataFrame: Performance metrics for all strategies
        """
        try:
            metrics_list = []
            
            for cof_term, strategy in self.strategies.items():
                logger.info(f"Analyzing {cof_term}")
                
                # Run grid search
                results = strategy.grid_search(param_grid)
                
                # Get best parameters
                best_params = results.iloc[0]
                
                # Run strategy with best parameters
                strategy.reset_strategy(
                    strategy.cof_data,
                    strategy.liquidity_data
                )
                strategy.generate_signals(
                    entry_threshold=best_params['entry_threshold'],
                    exit_threshold=best_params['exit_threshold']
                )
                strategy.backtest()
                strategy.calculate_performance_metrics()
                
                # Store metrics
                metrics = strategy.trade_tracker.metrics.copy()
                metrics['cof_term'] = cof_term
                metrics['entry_threshold'] = best_params['entry_threshold']
                metrics['exit_threshold'] = best_params['exit_threshold']
                metrics_list.append(metrics)
                
            # Create performance comparison table
            performance_table = pd.DataFrame(metrics_list)
            performance_table = performance_table.set_index('cof_term')
            
            # Format metrics
            float_columns = ['total_return', 'sharpe_ratio', 'max_drawdown', 
                           'win_rate', 'avg_win_pnl', 'avg_loss_pnl',
                           'win_loss_ratio', 'avg_trade_duration',
                           'entry_threshold', 'exit_threshold']
            performance_table[float_columns] = performance_table[float_columns].round(4)
            
            self.performance_metrics = performance_table
            return performance_table
            
        except Exception as e:
            logger.error(f"Error analyzing strategies: {str(e)}")
            raise
            
    def combine_portfolio(self, weights: Optional[Dict[str, float]] = None) -> Dict[str, float]:
        """Combine individual strategies into a portfolio.
        
        Args:
            weights (Optional[Dict[str, float]]): Dictionary of weights for each strategy.
                If None, equal weights will be used.
                
        Returns:
            Dict[str, float]: Portfolio performance metrics
        """
        try:
            if weights is None:
                # Use equal weights
                weights = {cof_term: 1.0/len(self.cof_terms) for cof_term in self.cof_terms}
            
            # Initialize portfolio results
            portfolio_capital = pd.Series(0.0, index=self.strategies[self.cof_terms[0]].trade_tracker.positions.index)
            portfolio_returns = pd.Series(0.0, index=portfolio_capital.index)
            
            # Combine individual strategy results
            for cof_term, strategy in self.strategies.items():
                weight = weights[cof_term]
                strategy_returns = strategy.trade_tracker.positions['capital'].diff()
                portfolio_returns += weight * strategy_returns
                
            # Calculate portfolio value
            portfolio_capital = portfolio_returns.cumsum() + 1.0  # Start with 1.0
            
            # Calculate portfolio metrics
            self.portfolio_metrics = {
                'total_return': portfolio_capital.iloc[-1] - 1.0,
                'sharpe_ratio': np.sqrt(52) * portfolio_returns.mean() / portfolio_returns.std(),
                'max_drawdown': (portfolio_capital - portfolio_capital.cummax()).min(),
                'volatility': portfolio_returns.std() * np.sqrt(52),
                'annualized_return': portfolio_returns.mean() * 52
            }
            
            return self.portfolio_metrics
            
        except Exception as e:
            logger.error(f"Error combining portfolio: {str(e)}")
            raise
            
    def predict_fair_value(self, cof_term: str, new_cftc_positions: float) -> Dict[str, float]:
        """Predict fair value COF for given CFTC positions.
        
        Args:
            cof_term (str): The COF term to analyze
            new_cftc_positions (float): New CFTC positions value
            
        Returns:
            Dict[str, float]: Dictionary containing:
                - predicted_cof: Predicted fair value
                - current_cof: Current actual COF
                - deviation: Difference between predicted and current
                - deviation_zscore: Z-score of the deviation
                - signal: Trading signal (-1, 0, 1)
        """
        try:
            analyzer = self.analyzers[cof_term]
            strategy = self.strategies[cof_term]
            
            # Get current data
            current_data = analyzer.data.iloc[-1].copy()
            current_cftc = current_data['cftc_positions']
            current_cof = current_data[cof_term]
            current_liquidity = current_data['fed_funds_sofr_spread']
            
            # Create spline prediction
            spline = analyzer.model_results['spline'].iloc[-1]
            predicted_cof = spline(new_cftc_positions) + current_liquidity
            
            # Calculate deviation
            deviation = predicted_cof - current_cof
            
            # Calculate z-score of deviation using rolling window
            window_size = 52  # 1 year of trading weeks
            historical_deviations = analyzer.model_results['cof_deviation']
            rolling_mean = historical_deviations.rolling(window=window_size, min_periods=10).mean().iloc[-1]
            rolling_std = historical_deviations.rolling(window=window_size, min_periods=10).std().iloc[-1]
            deviation_zscore = (deviation - rolling_mean) / rolling_std if rolling_std != 0 else 0
            
            # Generate signal based on z-score
            if deviation_zscore < -strategy.trade_tracker.metrics['entry_threshold']:
                signal = 1  # Long signal
            elif deviation_zscore > strategy.trade_tracker.metrics['entry_threshold']:
                signal = -1  # Short signal
            else:
                signal = 0  # No signal
                
            return {
                'predicted_cof': predicted_cof,
                'current_cof': current_cof,
                'deviation': deviation,
                'deviation_zscore': deviation_zscore,
                'signal': signal
            }
            
        except Exception as e:
            logger.error(f"Error predicting fair value: {str(e)}")
            raise
            
    def plot_results(self) -> None:
        """Plot results for individual strategies and portfolio."""
        try:
            # Create figure with subplots
            fig, axes = plt.subplots(2, 2, figsize=(20, 15))
            fig.suptitle('COF Portfolio Analysis', fontsize=16)
            
            # Plot 1: Individual Strategy Performance
            ax1 = axes[0, 0]
            for cof_term, strategy in self.strategies.items():
                ax1.plot(strategy.trade_tracker.positions.index,
                        strategy.trade_tracker.positions['capital'],
                        label=f'{cof_term} Strategy')
            ax1.set_title('Individual Strategy Performance')
            ax1.legend()
            ax1.grid(True)
            
            # Plot 2: Strategy Correlations
            ax2 = axes[0, 1]
            strategy_returns = pd.DataFrame({
                cof_term: strategy.trade_tracker.positions['capital'].diff()
                for cof_term, strategy in self.strategies.items()
            })
            sns.heatmap(strategy_returns.corr(),
                       annot=True,
                       cmap='RdYlGn',
                       ax=ax2)
            ax2.set_title('Strategy Return Correlations')
            
            # Plot 3: Performance Metrics Comparison
            ax3 = axes[1, 0]
            metrics_to_plot = ['total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
            self.performance_metrics[metrics_to_plot].plot(kind='bar', ax=ax3)
            ax3.set_title('Strategy Performance Metrics')
            ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # Plot 4: Rolling Portfolio Metrics
            ax4 = axes[1, 1]
            portfolio_returns = pd.Series(0.0, index=self.strategies[self.cof_terms[0]].trade_tracker.positions.index)
            for cof_term, strategy in self.strategies.items():
                portfolio_returns += strategy.trade_tracker.positions['capital'].diff() / len(self.cof_terms)
            
            rolling_sharpe = (portfolio_returns.rolling(window=52)
                            .mean() / portfolio_returns.rolling(window=52)
                            .std() * np.sqrt(52))
            ax4.plot(rolling_sharpe.index, rolling_sharpe, label='Rolling Sharpe Ratio')
            ax4.set_title('Rolling Portfolio Sharpe Ratio')
            ax4.legend()
            ax4.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Print performance metrics
            self._print_metrics()
            
        except Exception as e:
            logger.error(f"Error plotting results: {str(e)}")
            raise
            
    def _print_metrics(self) -> None:
        """Print performance metrics for individual strategies and portfolio."""
        print("\nIndividual Strategy Performance:")
        print(self.performance_metrics)
        
        if self.portfolio_metrics:
            print("\nPortfolio Performance:")
            for metric, value in self.portfolio_metrics.items():
                print(f"{metric}: {value:.4f}")

def main():
    """Main function to run the portfolio analysis."""
    # Initialize portfolio analyzer
    portfolio = COFPortfolioAnalyzer()
    
    # Load data
    portfolio.load_data()
    
    # Define parameter grid for optimization
    param_grid = {
        'entry_threshold': [1.5, 2.0, 2.5, 3.0],
        'exit_threshold': [0.5, 1.0, 1.5, 2.0]
    }
    
    # Analyze individual strategies
    performance_table = portfolio.analyze_strategies(param_grid)
    print("\nStrategy Performance Comparison:")
    print(performance_table)
    
    # Combine into portfolio
    portfolio_metrics = portfolio.combine_portfolio()
    print("\nPortfolio Performance:")
    print(portfolio_metrics)
    
    # Example of fair value prediction
    for cof_term in portfolio.cof_terms:
        # Get current CFTC positions
        current_cftc = portfolio.analyzers[cof_term].data['cftc_positions'].iloc[-1]
        
        # Predict fair value with 10% higher CFTC positions
        new_cftc = current_cftc * 1.1
        prediction = portfolio.predict_fair_value(cof_term, new_cftc)
        
        print(f"\nFair Value Prediction for {cof_term}:")
        print(f"Current CFTC: {current_cftc:.2f}")
        print(f"New CFTC: {new_cftc:.2f}")
        print(f"Predicted COF: {prediction['predicted_cof']:.4f}")
        print(f"Current COF: {prediction['current_cof']:.4f}")
        print(f"Deviation: {prediction['deviation']:.4f}")
        print(f"Deviation Z-score: {prediction['deviation_zscore']:.4f}")
        print(f"Signal: {prediction['signal']}")
    
    # Plot results
    portfolio.plot_results()

if __name__ == "__main__":
    main() 