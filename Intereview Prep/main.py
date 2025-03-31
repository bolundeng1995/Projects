"""
Main execution script for the market-neutral strategy.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
from datetime import datetime
import os

# Import project modules
from src.data_loader import DataLoader
from src.factors.value import ValueFactors
from src.factors.momentum import MomentumFactors
from src.factors.quality import QualityFactors
from src.backtest.engine import BacktestEngine
from src.backtest.metrics import PerformanceMetrics
from src.portfolio.construction import PortfolioConstructor
from src.portfolio.optimization import PortfolioOptimizer
from src.visualization.performance import PerformanceVisualizer

# Import configuration
from config import DATA_CONFIG, FACTOR_CONFIG, PORTFOLIO_CONFIG, BACKTEST_CONFIG, VIZ_CONFIG

def load_data():
    """Load and prepare market data."""
    print("Loading market data...")
    data_loader = DataLoader(DATA_CONFIG)
    price_data, fundamental_data = data_loader.prepare_factor_data()
    print(f"Loaded data for {len(price_data.index.get_level_values('Ticker').unique())} stocks.")
    return price_data, fundamental_data

def calculate_factors(price_data, fundamental_data):
    """Calculate factor values for all stocks."""
    print("Calculating factors...")
    
    # Calculate value factors
    if FACTOR_CONFIG['value']['enabled']:
        print("Computing value factors...")
        value_calculator = ValueFactors(price_data, fundamental_data)
        value_factor = value_calculator.combine_value_factors(
            weights=FACTOR_CONFIG['value']['parameters']
        )
    else:
        value_factor = None
    
    # Calculate momentum factors
    if FACTOR_CONFIG['momentum']['enabled']:
        print("Computing momentum factors...")
        momentum_calculator = MomentumFactors(price_data)
        momentum_factor = momentum_calculator.combine_momentum_factors(
            weights=FACTOR_CONFIG['momentum']['parameters']
        )
    else:
        momentum_factor = None
    
    # Calculate quality factors
    if FACTOR_CONFIG['quality']['enabled']:
        print("Computing quality factors...")
        quality_calculator = QualityFactors(price_data, fundamental_data)
        quality_factor = quality_calculator.combine_quality_factors(
            weights=FACTOR_CONFIG['quality']['parameters']
        )
    else:
        quality_factor = None
    
    # Combine all factors
    factors = {}
    if value_factor is not None:
        factors['value'] = value_factor
    if momentum_factor is not None:
        factors['momentum'] = momentum_factor
    if quality_factor is not None:
        factors['quality'] = quality_factor
    
    # Create composite factor
    if len(factors) > 0:
        composite_factor = pd.DataFrame(0, index=list(factors.values())[0].index, 
                                     columns=list(factors.values())[0].columns)
        
        for factor_name, factor_data in factors.items():
            factor_weight = FACTOR_CONFIG[factor_name]['weight']
            composite_factor += factor_data * factor_weight
            
        factors['composite'] = composite_factor
    
    print(f"Calculated {len(factors)} factors.")
    return factors

def run_backtest(price_data, factors, args):
    """Run backtest for selected factor."""
    print(f"Running backtest for {args.factor} factor...")
    
    # Configure backtest parameters
    backtest_config = BACKTEST_CONFIG.copy()
    if args.start_date:
        backtest_config['start_date'] = args.start_date
    if args.end_date:
        backtest_config['end_date'] = args.end_date
    
    # Initialize backtest engine
    backtest_engine = BacktestEngine(price_data, backtest_config)
    
    # Run backtest
    results = backtest_engine.run_backtest(
        factor_data=factors[args.factor],
        start_date=backtest_config['start_date'],
        end_date=backtest_config['end_date'],
        market_neutral=PORTFOLIO_CONFIG['market_neutral'],
        position_limits=PORTFOLIO_CONFIG['position_limits']
    )
    
    print("Backtest complete.")
    print(f"Total Return: {results['performance_metrics']['total_return']:.2%}")
    print(f"Annual Return: {results['performance_metrics']['annual_return']:.2%}")
    print(f"Annual Volatility: {results['performance_metrics']['annual_volatility']:.2%}")
    print(f"Sharpe Ratio: {results['performance_metrics']['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {results['performance_metrics']['max_drawdown']:.2%}")
    
    return results

def analyze_performance(price_data, factors, results, args):
    """Analyze strategy performance and generate visualizations."""
    print("Analyzing performance...")
    
    # Initialize performance metrics calculator
    metrics_calculator = PerformanceMetrics(price_data)
    
    # Calculate factor returns
    factor_returns = metrics_calculator.calculate_factor_returns(
        factor_data=factors[args.factor],
        n_quantiles=5,
        holdings_period=20
    )
    
    # Calculate information coefficient
    ic_series = metrics_calculator.calculate_information_coefficient(
        factor_data=factors[args.factor],
        forward_period=20
    )
    
    # Calculate factor decay
    factor_decay = metrics_calculator.calculate_factor_decay(
        factor_data=factors[args.factor],
        max_periods=60,
        step=5
    )
    
    # Initialize performance visualizer
    visualizer = PerformanceVisualizer(figsize=VIZ_CONFIG['figsize'])
    
    # Calculate daily returns
    daily_returns = results['portfolio_value'].pct_change(fill_method=None).dropna()
    
    # Plot cumulative returns
    cum_returns_fig = visualizer.plot_cumulative_returns(
        returns=daily_returns,
        title=f"{args.factor.capitalize()} Factor Strategy Cumulative Returns"
    )
    
    # Plot drawdowns
    drawdowns_fig = visualizer.plot_drawdowns(
        returns=daily_returns,
        top_n=5,
        title=f"{args.factor.capitalize()} Factor Strategy Drawdowns"
    )
    
    # Plot monthly returns
    monthly_returns_fig = visualizer.plot_monthly_returns(
        returns=daily_returns,
        title=f"{args.factor.capitalize()} Factor Strategy Monthly Returns"
    )
    
    # Save figures if required
    if VIZ_CONFIG['save_plots']:
        save_dir = VIZ_CONFIG['plot_dir']
        os.makedirs(save_dir, exist_ok=True)
        
        cum_returns_fig.savefig(f"{save_dir}/{args.factor}_cumulative_returns.png", dpi=VIZ_CONFIG['dpi'])
        drawdowns_fig.savefig(f"{save_dir}/{args.factor}_drawdowns.png", dpi=VIZ_CONFIG['dpi'])
        monthly_returns_fig.savefig(f"{save_dir}/{args.factor}_monthly_returns.png", dpi=VIZ_CONFIG['dpi'])
        
        print(f"Saved plots to {save_dir}")
    
    # Show figures
    plt.show()
    
    return {
        'factor_returns': factor_returns,
        'ic_series': ic_series,
        'factor_decay': factor_decay
    }

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run market-neutral strategy backtests.')
    parser.add_argument('--factor', type=str, default='momentum', choices=['value', 'momentum', 'quality', 'composite'],
                        help='Factor to use for backtesting')
    parser.add_argument('--start-date', type=str, help='Backtest start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='Backtest end date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Load data
    price_data, fundamental_data = load_data()
    
    # Calculate factors
    factors = calculate_factors(price_data, fundamental_data)
    
    # Run backtest
    results = run_backtest(price_data, factors, args)
    
    # Analyze performance
    analysis = analyze_performance(price_data, factors, results, args)
    
    print("Analysis complete.")
    return results, analysis

if __name__ == "__main__":
    main() 