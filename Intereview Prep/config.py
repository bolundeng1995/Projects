"""
Configuration parameters for the market-neutral strategy.
"""

import os
from datetime import datetime, timedelta

# Data Parameters
DATA_CONFIG = {
    'start_date': '2018-01-01',
    'end_date': datetime.now().strftime('%Y-%m-%d'),
    'data_dir': 'data/',
    'tickers_source': 'sp500',  # 'sp500' or 'custom'
    'custom_tickers': None,  # List of custom tickers if tickers_source is 'custom'
}

# Factor Parameters
FACTOR_CONFIG = {
    'value': {
        'enabled': False,
        'weight': 0.4,
        'parameters': {
            'price_to_book': 0.4,
            'price_to_earnings': 0.4,
            'ev_to_ebitda': 0.2,
        }
    },
    'momentum': {
        'enabled': True,
        'weight': 0.3,
        'parameters': {
            'price_momentum': 0.5,
            'rsi': 0.3,
            'mean_reversion': 0.2,
            'lookback_months': 12,
            'skip_months': 1,
            'rsi_window': 14,
        }
    },
    'quality': {
        'enabled': False,
        'weight': 0.3,
        'parameters': {
            'return_on_equity': 0.4,
            'earnings_stability': 0.4,
            'debt_to_equity': 0.2,
        }
    }
}

# Portfolio Construction Parameters
PORTFOLIO_CONFIG = {
    'market_neutral': False,
    'rebalance_frequency': 'quarterly',  # 'daily', 'weekly', 'monthly', 'quarterly'
    'position_limits': {
        'max_position': 0.05,  # Maximum position size
    },
    'sector_neutral': True,
    'optimization_method': 'mean_variance',  # 'mean_variance', 'risk_parity', 'max_sharpe'
    'risk_aversion': 1.0,  # Risk aversion parameter for mean-variance optimization
}

# Backtest Parameters
BACKTEST_CONFIG = {
    'start_date': '2020-01-01',  # Default backtest start date
    'end_date': datetime.now().strftime('%Y-%m-%d'),  # Default backtest end date
    'transaction_cost': 0.0000,  # 0 basis points per trade (one-way)
    'benchmark': 'SPY',  # Benchmark for performance comparison
}

# Visualization Parameters
VIZ_CONFIG = {
    'figsize': (12, 8),
    'dpi': 100,
    'save_plots': True,
    'plot_dir': 'reports/figures/',
}

# Ensure necessary directories exist
def ensure_directories():
    """Create necessary directories if they don't exist."""
    dirs = [
        'data/raw',
        'data/processed',
        'reports/figures',
        'notebooks',
    ]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)

ensure_directories() 