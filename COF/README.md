# COF Trading Strategy

A sophisticated trading strategy based on Cost of Funds (COF) analysis, implementing mean reversion principles with dynamic position sizing and risk management.

## Overview

This strategy trades based on deviations between actual and predicted Cost of Funds (COF) levels, with the following key features:

- Mean reversion trading based on COF mispricing
- Dynamic position sizing with doubling down on extreme deviations
- Comprehensive risk management with stop-loss protection
- Liquidity stress consideration for trade entry
- Detailed performance tracking and visualization

## Strategy Components

### 1. Signal Generation
- Calculates COF deviation and its z-score using rolling windows
- Generates trading signals based on z-score thresholds
- Incorporates liquidity stress indicators for trade entry
- Implements separate entry and exit thresholds

### 2. Position Management
- Dynamic position sizing with doubling down capability
- Tracks average entry price for accurate PnL calculation
- Implements absolute stop-loss protection
- Records detailed trade information including entry/exit reasons

### 3. Performance Tracking
- Tracks daily and cumulative PnL
- Records trade duration and exit reasons
- Calculates key performance metrics:
  - Total return
  - Sharpe ratio
  - Maximum drawdown
  - Win rate
  - Average win/loss PnL
  - Average trade duration

### 4. Visualization
- Portfolio value over time
- Daily mark-to-market performance
- Trading positions visualization
- Performance metrics summary

## Usage

1. Prepare your data:
   - COF actual values
   - COF predicted values
   - Liquidity indicators (e.g., fed_funds_sofr_spread)

2. Initialize the strategy:
```python
strategy = COFTradingStrategy(cof_data, liquidity_data, initial_capital=1000000)
```

3. Generate signals:
```python
strategy.generate_signals(
    entry_threshold=2.0,    # Z-score threshold for entry
    exit_threshold=0.5,     # Z-score threshold for exit
    liquidity_threshold=0.01 # Optional liquidity stress threshold
)
```

4. Run backtest:
```python
strategy.backtest(
    transaction_cost=0.0001,  # Transaction cost as fraction
    max_loss=50,             # Maximum loss in absolute terms
    double_threshold=3.0,    # Z-score threshold for doubling down
    max_position_size=2      # Maximum position size multiplier
)
```

5. View results:
```python
strategy.plot_results()
```

## Key Parameters

### Signal Generation
- `entry_threshold`: Z-score threshold for entering positions (default: 2.0)
- `exit_threshold`: Z-score threshold for exiting positions (default: 0.5)
- `liquidity_threshold`: Optional threshold for liquidity stress (default: 0.01)

### Position Management
- `max_loss`: Maximum loss in absolute terms (default: 50)
- `double_threshold`: Z-score threshold for doubling down (default: 3.0)
- `max_position_size`: Maximum position size multiplier (default: 2)

### Transaction Costs
- `transaction_cost`: Transaction cost as a fraction of trade value (default: 0.0001)

## Output Files

- `trading_results.csv`: Detailed trade records including:
  - Position sizes
  - Entry/exit prices
  - PnL
  - Trade duration
  - Entry/exit reasons
  - COF metrics

## Performance Metrics

The strategy calculates and displays:
1. Total Return
2. Sharpe Ratio
3. Maximum Drawdown
4. Win Rate
5. Average Win/Loss PnL
6. Average Trade Duration

## Requirements

- Python 3.7+
- pandas
- numpy
- matplotlib
- logging

## Installation

```bash
pip install pandas numpy matplotlib
```

## Example

```python
# Load data
data = pd.read_excel('COF_DATA.xlsx', index_col=0)

# Prepare data
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
```

## Strategy Logic

1. **Entry Conditions**:
   - Long: COF deviation z-score < -entry_threshold
   - Short: COF deviation z-score > entry_threshold
   - Optional: Liquidity stress below threshold

2. **Exit Conditions**:
   - Signal reversal (z-score crosses exit_threshold)
   - Stop-loss triggered (absolute loss exceeds max_loss)
   - Position doubling (z-score exceeds double_threshold)

3. **Position Management**:
   - Initial position size: 1
   - Doubling down when z-score exceeds double_threshold
   - Maximum position size: max_position_size
   - Stop-loss: max_loss in absolute terms

## Performance Analysis

The strategy provides comprehensive performance analysis through:
1. Visual charts of portfolio value and positions
2. Detailed trade records in CSV format
3. Key performance metrics
4. Trade duration and win/loss analysis

## Contributing

Feel free to submit issues and enhancement requests! 