# COF Trading Strategy Presentation Outline

## 1. Title Slide
- Title: "COF Trading Strategy: A Mean Reversion Approach"
- Your name and date

## 2. Introduction to COF
- What is Cost of Funds (COF)?
  - Definition and importance in financial markets
  - Relationship between actual and predicted COF
  - Why COF mispricing presents trading opportunities

## 3. Strategy Overview
- Key Features:
  - Mean reversion trading based on COF mispricing
  - Dynamic position sizing with doubling down capability
  - Comprehensive risk management with stop-loss protection
  - Liquidity stress consideration for trade entry
  - Detailed performance tracking and visualization
- Strategy Flow Diagram

## 4. Signal Generation
- Trading Signal Components:
  - COF deviation calculation
  - Z-score thresholds for entry/exit
  - Liquidity stress indicators
- Example chart showing signal generation
- Entry/Exit conditions:
  - Long: COF deviation z-score < -entry_threshold
  - Short: COF deviation z-score > entry_threshold
  - Optional: Liquidity stress below threshold

## 5. Position Management
- Dynamic Sizing Approach:
  - Initial position size: 1
  - Doubling down when z-score exceeds double_threshold
  - Maximum position size: max_position_size
- Stop-loss: max_loss in absolute terms
- Visual example of position management

## 6. Risk Management
- Stop-loss Implementation:
  - Absolute loss threshold
  - Position-specific risk management
- Position Sizing Rules:
  - Initial sizing
  - Doubling down conditions
  - Maximum position limits
- Liquidity Stress Consideration:
  - Impact on trade entry
  - Risk mitigation
- Risk Metrics Tracking:
  - Maximum drawdown
  - Win rate
  - Average win/loss PnL

## 7. Performance Metrics
- Key Metrics:
  - Total Return
  - Sharpe Ratio
  - Maximum Drawdown
  - Win Rate
  - Average Win/Loss PnL
  - Average Trade Duration
- Visual Charts:
  - Portfolio value over time
  - Daily mark-to-market performance
  - Trading positions

## 8. Backtest Results
- Portfolio Performance:
  - Value over time
  - Daily mark-to-market
  - Position visualization
- Trade Analysis:
  - Duration statistics
  - Win/loss distribution
  - Entry/exit reasons

## 9. Strategy Parameters
- Signal Generation:
  - entry_threshold: 2.0 (default)
  - exit_threshold: 0.5 (default)
  - liquidity_threshold: 0.01 (default)
- Position Management:
  - max_loss: 50 (default)
  - double_threshold: 3.0 (default)
  - max_position_size: 2 (default)
- Transaction Costs:
  - transaction_cost: 0.0001 (default)

## 10. Conclusion
- Strategy Summary:
  - Key features and benefits
  - Performance highlights
- Future Improvements:
  - Potential enhancements
  - Additional indicators
  - Risk management refinements
- Q&A Session

## Technical Requirements
- Python 3.7+
- Required packages:
  - pandas
  - numpy
  - matplotlib
  - logging

## Data Requirements
- COF actual values
- COF predicted values
- Liquidity indicators (e.g., fed_funds_sofr_spread)

## Output Files
- trading_results.csv:
  - Position sizes
  - Entry/exit prices
  - PnL
  - Trade duration
  - Entry/exit reasons
  - COF metrics 