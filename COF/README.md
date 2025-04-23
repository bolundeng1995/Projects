# SPX Cost of Financing (COF) Analysis and Trading Strategy

This project implements a quantitative analysis of the S&P 500 (SPX) equity cost of financing (COF) and develops a trading strategy based on COF mispricing and liquidity indicators.

## Project Structure

```
.
├── README.md
├── requirements.txt
├── spx_cof_analysis.py
└── trading_strategy.py
```

## Setup

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Components

### 1. COF Analysis (`spx_cof_analysis.py`)
- Implements the theoretical framework for COF calculation
- Develops a linear regression model using rolling 1-year windows
- Analyzes the relationship between COF and CFTC Asset Manager positions
- Incorporates liquidity indicators (Fed Funds-SOFR spread, swap spread, JPYUSD basis)

### 2. Trading Strategy (`trading_strategy.py`)
- Implements a systematic trading strategy based on COF mispricing
- Incorporates liquidity indicators for signal generation
- Includes comprehensive backtesting framework
- Calculates key performance metrics

## Usage

1. Run the COF analysis:
```bash
python spx_cof_analysis.py
```

2. Run the trading strategy backtest:
```bash
python trading_strategy.py
```

## Data Requirements

The analysis requires the following data:
- SPX futures prices
- SPX spot prices
- Risk-free rates
- CFTC Asset Manager positions
- Liquidity indicators:
  - Fed Funds-SOFR spread
  - Swap spread
  - JPYUSD basis

## Notes

- The current implementation uses placeholder data for some components
- TODO items are marked in the code
- Additional data sources and indicators can be incorporated as needed

## License

This project is licensed under the MIT License - see the LICENSE file for details. 