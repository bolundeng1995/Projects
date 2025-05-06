# SPX Cost of Financing (COF) Analysis

This project analyzes the relationship between SPX Cost of Financing (COF) and CFTC positions using a monotonic spline regression approach with time series cross-validation.

## Technical Overview

### Core Algorithm
- Monotonic spline regression using `make_smoothing_spline`
- Time series cross-validation (10-fold) without data shuffling
- Rolling window analysis (52-week window)
- Proper X-y pairing during sorting operations
- Fed Funds-SOFR spread integration for liquidity analysis

### Key Technical Features

#### 1. Spline Fitting
```python
# Optimal smoothing selection via cross-validation
best_s, _, _ = self._find_optimal_smoothing(
    X_sorted['cftc_positions'].values,
    y_sorted.values,
    n_splits=10
)

# Create spline with optimal smoothing
spline = make_smoothing_spline(X_sorted, y_sorted, lam=best_s)
```
- Uses `make_smoothing_spline` from scipy.interpolate
- Ensures proper pairing of X (CFTC positions) and y (COF) values
- Maintains monotonicity in the relationship
- Adaptive smoothing parameter per window

#### 2. Cross-Validation
```python
# Time series cross-validation
kf = KFold(n_splits=10, shuffle=False)

# Proper X-y pairing during sorting
pairs = list(zip(X_train, y_train))
pairs.sort(key=lambda x: x[0])
```
- 10-fold time series CV without shuffling
- Preserves temporal order of data
- Proper handling of X-y pairs during sorting
- R² score for model evaluation

#### 3. Rolling Window Analysis
```python
# 52-week rolling window
window_size = 52  # 1 year of trading weeks

# Adaptive smoothing per window
best_s, _, _ = self._find_optimal_smoothing(
    X_sorted['cftc_positions'].values,
    y_sorted.values,
    n_splits=min(10, window_size//10)
)
```
- 52-week window size for analysis
- Adaptive smoothing parameter selection
- Fed Funds-SOFR spread integration
- Results saved to Excel for analysis

## Implementation Details

### Data Requirements
Excel file ('COF_DATA.xlsx') must contain:
- CFTC positions (numeric)
- COF values (1Y COF by default)
- Fed Funds-SOFR spread (numeric)

### Dependencies
```python
numpy>=1.21.0
pandas>=1.3.0
matplotlib>=3.4.0
seaborn>=0.11.0
scipy>=1.7.0
statsmodels>=0.13.0
scikit-learn>=0.24.0
```

### Usage

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Prepare data in 'COF_DATA.xlsx'

3. Run analysis:
```python
python spx_cof_analysis.py
```

4. View results:
- Model results: 'Model_Results.xlsx'
- Updated data: 'COF_DATA.xlsx'
- Visualizations: Displayed during execution

### Output Files

1. Model Results ('Model_Results.xlsx'):
   - Predicted COF values
   - R² scores
   - Optimal smoothing parameters
   - COF deviations

2. Visualizations:
   - Smoothing trade-off analysis
   - Actual vs predicted COF
   - COF deviations
   - Liquidity analysis plots

## Technical Notes

### Data Handling
- Proper X-y pairing during sorting operations
- Temporal order preservation in cross-validation
- Monotonicity maintenance in spline fitting
- Handling of missing values via forward fill

### Model Assumptions
- Monotonic relationship between CFTC positions and COF
- Stationarity of the relationship within each window
- Sufficient data points for reliable cross-validation
- Proper liquidity conditions for valid predictions

### Performance Considerations
- Results sensitive to window size (52 weeks)
- Smoothing parameter selection via cross-validation
- Proper handling of time series dependencies
- Integration of liquidity indicators

## Future Enhancements

### Planned Improvements
1. Alternative smoothing methods
2. Enhanced cross-validation strategies
3. Additional liquidity metrics
4. Real-time monitoring capabilities

### Potential Extensions
1. Machine learning integration
2. Automated parameter optimization
3. Advanced visualization tools
4. Real-time data processing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 