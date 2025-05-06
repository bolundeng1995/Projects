# Technical Appendix: SPX COF Analysis

## Mathematical Foundations

### Spline Regression Model
The strategy uses a monotonic spline regression model to predict COF levels:

\[
\begin{aligned}
COF_{\text{predicted}} &= S(\text{CFTC}) + \beta \cdot \text{Liquidity}
\end{aligned}
\]

Where:
- \(S(\text{CFTC})\) is the smoothing spline function of CFTC positions
- \(\text{Liquidity}\) is the Fed Funds-SOFR spread
- \(\beta\) is the liquidity coefficient

### Spline Fitting
The smoothing spline minimizes:

\[
\begin{aligned}
\sum_{i=1}^n (y_i - S(x_i))^2 + \lambda \int S''(x)^2 \, dx
\end{aligned}
\]

Where:
- \(\lambda\) is the smoothing parameter
- \(S''(x)\) is the second derivative of the spline
- \(n\) is the number of data points

### Monotonicity Constraint
To ensure monotonicity, we enforce:

\[
\begin{aligned}
\frac{dS(x)}{dx} &\geq 0 \quad \forall x
\end{aligned}
\]

This is implemented through:
1. Proper X-y pairing during sorting
2. Monotonic spline construction
3. Validation checks

### Cross-Validation
For each smoothing parameter \(\lambda\), we compute:

\[
\begin{aligned}
CV(\lambda) &= \frac{1}{n} \sum_{i=1}^n (y_i - S_{-i}(x_i))^2
\end{aligned}
\]

Where:
- \(S_{-i}\) is the spline fit without the \(i\)th observation
- \(n\) is the number of folds (10)

### Rolling Window Analysis
For each window \(w\) of size \(k\):

\[
\begin{aligned}
R^2_w &= 1 - \frac{\sum_{i=1}^k (y_i - \hat{y}_i)^2}{\sum_{i=1}^k (y_i - \bar{y}_w)^2}
\end{aligned}
\]

Where:
- \(\hat{y}_i\) is the predicted value
- \(\bar{y}_w\) is the mean of the window
- \(k\) is the window size (52 weeks)

## Implementation Details

### Data Processing
```python
def process_data(data):
    # Forward fill missing values
    data = data.ffill()
    
    # Sort by date
    data = data.sort_index()
    
    # Create X-y pairs for proper sorting
    pairs = list(zip(data['cftc_positions'], data['cof']))
    pairs.sort(key=lambda x: x[0])
    
    # Validate monotonicity
    for i in range(1, len(pairs)):
        if pairs[i][1] < pairs[i-1][1]:
            raise ValueError("Data violates monotonicity constraint")
    
    return data
```

### Spline Fitting
```python
def fit_spline(X, y, smoothing_param):
    # Create pairs and sort
    pairs = list(zip(X, y))
    pairs.sort(key=lambda x: x[0])
    X_sorted = np.array([x for x, _ in pairs])
    y_sorted = np.array([y for _, y in pairs])
    
    # Validate monotonicity
    if not is_monotonic(y_sorted):
        raise ValueError("Data violates monotonicity constraint")
    
    # Fit spline
    spline = make_smoothing_spline(X_sorted, y_sorted, lam=smoothing_param)
    
    # Validate spline monotonicity
    x_test = np.linspace(X_sorted.min(), X_sorted.max(), 1000)
    y_test = spline(x_test)
    if not is_monotonic(y_test):
        raise ValueError("Spline violates monotonicity constraint")
    
    return spline
```

### Cross-Validation
```python
def cross_validate(X, y, n_splits=10):
    kf = KFold(n_splits=n_splits, shuffle=False)
    scores = []
    
    for train_idx, val_idx in kf.split(X):
        # Get training and validation data
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]
        
        # Sort training data
        train_pairs = list(zip(X_train, y_train))
        train_pairs.sort(key=lambda x: x[0])
        X_train_sorted = np.array([x for x, _ in train_pairs])
        y_train_sorted = np.array([y for _, y in train_pairs])
        
        # Sort validation data
        val_pairs = list(zip(X_val, y_val))
        val_pairs.sort(key=lambda x: x[0])
        X_val_sorted = np.array([x for x, _ in val_pairs])
        y_val_sorted = np.array([y for _, y in val_pairs])
        
        # Validate monotonicity
        if not is_monotonic(y_train_sorted):
            continue
        
        # Fit and evaluate
        spline = make_smoothing_spline(X_train_sorted, y_train_sorted, lam=s)
        y_pred = spline(X_val_sorted)
        score = r2_score(y_val_sorted, y_pred)
        scores.append(score)
    
    return np.mean(scores), np.std(scores)
```

### Rolling Window Analysis
```python
def rolling_window_analysis(data, window_size=52):
    results = []
    
    for i in range(window_size, len(data)):
        window_data = data.iloc[i-window_size:i+1]
        
        # Prepare data
        X = window_data['cftc_positions']
        y = window_data['cof']
        
        # Create pairs and sort
        pairs = list(zip(X, y))
        pairs.sort(key=lambda x: x[0])
        X_sorted = np.array([x for x, _ in pairs])
        y_sorted = np.array([y for _, y in pairs])
        
        # Validate monotonicity
        if not is_monotonic(y_sorted):
            continue
        
        # Find optimal smoothing
        best_s = find_optimal_smoothing(X_sorted, y_sorted)
        
        # Fit spline and predict
        spline = fit_spline(X_sorted, y_sorted, best_s)
        y_pred = spline(X_sorted)
        
        # Calculate metrics
        r2 = r2_score(y_sorted, y_pred)
        mse = mean_squared_error(y_sorted, y_pred)
        
        results.append({
            'date': data.index[i],
            'cof_actual': y_sorted[-1],
            'cof_predicted': y_pred[-1],
            'smoothing': best_s,
            'r2': r2,
            'mse': mse
        })
    
    return pd.DataFrame(results)
```

### Monotonicity Validation
```python
def is_monotonic(y):
    """Check if array is monotonically increasing."""
    return np.all(np.diff(y) >= 0)
```

## Performance Metrics

### Model Performance
1. R² Score:
$$R^2 = 1 - \frac{\sum_{i=1}^n (y_i - \hat{y}_i)^2}{\sum_{i=1}^n (y_i - \bar{y})^2}$$

2. Mean Squared Error:
$$MSE = \frac{1}{n} \sum_{i=1}^n (y_i - \hat{y}_i)^2$$

3. Monotonicity Score:
$$M = \frac{1}{n-1} \sum_{i=1}^{n-1} \mathbb{1}(\hat{y}_{i+1} \geq \hat{y}_i)$$

### Cross-Validation Metrics
1. Average R² across folds
2. Standard deviation of R² scores
3. Optimal smoothing parameter
4. Model stability metrics

## Optimization

### Smoothing Parameter Selection
```python
def find_optimal_smoothing(X, y):
    smoothing_factors = np.logspace(4, 7, 30)
    cv_scores = []
    cv_stds = []
    
    for s in smoothing_factors:
        mean_score, std_score = cross_validate(X, y, s)
        cv_scores.append(mean_score)
        cv_stds.append(std_score)
    
    # Find best smoothing parameter
    best_s_idx = np.argmax(cv_scores)
    best_s = smoothing_factors[best_s_idx]
    
    # Validate stability
    if cv_stds[best_s_idx] > 0.1:
        warnings.warn("High cross-validation variance")
    
    return best_s
```

### Window Size Selection
- 52-week window (1 year of trading weeks)
- Adaptive smoothing per window
- Proper X-y pairing
- Temporal order preservation

## Monitoring and Maintenance

### Key Metrics to Monitor
1. Model Performance
   - R² scores
   - Smoothing parameters
   - Prediction accuracy
   - Cross-validation stability

2. Data Quality
   - Missing values
   - Temporal ordering
   - X-y pairing
   - Monotonicity

3. System Performance
   - Computational speed
   - Memory usage
   - Error rates
   - Stability

### Maintenance Schedule
1. Daily
   - Data validation
   - Error checking
   - Performance monitoring

2. Weekly
   - Model retraining
   - Parameter review
   - Performance analysis

3. Monthly
   - Full system review
   - Documentation updates
   - Optimization

## Future Enhancements

### Planned Improvements
1. Algorithm Enhancements
   - Alternative smoothing methods
   - Enhanced CV strategies
   - Additional metrics
   - Real-time processing

2. Technical Infrastructure
   - Automated testing
   - Performance monitoring
   - Error tracking
   - Documentation

3. Analysis Tools
   - Advanced visualizations
   - Real-time monitoring
   - Automated reporting
   - Interactive dashboards

---

*This technical appendix provides detailed information about the spline regression implementation and mathematical foundations. For more information, please refer to the main documentation.* 