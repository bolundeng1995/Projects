import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm
from sklearn.metrics import r2_score
import logging
from scipy.optimize import minimize
from scipy.interpolate import UnivariateSpline, BSpline, make_smoothing_spline, make_lsq_spline
from sklearn.model_selection import ShuffleSplit
from typing import Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SPXCOFAnalyzer:
    def __init__(self, cof_term: str = "1Y COF"):
        """Initialize the SPX COF analyzer
        
        Parameters:
        -----------
        cof_term : str, default="1Y COF"
            The COF term to analyze (e.g., "1Y COF", "3M COF", etc.)
        """
        self.data = None
        self.model_results = None
        self.cof_term = cof_term  # The value to predict or train
        
    def load_data_from_excel(self, file_path='COF_DATA.xlsx'):
        """
        Load data from Excel file
        
        Parameters:
        -----------
        file_path : str
            Path to the Excel file containing the data
        """
        try:
            # Read data from Excel
            self.data = pd.read_excel(file_path, sheet_name="Data", index_col=0)

            self.data = self.data.ffill()
            
            # Multiply fed_funds_sofr_spread by -1, and thus positive values will be associated with higher liquidity stress
            self.data['fed_funds_sofr_spread'] = self.data['fed_funds_sofr_spread'] * -1
            
            # Sort data in ascending order (oldest to latest)
            self.data = self.data.sort_index()
            
            # Ensure all required columns are present
            required_columns = [
                self.cof_term, 'cftc_positions',
                'fed_funds_sofr_spread'
            ]
            
            missing_columns = [col for col in required_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info("Data loaded successfully from Excel file")
            
        except Exception as e:
            logger.error(f"Error loading data from Excel: {str(e)}")
            raise
    
    def _quadratic_objective(self, params, X, y):
        """Objective function for quadratic regression with constraints"""
        y_pred = X @ params  # Matrix multiplication for prediction
        return np.sum((y - y_pred) ** 2)
    
    def _find_optimal_smoothing(self, X, y, n_splits=20):
        """Find optimal smoothing parameter using cross-validation"""

        
        # Define range of smoothing parameters to try - using log spacing
        smoothing_factors = np.logspace(4, 7, 30)  # 30 values from 10000 to 10000000 on log scale
        cv_scores = []
        
        # Use K-fold cross-validation without shuffling for time series
        kf = ShuffleSplit(n_splits=n_splits, test_size=0.05, random_state=1)
        
        for s in smoothing_factors:
            scores = []
            
            for train_idx, val_idx in kf.split(X):
                # Get training and validation data
                X_train, X_val = X[train_idx], X[val_idx]
                y_train, y_val = y[train_idx], y[val_idx]
                
                # Create pairs of (X, y) and sort by X
                train_pairs = list(zip(X_train, y_train))
                train_pairs.sort(key=lambda x: x[0])  # Sort by X value
                X_train_sorted = np.array([x for x, _ in train_pairs])
                y_train_sorted = np.array([y for _, y in train_pairs])
                
                # Create spline with smoothing parameter
                spline = make_smoothing_spline(X_train_sorted, y_train_sorted, lam=s)
                
                # Sort validation data the same way
                val_pairs = list(zip(X_val, y_val))
                val_pairs.sort(key=lambda x: x[0])  # Sort by X value
                X_val_sorted = np.array([x for x, _ in val_pairs])
                y_val_sorted = np.array([y for _, y in val_pairs])
                
                # Predict on validation set
                y_pred = spline(X_val_sorted)
                score = r2_score(y_val_sorted, y_pred)
                scores.append(score)
            
            mean_score = np.mean(scores)
            cv_scores.append(mean_score)
        
        # Find best smoothing parameter
        best_s_idx = np.argmax(cv_scores)
        best_s = smoothing_factors[best_s_idx]
        logger.info(f"Best smoothing parameter: {best_s:.2f} (R² = {cv_scores[best_s_idx]:.3f})")
        
        return best_s, smoothing_factors, cv_scores

    def visualize_smoothing_tradeoff(self):
        """Visualize the trade-off between smoothness and fit"""
        try:
            from scipy.interpolate import make_smoothing_spline
            
            # Sort data
            sort_idx = self.data['cftc_positions'].argsort()
            X_sorted = self.data['cftc_positions'].iloc[sort_idx].values
            y_sorted = self.data[self.cof_term].iloc[sort_idx].values
            
            # Find optimal smoothing
            best_s, smoothing_factors, cv_scores = self._find_optimal_smoothing(X_sorted, y_sorted)

            # Update the model with optimal smoothing
            self.optimal_smoothing = best_s
            # Create figure with two subplots
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Plot 1: Different smoothing levels
            smoothing_levels = [2e3, best_s, 1e8]  # low, optimal, high
            colors = ['blue', 'red', 'green']
            labels = ['Low Smoothing', 'Optimal Smoothing', 'High Smoothing']
            
            x_plot = np.linspace(X_sorted.min(), X_sorted.max(), 1000)
            
            for s, color, label in zip(smoothing_levels, colors, labels):
                # Create spline with smoothing parameter
                spline = make_smoothing_spline(X_sorted, y_sorted, lam=s)
                ax1.plot(x_plot, spline(x_plot), color=color, label=label, alpha=0.8)
            
            ax1.scatter(X_sorted, y_sorted, color='gray', alpha=0.3, label='Data Points')
            ax1.set_xlabel('CFTC Positions')
            ax1.set_ylabel(f'{self.cof_term}')
            ax1.set_title('Effect of Smoothing Parameter')
            ax1.legend()
            ax1.grid(True)
            
            # Plot 2: Cross-validation scores
            ax2.semilogx(smoothing_factors, cv_scores, 'b-', label='CV Score')
            ax2.axvline(x=best_s, color='r', linestyle='--', label=f'Optimal s={best_s:.2f}')
            ax2.set_xlabel('Smoothing Parameter (s)')
            ax2.set_ylabel('Cross-validation R² Score')
            ax2.set_title('Smoothing Parameter Selection')
            ax2.legend()
            ax2.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Update the model with optimal smoothing
            self.optimal_smoothing = best_s
            logger.info(f"Optimal smoothing parameter: {best_s:.2f}")
            
        except Exception as e:
            logger.error(f"Error in smoothing trade-off visualization: {str(e)}")
            raise

    def train_model(self, window_size=52):  # 52 trading weeks = 1 year
        """Train rolling window regression model using monotonic spline with controlled knots"""
        try:
            from scipy.interpolate import make_smoothing_spline
            results = []
            
            for i in range(window_size, len(self.data)):
                window_data = self.data.iloc[i-window_size:i+1]
                
                # Prepare data
                X = pd.DataFrame({
                    'cftc_positions': window_data['cftc_positions']
                })
                y = window_data[self.cof_term]
                
                # Create pairs of (X, y) and sort by X
                pairs = list(zip(X['cftc_positions'], y))
                pairs.sort(key=lambda x: x[0])  # Sort by X value
                X_sorted = pd.DataFrame({'cftc_positions': [x for x, _ in pairs]})
                y_sorted = pd.Series([y for _, y in pairs], index=X_sorted.index)
                               
                # Create spline with optimal smoothing
                spline = make_smoothing_spline(X_sorted['cftc_positions'], y_sorted, lam=self.optimal_smoothing)
                
                # Store the spline model for the latest window
                if i == len(self.data) - 1:
                    self.spline_model = spline
                
                # Calculate predictions
                y_pred = spline(X_sorted['cftc_positions'])
                r_squared = r2_score(y_sorted, y_pred)
                
                # Predict using the last row of data
                last_x = window_data['cftc_positions'].iloc[-1]
                cof_predicted = spline(last_x)
                
                results.append({
                    'date': self.data.index[i],
                    'cof_actual': self.data[self.cof_term].iloc[i],
                    'cof_predicted': cof_predicted + self.data['fed_funds_sofr_spread'].iloc[i],
                    'r_squared': r_squared,
                    'spline_smoothing': self.optimal_smoothing
                })
            
            self.model_results = pd.DataFrame(results).set_index('date')
            self.model_results.to_excel('Model_Results.xlsx')
            logger.info("Monotonic spline model training completed successfully")
            
        except Exception as e:
            logger.error(f"Error training model: {str(e)}")
            raise
    
    def analyze_liquidity(self):
        """Analyze liquidity indicators and their relationship with COF deviations"""
        try:
            # Calculate COF deviation (actual - predicted)
            self.model_results['cof_deviation'] = (
                self.model_results['cof_actual'] - self.model_results['cof_predicted']
            )
            
            # Merge model results with liquidity data
            analysis_data = pd.merge(
                self.model_results[['cof_deviation']],
                self.data[['fed_funds_sofr_spread']],
                left_index=True,
                right_index=True,
                how='inner'
            )
            
            # Calculate correlation between COF deviation and liquidity indicators
            liquidity_corr = analysis_data[['cof_deviation', 'fed_funds_sofr_spread']].corr()
            
            # Calculate rolling correlation
            window_size = 13  # 3 months
            rolling_corr = analysis_data[['cof_deviation', 'fed_funds_sofr_spread']].rolling(window_size).corr()
            
            # Store results
            self.liquidity_analysis = {
                'correlation': liquidity_corr,
                'rolling_correlation': rolling_corr,
                'analysis_data': analysis_data
            }
            
            logger.info("Liquidity analysis completed successfully")
            
        except Exception as e:
            logger.error(f"Error in liquidity analysis: {str(e)}")
            raise
    
    def plot_results(self):
        """Create visualization of results"""
        try:
            plt.figure(figsize=(15, 15))
            
            # Plot actual vs predicted COF
            plt.subplot(3, 1, 1)
            plt.plot(self.model_results.index, self.model_results['cof_actual'], 
                    label=f'Actual {self.cof_term}', color='blue')
            plt.plot(self.model_results.index, self.model_results['cof_predicted'], 
                    label=f'Predicted {self.cof_term}', color='red', linestyle='--')
            plt.title(f'SPX Cost of Financing: Actual vs Predicted')
            plt.legend()
            plt.grid(True)
            
            # Plot COF deviation
            plt.subplot(3, 1, 2)
            plt.plot(self.model_results.index, self.model_results['cof_deviation'], 
                    label=f'{self.cof_term} Deviation', color='green')
            plt.title(f'{self.cof_term} Deviation from Fair Value')
            plt.legend()
            plt.grid(True)
            
            # Plot actual COF vs CFTC positions with spline fit
            plt.subplot(3, 1, 3)
            plt.scatter(self.data['cftc_positions'], self.data[self.cof_term], 
                       alpha=0.5, color='blue', label=f'{self.cof_term} vs CFTC Positions')
            
            # Sort data for spline fitting
            sort_idx = self.data['cftc_positions'].argsort()
            x_sorted = self.data['cftc_positions'].iloc[sort_idx]
            y_sorted = self.data[self.cof_term].iloc[sort_idx]
            
            # Add spline regression line
            spline = make_smoothing_spline(x_sorted, y_sorted, lam=self.model_results['spline_smoothing'].iloc[-1])
            x_plot = np.linspace(x_sorted.min(), x_sorted.max(), 1000)
            plt.plot(x_plot, spline(x_plot), 
                    "r--", alpha=0.8, 
                    label=f'Spline Fit (smoothing={self.model_results["spline_smoothing"].iloc[-1]:.2f})')
            
            plt.xlabel('CFTC Positions')
            plt.ylabel(f'Actual {self.cof_term}')
            plt.title(f'{self.cof_term} vs CFTC Positions Scatter Plot with Spline Fit')
            plt.legend()
            plt.grid(True)
            
            # Add R-squared value
            y_pred = spline(x_sorted)
            r2 = r2_score(y_sorted, y_pred)
            plt.text(0.05, 0.95, f'R² = {r2:.2f}', 
                    transform=plt.gca().transAxes, 
                    bbox=dict(facecolor='white', alpha=0.8))
            
            plt.tight_layout()
            plt.show()
            
            # Plot liquidity indicators if available
            if hasattr(self, 'liquidity_analysis'):
                # Plot rolling correlations between COF deviation and liquidity indicators
                plt.figure(figsize=(15, 10))
                
                plt.plot(self.liquidity_analysis['analysis_data'].index, 
                        self.liquidity_analysis['rolling_correlation'].xs('cof_deviation', level=1)['fed_funds_sofr_spread'],
                        label='COF Deviation vs Fed Funds-SOFR Spread')
                
                plt.title('Rolling Correlation: COF Deviation vs Fed Funds-SOFR Spread')
                plt.legend()
                plt.grid(True)
                plt.show()
                
                # Print correlation matrix
                print("\nCorrelation Matrix (COF Deviation vs Fed Funds-SOFR Spread):")
                print(self.liquidity_analysis['correlation'].round(3))
                
                # Plot COF deviation vs liquidity indicator
                fig, ax1 = plt.subplots(figsize=(15, 8))
                fig.suptitle('COF Deviation vs Fed Funds-SOFR Spread (Normalized)', fontsize=16)
                
                ax2 = ax1.twinx()
                
                # Normalize the data
                cof_deviation = self.liquidity_analysis['analysis_data']['cof_deviation']
                liquidity_indicator = self.liquidity_analysis['analysis_data']['fed_funds_sofr_spread']
                
                cof_normalized = (cof_deviation - cof_deviation.rolling(window=52, min_periods=10).mean()) / cof_deviation.rolling(window=52, min_periods=10).std()
                liquidity_normalized = (liquidity_indicator - liquidity_indicator.rolling(window=52, min_periods=10).mean()) / liquidity_indicator.rolling(window=52, min_periods=10).std()
                
                # Plot normalized COF deviation as line
                line = ax1.plot(self.liquidity_analysis['analysis_data'].index, 
                              cof_normalized,
                              label='COF Deviation', color='blue')
                
                # Add horizontal line at y=0
                ax1.axhline(y=0, color='black', linestyle='--', alpha=0.3)
                
                # Plot normalized liquidity indicator as bars
                bars = ax2.bar(self.liquidity_analysis['analysis_data'].index,
                             liquidity_normalized,
                             label='Fed Funds-SOFR Spread', color='red', alpha=0.3)
                
                # Set labels and title
                ax1.set_ylabel('COF Deviation', color='blue')
                ax2.set_ylabel('Fed Funds-SOFR Spread', color='red')
                ax1.set_title('COF Deviation vs Fed Funds-SOFR Spread')
                
                # Align y-axes at y=0 with symmetric limits
                y1_min, y1_max = ax1.get_ylim()
                y2_min, y2_max = ax2.get_ylim()
                
                # Find the maximum absolute value for each axis
                y1_abs_max = max(abs(y1_min), abs(y1_max))
                y2_abs_max = max(abs(y2_min), abs(y2_max))
                
                # Set symmetric limits around zero
                ax1.set_ylim(-y1_abs_max, y1_abs_max)
                ax2.set_ylim(-y2_abs_max, y2_abs_max)
                
                # Add legends
                lines1, labels1 = ax1.get_legend_handles_labels()
                lines2, labels2 = ax2.get_legend_handles_labels()
                ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
                
                # Add grid
                ax1.grid(True, alpha=0.3)
                
                plt.tight_layout()
                plt.show()
            
        except Exception as e:
            logger.error(f"Error plotting results: {str(e)}")
            raise

    def adjust_cftc_for_current_price(self, current_spx_price: float) -> float:
        """Adjust CFTC positions to current date based on SPX price change.
        
        Args:
            current_spx_price (float): Current SPX index price
            
        Returns:
            float: Adjusted CFTC positions value
        """
        try:
            # Get the latest data point
            latest_data = self.data.iloc[-1].copy()
            latest_cftc = latest_data['cftc_positions']
            latest_spx = latest_data['spx_price']  # Make sure 'spx_price' column exists
            
            # Calculate price change ratio
            price_change_ratio = current_spx_price / latest_spx
            
            # Adjust CFTC positions proportionally
            adjusted_cftc = latest_cftc * price_change_ratio
            
            logger.info(f"CFTC positions adjusted from {latest_cftc:.2f} to {adjusted_cftc:.2f} "
                       f"based on SPX price change from {latest_spx:.2f} to {current_spx_price:.2f}")
            
            return adjusted_cftc
            
        except Exception as e:
            logger.error(f"Error adjusting CFTC positions: {str(e)}")
            raise

    def predict_fair_value_with_current_price(self, current_spx_price: float) -> Dict[str, float]:
        """Predict fair value COF using current SPX price to adjust CFTC positions.
        
        Args:
            current_spx_price (float): Current SPX index price
            
        Returns:
            Dict[str, float]: Dictionary containing:
                - predicted_cof: Predicted fair value
                - current_cof: Current actual COF
                - deviation: Difference between predicted and current
                - deviation_zscore: Z-score of the deviation
                - signal: Trading signal (-1, 0, 1)
                - adjusted_cftc: The adjusted CFTC positions value used
        """
        try:
            # Adjust CFTC positions for current price
            adjusted_cftc = self.adjust_cftc_for_current_price(current_spx_price)
            
            # Get current data
            current_data = self.data.iloc[-1].copy()
            current_cof = current_data[self.cof_term]
            current_liquidity = current_data['fed_funds_sofr_spread']
            
            # Get the spline model
            spline_model = self.spline_model
            
            # Create prediction using the adjusted CFTC positions
            predicted_cof = spline_model(adjusted_cftc) + current_liquidity
            
            # Calculate deviation
            deviation = predicted_cof - current_cof
            
            # Calculate z-score of deviation using rolling window
            window_size = 52  # 1 year of trading weeks
            historical_deviations = self.model_results['cof_deviation']
            rolling_mean = historical_deviations.rolling(window=window_size, min_periods=10).mean().iloc[-1]
            rolling_std = historical_deviations.rolling(window=window_size, min_periods=10).std().iloc[-1]
            deviation_zscore = (deviation - rolling_mean) / rolling_std if rolling_std != 0 else 0
            
            return {
                'predicted_cof': predicted_cof,
                'current_cof': current_cof,
                'deviation': deviation,
                'deviation_zscore': deviation_zscore,
                'adjusted_cftc': adjusted_cftc
            }
            
        except Exception as e:
            logger.error(f"Error predicting fair value with current price: {str(e)}")
            raise

def main():
    # Initialize analyzer with specific COF term
    analyzer = SPXCOFAnalyzer(cof_term="1Y COF")  # Can be changed to any COF term
    
    # Load data from Excel
    analyzer.load_data_from_excel(r'F:\geds\hedgefundgroup\ENG\EFI\4 - Bolun\12. Data\COF Backtesting.xlsx')
    
    # Visualize smoothing trade-off
    analyzer.visualize_smoothing_tradeoff()
    
    # Run analysis
    analyzer.train_model()
    analyzer.analyze_liquidity()
    analyzer.plot_results()
    
    # Save results back to Excel
    try:
        # Create a copy of the original data
        results_data = analyzer.data.copy()
        
        # Add predicted COF and deviation
        results_data[f'{analyzer.cof_term}_predicted'] = analyzer.model_results['cof_predicted']
        results_data[f'{analyzer.cof_term}_deviation'] = analyzer.model_results['cof_deviation']
        results_data = results_data.dropna(subset=[f'{analyzer.cof_term}_deviation'])
        
        # Save to Excel
        results_data.to_excel('COF_DATA.xlsx')
        logger.info("Analysis results saved to COF_DATA.xlsx")
        
    except Exception as e:
        logger.error(f"Error saving results to Excel: {str(e)}")
        raise

if __name__ == "__main__":
    main() 