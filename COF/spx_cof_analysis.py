import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.regression.linear_model import OLS
from sklearn.metrics import r2_score
import logging
from scipy.optimize import minimize
from typing import Optional, Dict, Tuple, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SPXCOFAnalyzer:
    """A class for analyzing the relationship between SPX Cost of Financing (COF) and CFTC positions.
    
    This class implements a constrained quadratic regression model to analyze the relationship
    between CFTC positions and COF, ensuring the relationship is non-decreasing. It includes
    methods for data loading, model training, and visualization of results.

    Attributes:
        data (pd.DataFrame): Raw data loaded from Excel file
        model_results (pd.DataFrame): Results from the trained model including predictions
        liquidity_analysis (dict): Results from liquidity analysis if performed
    """

    def __init__(self):
        """Initialize the SPX COF analyzer with empty data structures."""
        self.data: Optional[pd.DataFrame] = None
        self.model_results: Optional[pd.DataFrame] = None
        self.liquidity_analysis: Optional[Dict[str, Any]] = None
        
    def load_data_from_excel(self, file_path: str = 'COF_DATA.xlsx') -> None:
        """Load and validate data from Excel file.
        
        This method loads data from an Excel file, performs basic validation,
        and prepares it for analysis. It ensures all required columns are present
        and sorts the data chronologically.

        Args:
            file_path (str): Path to the Excel file containing the data

        Raises:
            ValueError: If required columns are missing
            Exception: For other data loading errors
        """
        try:
            # Read data from Excel
            self.data = pd.read_excel(file_path, sheet_name="Data", index_col=0)
            
            # Sort data in ascending order (oldest to latest)
            self.data = self.data.sort_index()
            
            # Ensure all required columns are present
            required_columns = [
                '1Y COF', 'cftc_positions',
                'fed_funds_sofr_spread'
            ]
            
            missing_columns = [col for col in required_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info("Data loaded successfully from Excel file")
            
        except Exception as e:
            logger.error(f"Error loading data from Excel: {str(e)}")
            raise

    def _create_quadratic_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create quadratic features for CFTC positions.
        
        This method creates a feature matrix containing both linear and quadratic
        terms of CFTC positions for the quadratic regression model.

        Args:
            data (pd.DataFrame): Input data containing CFTC positions
            
        Returns:
            pd.DataFrame: Features matrix with columns:
                - cftc_positions: Original CFTC position values
                - cftc_positions_squared: Squared CFTC position values
        """
        return pd.DataFrame({
            'cftc_positions': data['cftc_positions'],
            'cftc_positions_squared': data['cftc_positions'] ** 2
        })

    def _objective_function(self, params: np.ndarray, X: pd.DataFrame, y: pd.Series) -> float:
        """Objective function for optimization: minimize sum of squared errors.
        
        This method implements the objective function for the optimization problem,
        which is to minimize the sum of squared errors between predicted and actual COF values.

        Args:
            params (np.ndarray): Model parameters [a, b, c] where:
                - a: coefficient for quadratic term
                - b: coefficient for linear term
                - c: intercept
            X (pd.DataFrame): Feature matrix
            y (pd.Series): Target values (actual COF)
            
        Returns:
            float: Sum of squared errors between predicted and actual values
        """
        a, b, c = params
        y_pred = a * X['cftc_positions_squared'] + b * X['cftc_positions'] + c
        return np.sum((y - y_pred) ** 2)

    def _monotonicity_constraint(self, params: np.ndarray, min_cftc: float, max_cftc: float) -> float:
        """Constraint ensuring the quadratic function is non-decreasing.
        
        This method implements the constraint that ensures the quadratic function
        is non-decreasing across the range of CFTC positions. It checks that the
        derivative (2ax + b) is non-negative at both the minimum and maximum
        CFTC position values.

        Args:
            params (np.ndarray): Model parameters [a, b, c]
            min_cftc (float): Minimum CFTC position value in the window
            max_cftc (float): Maximum CFTC position value in the window
            
        Returns:
            float: Minimum value of the derivative in the range, which must be ≥ 0
        """
        a, b, _ = params
        return min(2 * a * min_cftc + b, 2 * a * max_cftc + b)

    def _fit_constrained_quadratic(self, X: pd.DataFrame, y: pd.Series, 
                                 min_cftc: float, max_cftc: float) -> Tuple[np.ndarray, float]:
        """Fit a constrained quadratic model using optimization.
        
        This method fits a quadratic model to the data while ensuring the function
        is non-decreasing. It uses scipy's minimize function with the SLSQP method
        to find the optimal parameters that minimize squared errors while satisfying
        the monotonicity constraint.

        Args:
            X (pd.DataFrame): Feature matrix
            y (pd.Series): Target values
            min_cftc (float): Minimum CFTC position value
            max_cftc (float): Maximum CFTC position value
            
        Returns:
            Tuple[np.ndarray, float]: A tuple containing:
                - np.ndarray: Model coefficients [a, b, c]
                - float: R-squared value of the fit
        """
        # Initial guess for parameters
        x0 = [0.1, 0.1, 0.1]
        
        # Constraints: derivative must be non-negative
        constraints = {
            'type': 'ineq',
            'fun': lambda params: self._monotonicity_constraint(params, min_cftc, max_cftc)
        }
        
        # Bounds for parameters (a ≥ 0 to ensure convexity)
        bounds = [(0, None), (None, None), (None, None)]
        
        # Optimize
        result = minimize(
            lambda params: self._objective_function(params, X, y),
            x0,
            method='SLSQP',
            constraints=constraints,
            bounds=bounds
        )
        
        # Calculate R-squared
        a, b, c = result.x
        y_pred = a * X['cftc_positions_squared'] + b * X['cftc_positions'] + c
        r2 = 1 - np.sum((y - y_pred) ** 2) / np.sum((y - y.mean()) ** 2)
        
        return result.x, r2

    def train_model(self, window_size: int = 52) -> None:
        """Train rolling window regression model using constrained quadratic form.
        
        This method implements a rolling window approach where for each window:
        1. Creates quadratic features
        2. Fits a constrained quadratic model
        3. Makes predictions
        4. Stores results
        
        The model ensures that the relationship between CFTC positions and COF
        is non-decreasing, meaning higher CFTC positions always correspond to
        higher COF values.

        Args:
            window_size (int): Size of the rolling window in weeks (default: 52)

        Raises:
            Exception: If there's an error during model training
        """
        try:
            results = []
            
            for i in range(window_size, len(self.data)):
                # Get window data
                window_data = self.data.iloc[i-window_size:i]
                min_cftc = window_data['cftc_positions'].min()
                max_cftc = window_data['cftc_positions'].max()
                
                # Prepare features and target
                X = self._create_quadratic_features(window_data)
                y = window_data['1Y COF']
                
                # Fit model and get coefficients
                (a, b, c), r2 = self._fit_constrained_quadratic(X, y, min_cftc, max_cftc)
                
                # Make prediction for the last data point
                last_cftc = window_data['cftc_positions'].iloc[-1]
                last_pred = a * (last_cftc ** 2) + b * last_cftc + c
                
                # Store results
                results.append({
                    'date': self.data.index[i-1],
                    'cof_actual': self.data['1Y COF'].iloc[i-1],
                    'cof_predicted': last_pred,
                    'r_squared': r2,
                    'quadratic_coef': a,
                    'linear_coef': b,
                    'intercept': c
                })
            
            self.model_results = pd.DataFrame(results).set_index('date')
            logger.info("Constrained quadratic model training completed successfully")
            
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
            window_size = 12  # 3 months
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
                    label='Actual COF', color='blue')
            plt.plot(self.model_results.index, self.model_results['cof_predicted'], 
                    label='Predicted COF', color='red', linestyle='--')
            plt.title('SPX Cost of Financing: Actual vs Predicted')
            plt.legend()
            plt.grid(True)
            
            # Plot COF deviation
            plt.subplot(3, 1, 2)
            plt.plot(self.model_results.index, self.model_results['cof_deviation'], 
                    label='COF Deviation', color='green')
            plt.title('COF Deviation from Fair Value')
            plt.legend()
            plt.grid(True)
            
            # Plot actual COF vs CFTC positions with constrained quadratic fit
            plt.subplot(3, 1, 3)
            plt.scatter(self.data['cftc_positions'], self.data['1Y COF'], 
                       alpha=0.5, color='blue', label='COF vs CFTC Positions')
            
            # Add constrained quadratic regression line
            z = np.polyfit(self.data['cftc_positions'], self.data['1Y COF'], 2)
            p = np.poly1d(z)
            x_sorted = np.sort(self.data['cftc_positions'])
            
            # Ensure the function is non-decreasing
            a, b, c = z
            if 2 * a * x_sorted[0] + b < 0:  # If derivative is negative at start
                # Adjust coefficients to make it non-decreasing
                b = -2 * a * x_sorted[0]  # Set derivative to 0 at start
                c = p(x_sorted[0]) - (a * x_sorted[0]**2 + b * x_sorted[0])  # Adjust intercept
            
            p = np.poly1d([a, b, c])
            plt.plot(x_sorted, p(x_sorted), 
                    "r--", alpha=0.8, 
                    label=f'Constrained Quadratic Fit (y={a:.2f}x²+{b:.2f}x+{c:.2f})')
            
            plt.xlabel('CFTC Positions')
            plt.ylabel('Actual COF')
            plt.title('COF vs CFTC Positions Scatter Plot with Constrained Quadratic Fit')
            plt.legend()
            plt.grid(True)
            
            # Add R-squared value
            y_pred = p(self.data['cftc_positions'])
            r2 = r2_score(self.data['1Y COF'], y_pred)
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

def main():
    # Initialize analyzer
    analyzer = SPXCOFAnalyzer()
    
    # Load data from Excel
    analyzer.load_data_from_excel('COF_DATA.xlsx')
    
    # Run analysis
    analyzer.train_model()
    analyzer.analyze_liquidity()
    analyzer.plot_results()
    
    # Save results back to Excel
    try:
        # Create a copy of the original data
        results_data = analyzer.data.copy()
        
        # Add predicted COF and deviation
        results_data['cof_predicted'] = analyzer.model_results['cof_predicted']
        results_data['cof_deviation'] = analyzer.model_results['cof_deviation']
        
        # Save to Excel
        results_data.to_excel('COF_DATA.xlsx')
        logger.info("Analysis results saved to COF_DATA.xlsx")
        
    except Exception as e:
        logger.error(f"Error saving results to Excel: {str(e)}")
        raise

if __name__ == "__main__":
    main() 