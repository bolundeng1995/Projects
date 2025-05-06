import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm
from sklearn.metrics import r2_score
import logging
from scipy.optimize import minimize
from scipy.interpolate import UnivariateSpline

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
    
    def _spline_objective(self, params, X, y, knots):
        """Objective function for spline regression"""
        # Create spline basis functions
        spline = UnivariateSpline(X['cftc_positions'], y, k=3, s=params[0])
        y_pred = spline(X['cftc_positions'])
        return np.sum((y - y_pred) ** 2)
    
    def train_model(self, window_size=52):  # 52 trading weeks = 1 year
        """Train rolling window regression model using monotonic spline"""
        try:
            results = []
            
            for i in range(window_size, len(self.data)):
                window_data = self.data.iloc[i-window_size:i+1]
                
                # Prepare data
                X = pd.DataFrame({
                    'cftc_positions': window_data['cftc_positions']
                })
                y = window_data[self.cof_term]
                
                # Sort data by CFTC positions to ensure monotonicity
                sort_idx = X['cftc_positions'].argsort()
                X_sorted = X.iloc[sort_idx]
                y_sorted = y.iloc[sort_idx]
                
                # Create monotonic spline
                smoothing_factor = len(X) * 0.1  # initial smoothing parameter
                spline = UnivariateSpline(
                    X_sorted['cftc_positions'],
                    y_sorted,
                    k=3,  # cubic spline
                    s=smoothing_factor
                )
                
                # Ensure monotonicity by checking derivatives
                x_range = np.linspace(X_sorted['cftc_positions'].min(), 
                                    X_sorted['cftc_positions'].max(), 
                                    1000)
                derivatives = spline.derivative()(x_range)
                
                if not np.all(derivatives >= 0):
                    # If not monotonic, increase smoothing
                    smoothing_factor = len(X) * 0.5  # increased smoothing
                    spline = UnivariateSpline(
                        X_sorted['cftc_positions'],
                        y_sorted,
                        k=3,
                        s=smoothing_factor
                    )
                
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
                    'spline_smoothing': smoothing_factor
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
            window_size = 26  # 6 months
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
            spline = UnivariateSpline(x_sorted, y_sorted, k=3, s=self.model_results['spline_smoothing'].iloc[-1])
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

def main():
    # Initialize analyzer with specific COF term
    analyzer = SPXCOFAnalyzer(cof_term="1Y COF")  # Can be changed to any COF term
    
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