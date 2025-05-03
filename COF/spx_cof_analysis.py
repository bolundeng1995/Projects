import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm
from sklearn.metrics import r2_score
import logging

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
    
    def train_model(self, window_size=52):  # 52 trading weeks = 1 year
        """Train rolling window regression model using quadratic form"""
        try:
            results = []
            
            for i in range(window_size, len(self.data)):
                window_data = self.data.iloc[i-window_size:i+1]
                
                # Create quadratic terms for CFTC positions
                X = pd.DataFrame({
                    'cftc_positions': window_data['cftc_positions'],
                    'cftc_positions_squared': window_data['cftc_positions'] ** 2
                })
                X = sm.add_constant(X)  # Add constant term
                y = window_data[self.cof_term]
                
                model = OLS(y, X).fit()
                
                # Predict using the last row of data
                last_X = pd.DataFrame({
                    'const': [1],  # Add constant term explicitly
                    'cftc_positions': [window_data['cftc_positions'].iloc[-1]],
                    'cftc_positions_squared': [window_data['cftc_positions'].iloc[-1] ** 2]
                })
                # Ensure columns are in the same order as the training data
                last_X = last_X[X.columns]
                
                results.append({
                    'date': self.data.index[i],
                    'cof_actual': self.data[self.cof_term].iloc[i],
                    'cof_predicted': model.predict(last_X)[0] + self.data['fed_funds_sofr_spread'].iloc[i],
                    'r_squared': model.rsquared,
                    'quadratic_coef': model.params['cftc_positions_squared'],
                    'linear_coef': model.params['cftc_positions'],
                    'intercept': model.params['const']
                })
            
            self.model_results = pd.DataFrame(results).set_index('date')
            self.model_results.to_excel('Model_Results.xlsx')
            logger.info("Quadratic model training completed successfully")
            
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
            
            # Plot actual COF vs CFTC positions with quadratic fit
            plt.subplot(3, 1, 3)
            plt.scatter(self.data['cftc_positions'], self.data[self.cof_term], 
                       alpha=0.5, color='blue', label=f'{self.cof_term} vs CFTC Positions')
            
            # Add quadratic regression line
            z = np.polyfit(self.data['cftc_positions'], self.data[self.cof_term], 2)
            p = np.poly1d(z)
            x_sorted = np.sort(self.data['cftc_positions'])
            plt.plot(x_sorted, p(x_sorted), 
                    "r--", alpha=0.8, 
                    label=f'Quadratic Fit (y={z[0]:.2f}x²+{z[1]:.2f}x+{z[2]:.2f})')
            
            plt.xlabel('CFTC Positions')
            plt.ylabel(f'Actual {self.cof_term}')
            plt.title(f'{self.cof_term} vs CFTC Positions Scatter Plot with Quadratic Fit')
            plt.legend()
            plt.grid(True)
            
            # Add R-squared value
            y_pred = p(self.data['cftc_positions'])
            r2 = r2_score(self.data[self.cof_term], y_pred)
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
        results_data['cof_predicted'] = analyzer.model_results['cof_predicted']
        results_data['cof_deviation'] = analyzer.model_results['cof_deviation']
        results_data = results_data.dropna(subset=['cof_deviation'])
        
        # Save to Excel
        results_data.to_excel('COF_DATA.xlsx')
        logger.info("Analysis results saved to COF_DATA.xlsx")
        
    except Exception as e:
        logger.error(f"Error saving results to Excel: {str(e)}")
        raise

if __name__ == "__main__":
    main() 