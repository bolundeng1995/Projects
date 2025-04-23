import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.regression.linear_model import OLS
from sklearn.metrics import r2_score
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SPXCOFAnalyzer:
    def __init__(self):
        """Initialize the SPX COF analyzer"""
        self.data = None
        self.model_results = None
        
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
            self.data = pd.read_excel(file_path, index_col=0)
            
            # Ensure all required columns are present
            required_columns = [
                'cof', 'cftc_positions',
                'fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis'
            ]
            
            missing_columns = [col for col in required_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info("Data loaded successfully from Excel file")
            
        except Exception as e:
            logger.error(f"Error loading data from Excel: {str(e)}")
            raise
    
    def train_model(self, window_size=252):  # 252 trading days = 1 year
        """Train rolling window regression model"""
        try:
            results = []
            
            for i in range(window_size, len(self.data)):
                window_data = self.data.iloc[i-window_size:i]
                
                # Use CFTC positions as independent variable
                X = window_data[['cftc_positions']]
                y = window_data['cof']
                
                model = OLS(y, X).fit()
                
                results.append({
                    'date': self.data.index[i],
                    'cof_actual': self.data['cof'].iloc[i],
                    'cof_predicted': model.predict(X.iloc[-1:])[0],
                    'r_squared': model.rsquared
                })
            
            self.model_results = pd.DataFrame(results)
            logger.info("Model training completed successfully")
            
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
                self.model_results[['date', 'cof_deviation']],
                self.data[['fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']],
                left_on='date',
                right_index=True,
                how='inner'
            )
            
            # Calculate correlation between COF deviation and liquidity indicators
            liquidity_corr = analysis_data[['cof_deviation', 'fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']].corr()
            
            # Calculate rolling correlation
            window_size = 60  # 3 months
            rolling_corr = analysis_data[['cof_deviation', 'fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']].rolling(window_size).corr()
            
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
            plt.figure(figsize=(15, 10))
            
            # Plot actual vs predicted COF
            plt.subplot(2, 1, 1)
            plt.plot(self.model_results['date'], self.model_results['cof_actual'], 
                    label='Actual COF', color='blue')
            plt.plot(self.model_results['date'], self.model_results['cof_predicted'], 
                    label='Predicted COF', color='red', linestyle='--')
            plt.title('SPX Cost of Financing: Actual vs Predicted')
            plt.legend()
            plt.grid(True)
            
            # Plot COF deviation
            plt.subplot(2, 1, 2)
            plt.plot(self.model_results['date'], self.model_results['cof_deviation'], 
                    label='COF Deviation', color='green')
            plt.title('COF Deviation from Fair Value')
            plt.legend()
            plt.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Plot liquidity indicators if available
            if hasattr(self, 'liquidity_analysis'):
                # Plot rolling correlations between COF deviation and liquidity indicators
                plt.figure(figsize=(15, 10))
                
                for indicator in ['fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']:
                    plt.plot(self.liquidity_analysis['analysis_data'].index, 
                            self.liquidity_analysis['rolling_correlation'].xs('cof_deviation', level=1)[indicator],
                            label=f'COF Deviation vs {indicator}')
                
                plt.title('Rolling Correlation: COF Deviation vs Liquidity Indicators')
                plt.legend()
                plt.grid(True)
                plt.show()
                
                # Print correlation matrix
                print("\nCorrelation Matrix (COF Deviation vs Liquidity Indicators):")
                print(self.liquidity_analysis['correlation'].round(3))
                
                # Plot COF deviation vs each liquidity indicator
                fig, axes = plt.subplots(3, 1, figsize=(15, 15))
                fig.suptitle('COF Deviation vs Liquidity Indicators (Normalized)', fontsize=16)
                
                for idx, indicator in enumerate(['fed_funds_sofr_spread', 'swap_spread', 'jpyusd_basis']):
                    ax1 = axes[idx]
                    ax2 = ax1.twinx()
                    
                    # Normalize the data
                    cof_deviation = self.liquidity_analysis['analysis_data']['cof_deviation']
                    liquidity_indicator = self.liquidity_analysis['analysis_data'][indicator]
                    
                    cof_normalized = (cof_deviation - cof_deviation.mean()) / cof_deviation.std()
                    liquidity_normalized = (liquidity_indicator - liquidity_indicator.mean()) / liquidity_indicator.std()
                    
                    # Plot normalized COF deviation as line
                    line = ax1.plot(self.liquidity_analysis['analysis_data'].index, 
                                  cof_normalized,
                                  label='COF Deviation', color='blue')
                    
                    # Add horizontal line at y=0
                    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.3)
                    
                    # Plot normalized liquidity indicator as bars
                    bars = ax2.bar(self.liquidity_analysis['analysis_data'].index,
                                 liquidity_normalized,
                                 label=indicator, color='red', alpha=0.3)
                    
                    # Set labels and title
                    ax1.set_ylabel('COF Deviation', color='blue')
                    ax2.set_ylabel(indicator, color='red')
                    ax1.set_title(f'COF Deviation vs {indicator}')
                    
                    # Align y-axes at y=0
                    y1_min, y1_max = ax1.get_ylim()
                    y2_min, y2_max = ax2.get_ylim()
                    
                    # Calculate the ratio of the ranges
                    y1_range = y1_max - y1_min
                    y2_range = y2_max - y2_min
                    ratio = y1_range / y2_range
                    
                    # Set the limits to maintain the ratio but align at 0
                    ax1.set_ylim(y1_min, y1_max)
                    ax2.set_ylim(y2_min * ratio, y2_max * ratio)
                    
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

if __name__ == "__main__":
    main() 