import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

class PerformanceVisualizer:
    """
    Visualization tools for analyzing strategy performance.
    """
    
    def __init__(self, figsize=(12, 8)):
        """
        Initialize the performance visualizer.
        
        Parameters:
        -----------
        figsize : tuple, optional
            Default figure size for plots.
        """
        self.figsize = figsize
        self.style_setup()
        
    def style_setup(self):
        """Set up the style for visualization."""
        # Use seaborn style
        sns.set(style="whitegrid")
        plt.rcParams['figure.figsize'] = self.figsize
        plt.rcParams['axes.labelsize'] = 12
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10
        
    def plot_cumulative_returns(self, returns, benchmark_returns=None, title="Cumulative Returns"):
        """
        Plot cumulative returns of the strategy and benchmark.
        
        Parameters:
        -----------
        returns : pandas.Series
            Series with strategy returns.
        benchmark_returns : pandas.Series, optional
            Series with benchmark returns.
        title : str, optional
            Plot title.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Calculate cumulative returns
        cum_returns = (1 + returns).cumprod() - 1
        
        # Plot strategy returns
        ax.plot(cum_returns.index, cum_returns.values * 100, label='Strategy', linewidth=2)
        
        # Plot benchmark returns if provided
        if benchmark_returns is not None:
            cum_benchmark = (1 + benchmark_returns).cumprod() - 1
            ax.plot(cum_benchmark.index, cum_benchmark.values * 100, label='Benchmark', 
                   linewidth=2, linestyle='--', alpha=0.7)
        
        # Add percent formatter to y-axis
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.0f}%'))
        
        # Add labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Cumulative Return (%)')
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add horizontal line at 0%
        ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3)
        
        # Rotate date labels for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return fig
    
    def plot_drawdowns(self, returns, top_n=5, title="Drawdown Analysis"):
        """
        Plot drawdowns of the strategy.
        
        Parameters:
        -----------
        returns : pandas.Series
            Series with strategy returns.
        top_n : int, optional
            Number of largest drawdowns to highlight.
        title : str, optional
            Plot title.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Calculate cumulative returns
        cum_returns = (1 + returns).cumprod()
        
        # Calculate running maximum
        running_max = cum_returns.cummax()
        
        # Calculate drawdowns
        drawdowns = (cum_returns / running_max - 1) * 100
        
        # Plot drawdowns
        ax.fill_between(drawdowns.index, drawdowns.values, 0, color='red', alpha=0.3)
        ax.plot(drawdowns.index, drawdowns.values, color='red', alpha=0.5)
        
        # Add percent formatter to y-axis
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.0f}%'))
        
        # Add labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Drawdown (%)')
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        
        # Highlight top drawdowns
        if top_n > 0:
            # Find the largest drawdowns
            dd_start = self._find_drawdown_start_end(drawdowns, top_n)
            
            # Highlight each major drawdown
            colors = plt.cm.tab10(np.linspace(0, 1, top_n))
            
            for i, ((start_date, end_date, recovery_date), drawdown) in enumerate(dd_start):
                # Skip if recovery hasn't happened yet
                if pd.isna(recovery_date):
                    recovery_date = drawdowns.index[-1]
                
                # Highlight the drawdown period
                ax.axvspan(start_date, end_date, color=colors[i], alpha=0.2)
                
                # Add text annotation for the drawdown
                min_idx = drawdowns.loc[start_date:end_date].idxmin()
                ax.annotate(f"{drawdown:.1f}%", 
                           xy=(min_idx, drawdowns.loc[min_idx]),
                           xytext=(min_idx, drawdowns.loc[min_idx] * 0.7),
                           arrowprops=dict(arrowstyle="->", color="black", alpha=0.7),
                           ha='center')
        
        # Rotate date labels for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return fig
    
    def _find_drawdown_start_end(self, drawdowns, n=5):
        """
        Find the start and end dates of the largest drawdowns.
        
        Parameters:
        -----------
        drawdowns : pandas.Series
            Series with drawdown values.
        n : int, optional
            Number of largest drawdowns to find.
            
        Returns:
        --------
        list
            List of tuples with (start_date, end_date, recovery_date, drawdown_value)
        """
        # Initialize result list
        result = []
        
        # Create a copy to avoid modifying the original
        dd_copy = drawdowns.copy()
        
        for _ in range(n):
            # Find the lowest point of the drawdown
            min_idx = dd_copy.idxmin()
            min_value = dd_copy.loc[min_idx]
            
            if min_value >= 0:
                break  # No more drawdowns
                
            # Find start of the drawdown (last time drawdown was 0 before the min)
            try:
                start_idx = dd_copy.loc[:min_idx][dd_copy.loc[:min_idx] >= 0].index[-1]
            except IndexError:
                start_idx = dd_copy.index[0]
                
            # Find end of the drawdown (first time drawdown was at min)
            end_idx = min_idx
            
            # Find recovery date (first time drawdown returns to 0 after min)
            recovery_mask = dd_copy.loc[end_idx:] >= 0
            recovery_idx = recovery_mask.idxmax() if recovery_mask.any() else None
            
            # Add to results
            result.append(((start_idx, end_idx, recovery_idx), min_value))
            
            # Mask this drawdown period to find the next largest
            if recovery_idx:
                dd_copy.loc[start_idx:recovery_idx] = 0
            else:
                dd_copy.loc[start_idx:] = 0
        
        return result
    
    def plot_monthly_returns(self, returns, title="Monthly Returns Heatmap", figsize=None, cmap='RdYlGn'):
        """
        Plot monthly returns as a heatmap.
        
        Parameters:
        -----------
        returns : pandas.Series
            Series with strategy returns.
        title : str, optional
            Plot title.
        figsize : tuple, optional
            Figure size (width, height) in inches.
        cmap : str, optional
            Colormap for the heatmap.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        if figsize is None:
            figsize = self.figsize
        
        # Use 'ME' (month end) instead of deprecated 'M'
        monthly_returns = returns.resample('ME').apply(lambda x: (1 + x).prod() - 1)
        
        # Convert to a DataFrame with year and month dimensions
        monthly_return_table = pd.DataFrame({
            'Year': monthly_returns.index.year,
            'Month': monthly_returns.index.month,
            'Return': monthly_returns.values
        })
        
        # Pivot to get years as rows and months as columns
        return_pivot = monthly_return_table.pivot(index='Year', columns='Month', values='Return')
        
        # Map month numbers to month names
        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        return_pivot.columns = [month_names[m] for m in return_pivot.columns]
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=figsize)
        
        # Calculate maximum absolute return for symmetric color scale
        max_abs_return = max(abs(return_pivot.min().min()), abs(return_pivot.max().max()))
        
        # Create heatmap
        sns.heatmap(return_pivot * 100, ax=ax, cmap=cmap, center=0,
                   annot=True, fmt=".1f", linewidths=0.5,
                   vmin=-max_abs_return * 100, vmax=max_abs_return * 100,
                   cbar_kws={'label': 'Return (%)'})
        
        # Set title and labels
        ax.set_title(title)
        ax.set_xlabel('')
        ax.set_ylabel('')
        
        plt.tight_layout()
        
        return fig
    
    def plot_factor_exposures(self, exposures, title="Factor Exposures Over Time"):
        """
        Plot factor exposures over time.
        
        Parameters:
        -----------
        exposures : pandas.DataFrame
            DataFrame with factor exposures over time.
        title : str, optional
            Plot title.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Plot each factor exposure
        for column in exposures.columns:
            ax.plot(exposures.index, exposures[column], label=column, linewidth=2, alpha=0.7)
        
        # Add labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Exposure')
        ax.set_title(title)
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        
        # Add horizontal line at 0
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
        
        # Rotate date labels for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return fig
    
    def plot_return_attribution(self, returns, factor_returns, exposures, title="Return Attribution"):
        """
        Plot return attribution to different factors.
        
        Parameters:
        -----------
        returns : pandas.Series
            Series with strategy returns.
        factor_returns : pandas.DataFrame
            DataFrame with factor returns.
        exposures : pandas.DataFrame
            DataFrame with strategy exposures to factors.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        # Calculate factor contributions
        contributions = pd.DataFrame(index=returns.index, columns=factor_returns.columns)
        
        for factor in factor_returns.columns:
            if factor in exposures.columns:
                contributions[factor] = exposures[factor] * factor_returns[factor]
        
        # Calculate unexplained returns
        contributions['Unexplained'] = returns - contributions.sum(axis=1)
        
        # Resample to monthly for better visualization
        monthly_contrib = contributions.resample('M').sum()
        
        # Plot stacked bar chart
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Create stacked bars
        bottom = pd.Series(0, index=monthly_contrib.index)
        for column in monthly_contrib.columns:
            ax.bar(monthly_contrib.index, monthly_contrib[column], bottom=bottom, 
                  label=column, alpha=0.7)
            bottom += monthly_contrib[column]
        
        # Add labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Return Attribution')
        ax.set_title(title)
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        
        # Add horizontal line at 0
        ax.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        
        # Rotate date labels for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return fig
    
    def plot_risk_decomposition(self, weights, cov_matrix, risk_factors=None, title="Risk Decomposition"):
        """
        Plot risk decomposition of the portfolio.
        
        Parameters:
        -----------
        weights : pandas.Series
            Series with portfolio weights.
        cov_matrix : pandas.DataFrame
            Covariance matrix of asset returns.
        risk_factors : pandas.DataFrame, optional
            DataFrame with risk factor exposures.
        title : str, optional
            Plot title.
            
        Returns:
        --------
        matplotlib.figure.Figure
            Figure object.
        """
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Calculate portfolio risk
        portfolio_risk = np.sqrt(weights @ cov_matrix @ weights)
        
        # Calculate marginal contribution to risk (MCR)
        mcr = (cov_matrix @ weights) / portfolio_risk
        
        # Calculate component contribution to risk (CCR)
        ccr = weights * mcr
        
        # Sort risk contribution
        ccr_sorted = ccr.sort_values(ascending=False)
        
        # Plot risk contribution
        ax.bar(range(len(ccr_sorted)), ccr_sorted.values, alpha=0.7)
        
        # Add labels and title
        ax.set_xlabel('Stocks')
        ax.set_ylabel('Risk Contribution')
        ax.set_title(title)
        ax.set_xticks(range(len(ccr_sorted)))
        ax.set_xticklabels(ccr_sorted.index, rotation=90)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        return fig 