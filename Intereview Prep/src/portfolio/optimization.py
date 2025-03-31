import pandas as pd
import numpy as np
import cvxpy as cp
from scipy.optimize import minimize

class PortfolioOptimizer:
    """
    Implements portfolio optimization techniques for factor-based strategies.
    """
    
    def __init__(self, config):
        """
        Initialize the portfolio optimizer.
        
        Parameters:
        -----------
        config : dict
            Configuration dictionary with optimization parameters.
        """
        self.config = config
        self.risk_aversion = config.get('risk_aversion', 1.0)
        self.method = config.get('optimization_method', 'mean_variance')
        
    def optimize(self, expected_returns, risk_model, constraints=None):
        """
        Optimize portfolio weights based on expected returns and risk model.
        
        Parameters:
        -----------
        expected_returns : pandas.Series
            Series with expected returns for each ticker.
        risk_model : pandas.DataFrame or dict
            Risk model (covariance matrix) or dict with risk factor exposures.
        constraints : dict, optional
            Dictionary with optimization constraints.
            
        Returns:
        --------
        pandas.Series
            Series with optimized weights for each ticker.
        """
        if self.method == 'mean_variance':
            return self._mean_variance_optimization(expected_returns, risk_model, constraints)
        elif self.method == 'risk_parity':
            return self._risk_parity_optimization(risk_model, constraints)
        elif self.method == 'max_sharpe':
            return self._max_sharpe_optimization(expected_returns, risk_model, constraints)
        else:
            raise ValueError(f"Unsupported optimization method: {self.method}")
    
    def _mean_variance_optimization(self, expected_returns, cov_matrix, constraints=None):
        """
        Perform mean-variance optimization using CVXPY.
        
        Parameters:
        -----------
        expected_returns : pandas.Series
            Series with expected returns for each ticker.
        cov_matrix : pandas.DataFrame
            Covariance matrix of asset returns.
        constraints : dict, optional
            Dictionary with optimization constraints.
            
        Returns:
        --------
        pandas.Series
            Series with optimized weights.
        """
        # Align data
        common_assets = expected_returns.index.intersection(cov_matrix.index)
        returns = expected_returns[common_assets].values
        cov = cov_matrix.loc[common_assets, common_assets].values
        
        n = len(common_assets)
        
        # Define optimization variables
        weights = cp.Variable(n)
        
        # Define objective function (mean-variance utility)
        returns_term = returns @ weights
        risk_term = cp.quad_form(weights, cov)
        objective = cp.Maximize(returns_term - self.risk_aversion * risk_term)
        
        # Define constraints
        constraint_list = []
        
        # Basic constraint: weights sum to 1 (or 0 for market-neutral)
        if constraints and constraints.get('market_neutral', False):
            constraint_list.append(cp.sum(weights) == 0)
        else:
            constraint_list.append(cp.sum(weights) == 1)
        
        # Position limits
        if constraints and 'position_limits' in constraints:
            pos_limits = constraints['position_limits']
            min_position = pos_limits.get('min_position', -np.inf)
            max_position = pos_limits.get('max_position', np.inf)
            constraint_list.append(weights >= min_position)
            constraint_list.append(weights <= max_position)
        
        # Sector exposure constraints
        if constraints and 'sector_exposures' in constraints:
            sector_data = constraints['sector_exposures']['data']
            sector_limits = constraints['sector_exposures']['limits']
            
            for sector, (min_exp, max_exp) in sector_limits.items():
                sector_stocks = sector_data[sector_data == sector].index
                sector_indices = [i for i, asset in enumerate(common_assets) if asset in sector_stocks]
                sector_exposure = cp.sum([weights[i] for i in sector_indices])
                
                constraint_list.append(sector_exposure >= min_exp)
                constraint_list.append(sector_exposure <= max_exp)
        
        # Style factor constraints
        if constraints and 'style_exposures' in constraints:
            factor_data = constraints['style_exposures']['data']
            factor_limits = constraints['style_exposures']['limits']
            
            for factor, (min_exp, max_exp) in factor_limits.items():
                factor_exposures = factor_data[factor].reindex(common_assets).fillna(0).values
                factor_exposure = factor_exposures @ weights
                
                constraint_list.append(factor_exposure >= min_exp)
                constraint_list.append(factor_exposure <= max_exp)
        
        # Solve optimization problem
        problem = cp.Problem(objective, constraint_list)
        problem.solve()
        
        if problem.status not in ["optimal", "optimal_inaccurate"]:
            raise ValueError(f"Optimization failed with status: {problem.status}")
        
        # Return optimized weights
        optimized_weights = pd.Series(weights.value, index=common_assets)
        
        return optimized_weights
    
    def _risk_parity_optimization(self, cov_matrix, constraints=None):
        """
        Perform risk parity optimization.
        
        Parameters:
        -----------
        cov_matrix : pandas.DataFrame
            Covariance matrix of asset returns.
        constraints : dict, optional
            Dictionary with optimization constraints.
            
        Returns:
        --------
        pandas.Series
            Series with optimized weights.
        """
        assets = cov_matrix.index
        n = len(assets)
        cov = cov_matrix.values
        
        # Define the risk parity objective function
        def risk_parity_objective(weights):
            weights = np.array(weights).reshape(-1, 1)
            portfolio_risk = np.sqrt(weights.T @ cov @ weights)[0, 0]
            asset_contributions = np.multiply(weights, (cov @ weights)) / portfolio_risk
            return np.sum(np.square(asset_contributions - portfolio_risk / n))
        
        # Initial guess: equal weights
        initial_weights = np.ones(n) / n
        
        # Define constraints
        constraints_list = []
        
        # Basic constraint: weights sum to 1
        constraints_list.append({'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0})
        
        # Bounds: non-negative weights (risk parity typically doesn't use shorts)
        bounds = [(0.0, None) for _ in range(n)]
        
        # Position limits
        if constraints and 'position_limits' in constraints:
            pos_limits = constraints['position_limits']
            max_position = pos_limits.get('max_position', 1.0)
            bounds = [(0.0, max_position) for _ in range(n)]
        
        # Solve optimization problem
        result = minimize(
            risk_parity_objective,
            initial_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints_list,
            options={'ftol': 1e-12, 'maxiter': 1000}
        )
        
        if not result.success:
            raise ValueError(f"Risk parity optimization failed: {result.message}")
        
        # Return optimized weights
        optimized_weights = pd.Series(result.x, index=assets)
        
        return optimized_weights
    
    def _max_sharpe_optimization(self, expected_returns, cov_matrix, constraints=None):
        """
        Perform maximum Sharpe ratio optimization.
        
        Parameters:
        -----------
        expected_returns : pandas.Series
            Series with expected returns for each ticker.
        cov_matrix : pandas.DataFrame
            Covariance matrix of asset returns.
        constraints : dict, optional
            Dictionary with optimization constraints.
            
        Returns:
        --------
        pandas.Series
            Series with optimized weights.
        """
        # Align data
        common_assets = expected_returns.index.intersection(cov_matrix.index)
        returns = expected_returns[common_assets].values
        cov = cov_matrix.loc[common_assets, common_assets].values
        
        n = len(common_assets)
        
        # Define optimization variables
        weights = cp.Variable(n)
        aux_var = cp.Variable(1, nonneg=True)  # Auxiliary variable for reformulating max Sharpe
        
        # Define the objective function (maximize Sharpe ratio)
        objective = cp.Maximize(returns @ weights)
        
        # Define constraints
        constraint_list = []
        
        # Basic constraints for max Sharpe reformulation
        constraint_list.append(cp.quad_form(weights, cov) <= aux_var)
        constraint_list.append(cp.sum(weights) == 1)
        
        # Position limits
        if constraints and 'position_limits' in constraints:
            pos_limits = constraints['position_limits']
            min_position = pos_limits.get('min_position', -np.inf)
            max_position = pos_limits.get('max_position', np.inf)
            constraint_list.append(weights >= min_position)
            constraint_list.append(weights <= max_position)
        
        # Sector exposure constraints
        if constraints and 'sector_exposures' in constraints:
            sector_data = constraints['sector_exposures']['data']
            sector_limits = constraints['sector_exposures']['limits']
            
            for sector, (min_exp, max_exp) in sector_limits.items():
                sector_stocks = sector_data[sector_data == sector].index
                sector_indices = [i for i, asset in enumerate(common_assets) if asset in sector_stocks]
                sector_exposure = cp.sum([weights[i] for i in sector_indices])
                
                constraint_list.append(sector_exposure >= min_exp)
                constraint_list.append(sector_exposure <= max_exp)
        
        # Solve optimization problem
        problem = cp.Problem(objective, constraint_list)
        problem.solve()
        
        if problem.status not in ["optimal", "optimal_inaccurate"]:
            raise ValueError(f"Optimization failed with status: {problem.status}")
        
        # Return optimized weights
        optimized_weights = pd.Series(weights.value / aux_var.value, index=common_assets)
        
        return optimized_weights 