import argparse
from data.database import IndexDatabase
from data.importers.price_data import PriceDataImporter
from data.importers.index_constituents import IndexConstituentImporter
from data.calendar import RebalanceCalendar
from detection.scanners import RebalanceScanner
from detection.prediction_models import AdditionPredictionModel
from signals.signal_generator import SignalGenerator
from strategies.sp_rebalance_strategy import SPRebalanceStrategy
from strategies.russell_rebalance_strategy import RussellRebalanceStrategy
from strategies.nasdaq_rebalance_strategy import NasdaqRebalanceStrategy
from backtesting.backtest_engine import BacktestEngine
from risk.risk_manager import RiskManager
from production.monitor import MonitoringDashboard
from production.alert_system import AlertSystem
from analytics.performance_tracker import PerformanceTracker
from execution.execution_optimizer import ExecutionOptimizer
from research.research_environment import ResearchEnvironment

def main():
    parser = argparse.ArgumentParser(description='Index Rebalance Strategy')
    parser.add_argument('--mode', choices=['backtest', 'live', 'research'], 
                      default='backtest', help='Operation mode')
    parser.add_argument('--start-date', default='2018-01-01', 
                      help='Start date for backtest')
    parser.add_argument('--end-date', default='2023-12-31', 
                      help='End date for backtest')
    parser.add_argument('--config', default='config.json', 
                      help='Configuration file path')
    args = parser.parse_args()
    
    # Initialize components
    db = IndexDatabase('index_rebalance.db')
    
    # Data components
    price_importer = PriceDataImporter(db)
    constituent_importer = IndexConstituentImporter(db)
    calendar = RebalanceCalendar(db)
    
    # Detection components
    scanner = RebalanceScanner(db)
    prediction_model = AdditionPredictionModel(db)
    
    # Signal and strategy components
    signal_generator = SignalGenerator(db)
    sp_strategy = SPRebalanceStrategy(db, signal_generator)
    russell_strategy = RussellRebalanceStrategy(db, signal_generator)
    nasdaq_strategy = NasdaqRebalanceStrategy(db, signal_generator)
    
    # Risk management
    risk_manager = RiskManager(db)
    
    # Performance tracking
    performance_tracker = PerformanceTracker(db)
    
    # Execution optimization
    execution_optimizer = ExecutionOptimizer(db)
    
    if args.mode == 'backtest':
        # Run backtest
        backtest_engine = BacktestEngine(db, args.start_date, args.end_date)
        results = backtest_engine.run_backtest(sp_strategy)
        performance_tracker.plot_performance(results['returns'])
        
    elif args.mode == 'live':
        # Run live trading system
        dashboard = MonitoringDashboard(db, {
            'sp': sp_strategy,
            'russell': russell_strategy,
            'nasdaq': nasdaq_strategy
        })
        alert_system = AlertSystem(db, {
            'from': 'alerts@example.com',
            'to': 'trader@example.com',
            'smtp_server': 'smtp.example.com',
            'smtp_port': 587,
            'username': 'alerts@example.com',
            'password': 'password'
        })
        dashboard.run_dashboard()
        
    elif args.mode == 'research':
        # Run research environment
        backtest_engine = BacktestEngine(db, args.start_date, args.end_date)
        research_env = ResearchEnvironment(db, backtest_engine)
        
        # Example research task
        param_grid = {
            'max_position_size': [0.03, 0.05, 0.07],
            'entry_days_before': [2, 3, 4],
            'exit_days_after': [1, 2, 3],
            'implementation_day_weight': [0.3, 0.4, 0.5]
        }
        optimization_results = research_env.optimize_parameters(
            SPRebalanceStrategy, param_grid, args.start_date, args.end_date)
        print("Optimal parameters:", optimization_results['optimal_params'])

if __name__ == '__main__':
    main() 