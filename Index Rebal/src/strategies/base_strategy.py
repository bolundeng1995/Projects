import pandas as pd
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    def __init__(self, database, signal_generator):
        self.db = database
        self.signal_generator = signal_generator
        self.positions = {}
        self.params = self._default_params()
        
    @abstractmethod
    def _default_params(self) -> Dict[str, Any]:
        """Define default strategy parameters"""
        pass
        
    @abstractmethod
    def generate_orders(self) -> List[Dict[str, Any]]:
        """Generate trading orders based on signals and current positions"""
        pass
        
    def update_params(self, params: Dict[str, Any]):
        """Update strategy parameters"""
        self.params.update(params)
        
    def get_current_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get current strategy positions"""
        return self.positions
        
    def update_positions(self, executions: List[Dict[str, Any]]):
        """Update positions based on trade executions"""
        # Implementation details
        pass 