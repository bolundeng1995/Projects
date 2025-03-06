from typing import Dict, Any, Callable
import pandas as pd
from pathlib import Path

"""
Data processing module for handling various data formats and transformations.

This module provides:
- Flexible data loading from different sources
- Customizable data processing functions
- Data transformation pipeline management
"""

class DataProcessor:
    """
    Handles data loading and processing operations.
    
    Features:
    - Multiple data source management
    - Custom processor registration
    - Flexible data transformations
    
    Usage:
        processor = DataProcessor()
        processor.register_processor('my_analysis', my_function)
        processor.load_data('source1', 'data.csv')
        results = processor.process_data('source1', 'my_analysis')
    """
    def __init__(self) -> None:
        self.data_sources: dict[str, pd.DataFrame] = {}
        self.processors: dict[str, Callable] = {}
        
    def register_processor(self, name: str, processor_func: Callable) -> None:
        """Register a data processing function"""
        self.processors[name] = processor_func
        
    def load_data(self, source_name: str, source_path: Path | str, 
                  file_type: str = 'csv', **kwargs) -> None:
        """Load data from various sources with flexible parameters"""
        source_path = Path(source_path)
        
        match file_type.lower():
            case 'csv':
                self.data_sources[source_name] = pd.read_csv(source_path, **kwargs)
            case 'excel' | 'xlsx' | 'xls':
                self.data_sources[source_name] = pd.read_excel(source_path, **kwargs)
            case _:
                raise ValueError(f"Unsupported file type: {file_type}")
            
    def process_data(self, source_name: str, processor_name: str, **kwargs) -> pd.DataFrame:
        """Process data using registered processor functions"""
        if source_name not in self.data_sources:
            raise KeyError(f"Data source '{source_name}' not found")
        if processor_name not in self.processors:
            raise KeyError(f"Processor '{processor_name}' not registered")
            
        data = self.data_sources[source_name].copy()
        return self.processors[processor_name](data, **kwargs) 