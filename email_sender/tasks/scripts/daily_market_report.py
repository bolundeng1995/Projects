import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent.parent.parent / 'src'
sys.path.append(str(src_path))

# Now we can import from src
from data_processor import DataProcessor
import pandas as pd
from datetime import datetime

def main():
    # Initialize components
    processor = DataProcessor()
    
    try:
        # Create sample data (in real case, this would be loaded from a file)
        data = pd.DataFrame({
            'price': [100, 101, 99, 102, 103, 98],
            'volume': [1000, 1200, 900, 1500, 1300, 1100]
        })
        
        # Register data processor
        processor.register_processor(
            'daily_analysis',
            lambda df: df.assign(
                returns=df['price'].pct_change(),
                avg_volume=df['volume'].rolling(3).mean()
            )
        )
        
        # Load data into processor
        processor.data_sources['market_data'] = data  # Direct assignment
        
        # Process data
        results = processor.process_data('market_data', 'daily_analysis')
        
        # Create timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d")
        
        # Save results
        output_dir = Path(__file__).parent.parent / 'output'
        excel_path = output_dir / f'daily_report_{timestamp}.xlsx'
        results.to_excel(excel_path)
        
        # Create and save summary
        summary = f"""
        Daily Market Summary ({datetime.now().strftime('%Y-%m-%d')})
        
        Average Price: ${results['price'].mean():.2f}
        Daily Returns: {results['returns'].mean():.2%}
        Volume: {results['volume'].sum():,.0f}
        """
        
        summary_path = output_dir / f'summary_{timestamp}.txt'
        summary_path.write_text(summary)
        
        print(f"Report generated successfully at {datetime.now()}")
        print(f"Files saved to: {output_dir}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 