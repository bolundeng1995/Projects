"""
Example task script template.

This script demonstrates:
- Basic data processing setup
- Result file generation
- Error handling

Usage:
    1. Copy this template for new tasks
    2. Modify the data processing logic
    3. Add to the scheduler using the GUI
"""

from data_processor import DataProcessor
from email_sender import EmailManager
from pathlib import Path

def main():
    """
    Main task execution function.
    
    Demonstrates:
    - Data processing setup
    - Result generation
    - Error handling
    """
    # Initialize components
    processor = DataProcessor()
    email_mgr = EmailManager()
    
    # Register data processor
    processor.register_processor('calculate_returns', 
        lambda df: df.assign(returns=df['price'].pct_change())
    )
    
    # Register email template
    email_mgr.register_template(
        'daily_report',
        'Daily Market Report - {date}',
        'Please find attached the daily report.\n\nSummary:\n{summary}',
        ['analyst@company.com']
    )
    
    # Process data
    processor.load_data('market_data', 'data/market.csv')
    results = processor.process_data('market_data', 'calculate_returns')
    
    # Save results
    output_path = Path('output/daily_report.xlsx')
    results.to_excel(output_path)
    
    # Send email
    email_mgr.send_email(
        'daily_report',
        data={'summary': results.describe().to_string()},
        attachments=[output_path]
    )

if __name__ == "__main__":
    main() 