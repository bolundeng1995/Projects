import sys
from pathlib import Path
import numpy as np
import pandas_market_calendars as mcal

# Add src directory to Python path
src_path = Path(__file__).parent.parent.parent / 'src'
sys.path.append(str(src_path))

from email_sender import EmailManager
import pandas as pd
from datetime import datetime, timedelta
from textwrap import fill

def get_business_days_to_effective(effective_date):
    """
    Calculate business days between today and effective date using NYSE calendar.
    
    Args:
        effective_date: datetime object of the effective date
        
    Returns:
        int: Number of business days, or None if date is invalid
    """
    if pd.isna(effective_date):
        return None
        
    # Get NYSE calendar
    nyse = mcal.get_calendar('NYSE')
    
    # Get today's date at midnight
    today = pd.Timestamp.now().normalize()
    effective = pd.Timestamp(effective_date).normalize()
    
    if effective < today:
        return None
    
    # Get valid business days between today and effective date
    business_days = nyse.valid_days(start_date=today, end_date=effective)
    return len(business_days)

def format_business_days(days):
    """Format business days count with trading day indicator."""
    if days is None:
        return 'N/A'
    return f"T-{days}td"  # td = trading days

def add_market_impact(df):
    """
    Add market impact indicators based on corporate action types.
    High impact: Mergers, Acquisitions, Spinoffs
    Medium impact: Stock Splits, Special Dividends
    Low impact: Regular Dividends, Minor Corporate Actions
    """
    high_impact = ['Merger', 'Acquisition', 'Spinoff']
    medium_impact = ['Split', 'Special Dividend']
    
    def get_impact(row):
        if row['TYPE'] in high_impact:
            return 'HIGH'
        elif row['TYPE'] in medium_impact:
            return 'MEDIUM'
        return 'LOW'
    
    df['Market Impact'] = df.apply(get_impact, axis=1)
    return df

def format_ticker_sequence(ticker_group):
    """Format all sequences for a single ticker with enhanced trading information."""
    ticker = ticker_group['Ticker'].iloc[0]
    company = ticker_group['Company Name'].iloc[0]
    impact = ticker_group['Market Impact'].iloc[0]
    
    # Header with impact indicator
    impact_color = {
        'HIGH': '🔴',    # Red circle
        'MEDIUM': '🟡',  # Yellow circle
        'LOW': '🟢'      # Green circle
    }
    
    ticker_text = [
        f"  {ticker} - {company}",
        f"  Market Impact: {impact_color.get(impact, '⚪')} {impact}"
    ]
    
    # Sort by sequence and effective date
    ticker_group = ticker_group.sort_values(['Sequence', 'EFFECTIVE DATE'])
    
    for _, row in ticker_group.iterrows():
        effective_date = pd.to_datetime(row['EFFECTIVE DATE']).strftime('%Y-%m-%d') if pd.notna(row['EFFECTIVE DATE']) else 'N/A'
        days_to_effective = (pd.to_datetime(row['EFFECTIVE DATE']) - datetime.now()).days if pd.notna(row['EFFECTIVE DATE']) else None
        
        # Add urgency indicator for near-term events
        urgency = ''
        if days_to_effective is not None:
            if days_to_effective <= 3:
                urgency = '⚠️ URGENT (≤ 3 days)'
            elif days_to_effective <= 7:
                urgency = '⚠️ Near-term (≤ 1 week)'
        
        comments = row['COMMENTS'] if pd.notna(row['COMMENTS']) else 'No comments'
        wrapped_comments = fill(comments, width=80, initial_indent='      ', subsequent_indent='      ')
        
        sequence_text = [
            f"    Sequence {row['Sequence']}:",
            f"      Effective Date: {effective_date} {urgency}",
            f"      Days Until Effective: {days_to_effective if days_to_effective is not None else 'N/A'}",
            f"      Comments:\n{wrapped_comments}"
        ]
        
        # Add other columns
        excluded_cols = ['TYPE', 'EFFECTIVE DATE', 'COMMENTS', 'Ticker', 'Company Name', 
                        'Sequence', 'Market Impact']
        other_cols = [col for col in ticker_group.columns if col not in excluded_cols]
        for col in other_cols:
            value = row[col] if pd.notna(row[col]) else 'N/A'
            sequence_text.append(f"      {col}: {value}")
            
        ticker_text.append('\n'.join(sequence_text))
    
    return '\n\n'.join(ticker_text)

def format_group_section(group_df):
    """
    Format a group of corporate actions by TYPE, organizing tickers and their sequences.
    
    Args:
        group_df: DataFrame containing rows for one TYPE
        
    Returns:
        str: Formatted section text
    """
    # Group by Ticker within the TYPE
    ticker_groups = []
    for ticker, ticker_group in group_df.groupby('Ticker'):
        ticker_text = format_ticker_sequence(ticker_group)
        ticker_groups.append(ticker_text)
    
    return '\n\n'.join(ticker_groups)

def create_summary_section(df):
    """Create a summary section with key statistics and urgent actions."""
    summary = []
    
    # Count by type
    type_counts = df['TYPE'].value_counts()
    summary.append("Summary of Corporate Actions:")
    for type_name, count in type_counts.items():
        summary.append(f"  • {type_name}: {count} action(s)")
    
    # Urgent actions (next 3 days)
    urgent_df = df[pd.to_datetime(df['EFFECTIVE DATE']) <= datetime.now() + timedelta(days=3)]
    if not urgent_df.empty:
        summary.append("\nUrgent Actions (Next 3 Days):")
        for _, row in urgent_df.iterrows():
            summary.append(
                f"  • {row['Ticker']} - {row['TYPE']} - "
                f"Effective: {pd.to_datetime(row['EFFECTIVE DATE']).strftime('%Y-%m-%d')}"
            )
    
    # High impact actions
    high_impact = df[df['Market Impact'] == 'HIGH']
    if not high_impact.empty:
        summary.append("\nHigh Impact Actions:")
        for _, row in high_impact.iterrows():
            summary.append(f"  • {row['Ticker']} - {row['TYPE']}")
    
    return '\n'.join(summary)

def create_html_summary(df):
    """Create an HTML summary section with business days to effective."""
    summary = ["""
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
            Summary of Corporate Actions
        </h2>
    """]
    
    # Count by type with progress bars
    type_counts = df['TYPE'].value_counts()
    total = len(df)
    summary.append('<div style="margin: 15px 0;">')
    for type_name, count in type_counts.items():
        percentage = (count / total) * 100
        summary.append(f"""
            <div style="margin: 5px 0;">
                <span style="display: inline-block; width: 120px;">{type_name}:</span>
                <span style="color: #2c3e50;"><b>{count}</b> action(s)</span>
                <div style="background-color: #e0e0e0; width: 200px; height: 10px; display: inline-block; margin: 0 10px;">
                    <div style="background-color: #3498db; width: {percentage}%; height: 10px;"></div>
                </div>
                <span>({percentage:.1f}%)</span>
            </div>
        """)
    summary.append('</div>')
    
    # Urgent actions (next 3 trading days)
    urgent_df = df.copy()
    urgent_df['business_days'] = urgent_df['EFFECTIVE DATE'].apply(get_business_days_to_effective)
    urgent_df = urgent_df[urgent_df['business_days'].between(0, 3)]
    
    if not urgent_df.empty:
        summary.append("""
            <div style="background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 15px 0;">
            <h3 style="color: #856404; margin-top: 0;">⚠️ Urgent Actions (Next 3 Trading Days)</h3>
            <ul style="list-style-type: none; padding-left: 0;">
        """)
        for _, row in urgent_df.iterrows():
            summary.append(f"""
                <li style="margin: 5px 0;">
                    <span style="color: #dc3545;">{format_business_days(row['business_days'])}</span> |
                    <b>{row['Ticker']}</b> - {row['TYPE']} -
                    Effective: {pd.to_datetime(row['EFFECTIVE DATE']).strftime('%Y-%m-%d')}
                </li>
            """)
        summary.append('</ul></div>')
    
    # High impact actions
    high_impact = df[df['Market Impact'] == 'HIGH']
    if not high_impact.empty:
        summary.append("""
            <div style="background-color: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; margin: 15px 0;">
            <h3 style="color: #721c24; margin-top: 0;">🔴 High Impact Actions</h3>
            <ul style="list-style-type: none; padding-left: 0;">
        """)
        for _, row in high_impact.iterrows():
            summary.append(f"""
                <li style="margin: 5px 0;">
                    <b>{row['Ticker']}</b> - {row['TYPE']} -
                    {row['Company Name']}
                </li>
            """)
        summary.append('</ul></div>')
    
    summary.append('</div>')
    return '\n'.join(summary)

def format_ticker_sequence_html(ticker_group):
    """Format ticker sequences with business days countdown."""
    ticker = ticker_group['Ticker'].iloc[0]
    company = ticker_group['Company Name'].iloc[0]
    impact = ticker_group['Market Impact'].iloc[0]
    
    impact_style = {
        'HIGH': 'background-color: #dc3545; color: white;',
        'MEDIUM': 'background-color: #ffc107; color: black;',
        'LOW': 'background-color: #28a745; color: white;'
    }
    
    html = [f"""
        <div style="margin: 20px 0; padding: 15px; background-color: white; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h3 style="margin: 0; color: #2c3e50;">
                    {ticker} - {company}
                </h3>
                <span style="padding: 5px 10px; border-radius: 3px; {impact_style.get(impact, '')}">
                    {impact}
                </span>
            </div>
    """]
    
    ticker_group = ticker_group.sort_values(['Sequence', 'EFFECTIVE DATE'])
    for _, row in ticker_group.iterrows():
        effective_date = pd.to_datetime(row['EFFECTIVE DATE'])
        business_days = get_business_days_to_effective(effective_date)
        
        urgency_style = ''
        if business_days is not None:
            if business_days <= 3:
                urgency_style = 'background-color: #dc3545; color: white;'
            elif business_days <= 5:
                urgency_style = 'background-color: #ffc107; color: black;'
        
        formatted_date = effective_date.strftime('%Y-%m-%d') if pd.notna(effective_date) else 'N/A'
        business_days_text = format_business_days(business_days)
        
        comments = row['COMMENTS'] if pd.notna(row['COMMENTS']) else 'No comments'
        
        html.append(f"""
            <div style="margin: 15px 0; padding: 10px; background-color: #f8f9fa; border-radius: 3px;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                    <span style="font-weight: bold;">Sequence {row['Sequence']}</span>
                    <span style="padding: 3px 8px; border-radius: 3px; {urgency_style}">
                        {business_days_text}
                    </span>
                </div>
                <div style="margin: 5px 0;">
                    <span style="color: #666;">Effective Date:</span> {formatted_date}
                    <span style="margin-left: 10px; font-size: 0.9em; color: #666;">
                        ({business_days_text} until effective)
                    </span>
                </div>
                <div style="margin: 10px 0; padding: 10px; background-color: white; border-left: 3px solid #3498db;">
                    {comments}
                </div>
        """)
        
        # Add other columns
        excluded_cols = ['TYPE', 'EFFECTIVE DATE', 'COMMENTS', 'Ticker', 'Company Name', 
                        'Sequence', 'Market Impact']
        other_cols = [col for col in ticker_group.columns if col not in excluded_cols]
        if other_cols:
            html.append('<div style="display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px;">')
            for col in other_cols:
                value = row[col] if pd.notna(row[col]) else 'N/A'
                html.append(f"""
                    <div style="background-color: #e9ecef; padding: 3px 8px; border-radius: 3px;">
                        <span style="color: #666;">{col}:</span> {value}
                    </div>
                """)
            html.append('</div>')
        
        html.append('</div>')
    
    html.append('</div>')
    return '\n'.join(html)

def format_group_section_html(group_df):
    """
    Format a group of corporate actions by TYPE, organizing tickers and their sequences in HTML.
    
    Args:
        group_df: DataFrame containing rows for one TYPE
        
    Returns:
        str: Formatted section text in HTML
    """
    # Group by Ticker within the TYPE
    ticker_groups = []
    for ticker, ticker_group in group_df.groupby('Ticker'):
        ticker_text = format_ticker_sequence_html(ticker_group)
        ticker_groups.append(ticker_text)
    
    return '\n\n'.join(ticker_groups)

def main():
    try:
        input_dir = Path(__file__).parent.parent / 'input'
        excel_path = input_dir / 'corporate_actions.xlsx'
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Input file not found: {excel_path}")
            
        df = pd.read_excel(excel_path)
        df = add_market_impact(df)
        df = df.sort_values(['Market Impact', 'TYPE', 'Ticker', 'Sequence'])
        
        # Create HTML email body
        html_body = ["""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1000px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
                S&P 500 Corporate Actions Report
                <br>
                <span style="font-size: 0.8em; color: #666;">
                    {date}
                </span>
            </h1>
        """]
        
        # Add HTML summary section
        html_body.append(create_html_summary(df))
        
        # Add detailed sections
        for type_name, group in df.groupby('TYPE'):
            html_body.append(f"""
                <div style="margin: 30px 0;">
                    <h2 style="color: #2c3e50; background-color: #f8f9fa; padding: 10px; border-left: 4px solid #3498db;">
                        {type_name}
                    </h2>
                    {format_group_section_html(group)}
                </div>
            """)
        
        html_body.append('</body></html>')
        final_html = '\n'.join(html_body)
        
        # Save both HTML and plain text versions
        output_dir = Path(__file__).parent.parent / 'output'
        html_path = output_dir / f'corporate_actions_report_{datetime.now().strftime("%Y%m%d")}.html'
        html_path.write_text(final_html)
        
        # Setup email manager with HTML content
        email_mgr = EmailManager()
        email_mgr.register_template(
            'corporate_actions',
            'S&P 500 Corporate Actions Report - {date}',
            final_html,
            ['your.email@company.com'],
            is_html=True  # Add this parameter to EmailManager
        )
        
        email_mgr.send_email(
            'corporate_actions',
            data={'date': datetime.now().strftime('%Y-%m-%d')},
            attachments=[excel_path, html_path]  # Attach both Excel and HTML report
        )
        
        print(f"Corporate actions report sent successfully at {datetime.now()}")
        print(f"Report saved to: {html_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 