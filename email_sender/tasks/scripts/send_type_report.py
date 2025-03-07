import sys
from pathlib import Path
import numpy as np
import pandas_market_calendars as mcal
import matplotlib.pyplot as plt
import io
import base64
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns
from datetime import datetime, timedelta
import json
import functools
import os

# Add src directory to Python path
src_path = Path(__file__).parent.parent.parent / 'src'
sys.path.append(str(src_path))

from email_sender import EmailManager
import pandas as pd
from textwrap import fill

# Global cache for NYSE calendar
_nyse_calendar_cache = None
_nyse_calendar_cache_date = None

def get_report_config():
    """Get configuration for this report from email_config.json"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'email_config.json'
    
    # Create config directory if it doesn't exist
    config_dir = config_path.parent
    if not config_dir.exists():
        config_dir.mkdir(parents=True)
    
    # If config file doesn't exist, create it with default values
    if not config_path.exists():
        default_config = {
            "corporate_actions_report": {
                "email_recipients": ["your.email@company.com"],
                "thresholds": {
                    "high_impact_percentile": 0.75,
                    "low_impact_percentile": 0.25,
                    "urgent_days": 3,
                    "warning_days": 5
                },
                "visualization": {
                    "chart_colors": ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6", "#1abc9c", "#34495e", "#e67e22"],
                    "high_impact_color": "#e74c3c",
                    "medium_impact_color": "#f39c12",
                    "low_impact_color": "#2ecc71",
                    "undefined_impact_color": "#7f8c8d"
                },
                "stock_profile_base_url": "https://finance.yahoo.com/quote/",
                "input_file": "corporate_actions.xlsx"
            }
        }
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
    
    # Load the config
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get("corporate_actions_report", {})
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        # Return default config on error
        return {
            "email_recipients": ["your.email@company.com"],
            "thresholds": {
                "high_impact_percentile": 0.75,
                "low_impact_percentile": 0.25,
                "urgent_days": 3,
                "warning_days": 5
            },
            "visualization": {
                "chart_colors": ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6", "#1abc9c", "#34495e", "#e67e22"],
                "high_impact_color": "#e74c3c",
                "medium_impact_color": "#f39c12",
                "low_impact_color": "#2ecc71",
                "undefined_impact_color": "#7f8c8d"
            },
            "stock_profile_base_url": "https://finance.yahoo.com/quote/",
            "input_file": "corporate_actions.xlsx"
        }

# Load the configuration
CONFIG = get_report_config()

# Set up Matplotlib styles for better visuals
plt.style.use('ggplot')
COLORS = CONFIG["visualization"]["chart_colors"]

def get_nyse_calendar(force_refresh=False):
    """
    Get NYSE calendar with caching for improved performance.
    
    Args:
        force_refresh: Force a refresh of the cached calendar
        
    Returns:
        NYSE calendar object
    """
    global _nyse_calendar_cache, _nyse_calendar_cache_date
    
    # If we have no cache or it's more than 24 hours old, or force refresh is requested
    current_date = datetime.now().date()
    if (_nyse_calendar_cache is None or 
        _nyse_calendar_cache_date is None or 
        current_date != _nyse_calendar_cache_date or 
        force_refresh):
        
        print("Refreshing NYSE calendar cache...")
        _nyse_calendar_cache = mcal.get_calendar('NYSE')
        _nyse_calendar_cache_date = current_date
    
    return _nyse_calendar_cache

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
        
    # Get NYSE calendar with caching
    nyse = get_nyse_calendar()
    
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
    Add market impact indicators based on ABS NET FLOW.
    
    Args:
        df: DataFrame containing corporate actions
        
    Returns:
        DataFrame with 'Market Impact' column added
    """
    # Get thresholds from config
    high_threshold_pct = CONFIG["thresholds"]["high_impact_percentile"]
    low_threshold_pct = CONFIG["thresholds"]["low_impact_percentile"]
    
    # Convert to numeric if needed
    if 'ABS NET FLOW' in df.columns:
        df['ABS NET FLOW'] = pd.to_numeric(df['ABS NET FLOW'], errors='coerce')
        
        # Define thresholds for impact levels based on config
        high_threshold = df['ABS NET FLOW'].quantile(high_threshold_pct)
        low_threshold = df['ABS NET FLOW'].quantile(low_threshold_pct)
        
        def get_impact(flow):
            if pd.isna(flow):
                return 'UNDEFINED'
            elif flow >= high_threshold:
                return 'HIGH'
            elif flow <= low_threshold:
                return 'LOW'
            else:
                return 'MEDIUM'
                
        df['Market Impact'] = df['ABS NET FLOW'].apply(get_impact)
        
        # Add percentile ranking for each flow
        df['Flow Percentile'] = df['ABS NET FLOW'].rank(pct=True).round(3) * 100
    else:
        # Fallback if ABS NET FLOW is not available
        df['Market Impact'] = 'UNDEFINED'
        df['Flow Percentile'] = None
    
    return df

def calculate_insights(df):
    """Calculate additional insights from the data."""
    insights = {}
    
    # Only proceed if we have the necessary columns
    if 'ACTION TYPE' not in df.columns or df.empty:
        return insights
    
    # 1. Total absolute flow by action type
    if 'ABS NET FLOW' in df.columns:
        flow_by_type = df.groupby('ACTION TYPE')['ABS NET FLOW'].sum().sort_values(ascending=False)
        insights['flow_by_type'] = flow_by_type
        
        # 2. Which action types have the highest average impact
        avg_flow_by_type = df.groupby('ACTION TYPE')['ABS NET FLOW'].mean().sort_values(ascending=False)
        insights['avg_flow_by_type'] = avg_flow_by_type
    
    # 3. Timeline of actions - when are most actions happening
    if 'EFFECTIVE DATE' in df.columns:
        df_dates = df.dropna(subset=['EFFECTIVE DATE'])
        if not df_dates.empty:
            date_counts = df_dates.groupby('EFFECTIVE DATE').size()
            insights['action_timeline'] = date_counts
    
    # 4. Actions by status
    if 'STATUS' in df.columns:
        status_counts = df['STATUS'].value_counts()
        insights['status_counts'] = status_counts
    
    # 5. Calculate weighted average IWF change
    if all(col in df.columns for col in ['CURRENT IWF', 'NEW IWF', 'ABS NET FLOW']):
        df_iwf = df.dropna(subset=['CURRENT IWF', 'NEW IWF', 'ABS NET FLOW'])
        if not df_iwf.empty:
            df_iwf['IWF_CHANGE'] = df_iwf['NEW IWF'] - df_iwf['CURRENT IWF']
            # Weight by ABS NET FLOW
            weighted_iwf_change = (df_iwf['IWF_CHANGE'] * df_iwf['ABS NET FLOW']).sum() / df_iwf['ABS NET FLOW'].sum()
            insights['weighted_iwf_change'] = weighted_iwf_change
    
    # 6. Growth vs Value distribution
    if all(col in df.columns for col in ['GROWTH', 'VALUE']):
        df_style = df.dropna(subset=['GROWTH', 'VALUE'])
        if not df_style.empty:
            avg_growth = df_style['GROWTH'].mean()
            avg_value = df_style['VALUE'].mean()
            insights['style_distribution'] = {'growth': avg_growth, 'value': avg_value}
    
    return insights

def generate_charts(df, insights):
    """Generate charts and graphs from the data and convert to base64 for HTML embedding."""
    charts = {}
    
    # 1. Flow Distribution by Action Type (Pie Chart)
    if 'flow_by_type' in insights and not insights['flow_by_type'].empty:
        plt.figure(figsize=(8, 6))
        insights['flow_by_type'].plot(kind='pie', autopct='%1.1f%%', colors=COLORS, 
                                     startangle=90, wedgeprops={'edgecolor': 'white'})
        plt.title('ABS NET FLOW Distribution by Action Type', fontsize=14)
        plt.ylabel('')
        plt.tight_layout()
        
        # Convert to base64
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=100)
        buffer.seek(0)
        image_png = buffer.getvalue()
        buffer.close()
        plt.close()
        
        charts['flow_distribution'] = base64.b64encode(image_png).decode('utf-8')
    
    # 2. Timeline of Corporate Actions
    if 'action_timeline' in insights and not insights['action_timeline'].empty:
        timeline = insights['action_timeline']
        plt.figure(figsize=(10, 4))
        
        # If we have more than one date, show as bars
        if len(timeline) > 1:
            bars = timeline.plot(kind='bar', color='#3498db')
            
            # Add urgent highlighting for dates within 3 trading days
            urgent_dates = []
            for date in timeline.index:
                days = get_business_days_to_effective(date)
                if days is not None and days <= 3:
                    urgent_dates.append(date)
            
            if urgent_dates:
                urgent_subset = timeline[urgent_dates]
                urgent_subset.plot(kind='bar', color='#e74c3c', ax=bars)
            
            plt.title('Number of Corporate Actions by Date', fontsize=14)
            plt.ylabel('Number of Actions')
            plt.xlabel('Effective Date')
            plt.xticks(rotation=45)
        else:
            # Just one date, show as text
            plt.text(0.5, 0.5, f"All actions effective on {timeline.index[0].strftime('%Y-%m-%d')}", 
                    fontsize=14, ha='center')
            plt.axis('off')
        
        plt.tight_layout()
        
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=100)
        buffer.seek(0)
        image_png = buffer.getvalue()
        buffer.close()
        plt.close()
        
        charts['action_timeline'] = base64.b64encode(image_png).decode('utf-8')
    
    return charts

def create_html_dashboard(df, insights, charts):
    """Create an HTML dashboard with insights and visualizations."""
    dashboard = ["""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 0;">
            Corporate Actions Dashboard
        </h2>
        
        <div style="display: flex; flex-wrap: wrap; justify-content: space-between; margin-top: 15px;">
    """]
    
    # Key Statistics Cards
    dashboard.append("""
        <div style="flex: 1; min-width: 250px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <h3 style="color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0;">
                Key Statistics
            </h3>
            <ul style="list-style-type: none; padding-left: 0;">
    """)
    
    # Total number of actions
    dashboard.append(f"<li style='margin: 8px 0;'><b>Total Actions:</b> {len(df)}</li>")
    
    # Actions by status
    if 'STATUS' in df.columns:
        status_counts = df['STATUS'].value_counts()
        for status, count in status_counts.items():
            dashboard.append(f"<li style='margin: 8px 0;'><b>{status} Actions:</b> {count}</li>")
    
    # High impact actions
    high_impact_count = len(df[df['Market Impact'] == 'HIGH'])
    if high_impact_count > 0:
        dashboard.append(f"<li style='margin: 8px 0;'><b>High Impact Actions:</b> {high_impact_count}</li>")
    
    # Urgent actions (next 3 trading days)
    if 'EFFECTIVE DATE' in df.columns:
        urgent_df = df.copy()
        urgent_df['business_days'] = urgent_df['EFFECTIVE DATE'].apply(get_business_days_to_effective)
        urgent_count = len(urgent_df[urgent_df['business_days'].between(0, 3)])
        if urgent_count > 0:
            dashboard.append(f"""<li style='margin: 8px 0;'><b>Urgent Actions:</b> {urgent_count} 
                            <span style='color: #e74c3c; font-size: 0.9em;'>(≤ 3 trading days)</span></li>""")
    
    dashboard.append("</ul></div>")
    
    # Market Impact Analysis Card
    if 'ABS NET FLOW' in df.columns:
        dashboard.append("""
            <div style="flex: 1; min-width: 250px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0;">
                    Market Impact Analysis
                </h3>
                <ul style="list-style-type: none; padding-left: 0;">
        """)
        
        # Total ABS NET FLOW
        total_flow = df['ABS NET FLOW'].sum()
        if not pd.isna(total_flow):
            dashboard.append(f"""<li style='margin: 8px 0;'><b>Total ABS NET FLOW:</b> 
                              ${total_flow/1e9:.2f}B</li>""")
        
        # Highest flow
        max_flow_row = df.loc[df['ABS NET FLOW'].idxmax()]
        if 'CURRENT TICKER' in max_flow_row and not pd.isna(max_flow_row['ABS NET FLOW']):
            dashboard.append(f"""<li style='margin: 8px 0;'><b>Highest Flow:</b> 
                              {max_flow_row['CURRENT TICKER']} (${max_flow_row['ABS NET FLOW']/1e9:.2f}B)</li>""")
        
        # Average flow
        avg_flow = df['ABS NET FLOW'].mean()
        if not pd.isna(avg_flow):
            dashboard.append(f"""<li style='margin: 8px 0;'><b>Average Flow:</b> 
                              ${avg_flow/1e6:.2f}M</li>""")
        
        # IWF change if available
        if 'weighted_iwf_change' in insights:
            dashboard.append(f"""<li style='margin: 8px 0;'><b>IWF Average Change:</b> 
                              {insights['weighted_iwf_change']:.3f}</li>""")
        
        dashboard.append("</ul></div>")
    
    # Close first row of cards
    dashboard.append("</div>")
    
    # Charts Row
    dashboard.append("""
        <div style="display: flex; flex-wrap: wrap; justify-content: space-between; margin-top: 20px;">
    """)
    
    # Flow Distribution Chart
    if 'flow_distribution' in charts:
        dashboard.append(f"""
            <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0;">
                    ABS NET FLOW Distribution
                </h3>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="data:image/png;base64,{charts['flow_distribution']}" style="max-width: 100%; height: auto;">
                </div>
            </div>
        """)
    
    # Timeline Chart
    if 'action_timeline' in charts:
        dashboard.append(f"""
            <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0;">
                    Timeline of Actions
                </h3>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="data:image/png;base64,{charts['action_timeline']}" style="max-width: 100%; height: auto;">
                </div>
            </div>
        """)
    
    # Removed: Growth/Value Distribution Chart
    # Removed: Correlation Heatmap
    
    # Close charts row
    dashboard.append("</div>")
    
    # Close main dashboard div
    dashboard.append("</div>")
    
    return '\n'.join(dashboard)

def create_html_summary(df):
    """Create HTML summary section with key insights."""
    html = ["""
        <div class="card">
            <div class="section-header">
                <h2 style="margin: 0;">Corporate Actions Summary</h2>
            </div>
    """]
    
    # Action Type Distribution
    if 'ACTION TYPE' in df.columns:
        type_counts = df['ACTION TYPE'].value_counts()
        total_count = len(df)
        
        html.append("""
            <div style="margin: 15px 0;">
                <h3 style="color: #2c3e50; margin-bottom: 10px;">Action Type Distribution</h3>
        """)
        
        for action_type, count in type_counts.items():
            percentage = (count / total_count) * 100
            html.append(f"""
                <div style="margin-bottom: 8px;">
                    <span style="display: inline-block; width: 150px;">{action_type}:</span>
                    <span style="display: inline-block; width: 80px;">{count} action(s)</span>
                    <span style="display: inline-block; width: 200px; background-color: #f1f1f1; border-radius: 3px;">
                        <span style="display: block; width: {percentage}%; background-color: #3498db; height: 20px; border-radius: 3px;"></span>
                    </span>
                    <span style="margin-left: 10px;">({percentage:.1f}%)</span>
                </div>
            """)
            
        html.append('</div>')
    
    # Urgent Actions
    if 'EFFECTIVE DATE' in df.columns:
        # Use urgent_days from config
        urgent_days_threshold = CONFIG["thresholds"]["urgent_days"]
        
        urgent_df = df.copy()
        urgent_df['business_days'] = urgent_df['EFFECTIVE DATE'].apply(get_business_days_to_effective)
        urgent_actions = urgent_df[urgent_df['business_days'].between(0, urgent_days_threshold)]
        
        if not urgent_actions.empty:
            html.append(f"""
                <div style="margin: 15px 0;">
                    <h3 style="color: #2c3e50; margin-bottom: 10px;">
                        <span style="color: {CONFIG["visualization"]["high_impact_color"]};">⚠️ Urgent Actions</span>
                        <span style="font-size: 0.8em; font-weight: normal;">(Next {urgent_days_threshold} Trading Days)</span>
                    </h3>
                    <ul style="margin-top: 5px; padding-left: 20px;">
            """)
            
            # Sort by business days and then ticker
            urgent_actions = urgent_actions.sort_values(['business_days', 'CURRENT TICKER'])
            
            for _, row in urgent_actions.iterrows():
                ticker = row['CURRENT TICKER'] if pd.notna(row['CURRENT TICKER']) else 'Unknown'
                action_type = row['ACTION TYPE'] if pd.notna(row['ACTION TYPE']) else 'Unknown'
                days = format_business_days(row['business_days'])
                date_str = pd.to_datetime(row['EFFECTIVE DATE']).strftime('%Y-%m-%d') if pd.notna(row['EFFECTIVE DATE']) else 'Unknown'
                
                ticker_html = format_ticker_with_link(ticker)
                
                html.append(f"""
                    <li style="margin: 5px 0;">
                        <b>{days}</b> | {ticker_html} - {action_type} - Effective: {date_str}
                    </li>
                """)
                
            html.append('</ul></div>')
    
    # High Impact Actions
    if 'ABS NET FLOW' in df.columns and 'Market Impact' in df.columns:
        high_impact = df[df['Market Impact'] == 'HIGH']
        
        if not high_impact.empty:
            html.append(f"""
                <div style="margin: 15px 0;">
                    <h3 style="color: #2c3e50; margin-bottom: 10px;">
                        <span style="color: {CONFIG["visualization"]["high_impact_color"]};">🔴 High Impact Actions</span>
                        <span style="font-size: 0.8em; font-weight: normal;">(Largest ABS NET FLOW)</span>
                    </h3>
                    <ul style="margin-top: 5px; padding-left: 20px;">
            """)
            
            # Sort by ABS NET FLOW descending
            high_impact = high_impact.sort_values('ABS NET FLOW', ascending=False)
            max_flow = high_impact['ABS NET FLOW'].max()
            
            for _, row in high_impact.iterrows():
                ticker = row['CURRENT TICKER'] if pd.notna(row['CURRENT TICKER']) else 'Unknown'
                action_type = row['ACTION TYPE'] if pd.notna(row['ACTION TYPE']) else 'Unknown'
                company = row['CURRENT COMPANY NAME'] if pd.notna(row['CURRENT COMPANY NAME']) else ''
                flow = row['ABS NET FLOW'] if pd.notna(row['ABS NET FLOW']) else 0
                relative_pct = (flow / max_flow) * 100 if max_flow > 0 else 0
                
                ticker_html = format_ticker_with_link(ticker)
                
                html.append(f"""
                    <li style="margin: 5px 0;">
                        <b>{ticker_html}</b> - {action_type} - {company} - 
                        Flow: {flow:,.0f} ({relative_pct:.1f}%)
                    </li>
                """)
                
            html.append('</ul></div>')
    
    html.append('</div>')
    return '\n'.join(html)

def format_ticker_sequence_html(ticker_group):
    """Format a group of rows for a single ticker in HTML."""
    if ticker_group.empty:
        return ""
    
    first_row = ticker_group.iloc[0]
    ticker = first_row['CURRENT TICKER'] if pd.notna(first_row['CURRENT TICKER']) else 'Unknown'
    company_name = first_row['CURRENT COMPANY NAME'] if pd.notna(first_row['CURRENT COMPANY NAME']) else ''
    
    # Get impact color from config
    impact_colors = {
        'HIGH': CONFIG["visualization"]["high_impact_color"],
        'MEDIUM': CONFIG["visualization"]["medium_impact_color"],
        'LOW': CONFIG["visualization"]["low_impact_color"],
        'UNDEFINED': CONFIG["visualization"]["undefined_impact_color"]
    }
    
    # Get the market impact
    market_impact = first_row['Market Impact'] if 'Market Impact' in first_row and pd.notna(first_row['Market Impact']) else 'UNDEFINED'
    impact_color = impact_colors.get(market_impact, impact_colors['UNDEFINED'])
    
    # Format ABS NET FLOW
    flow_text = ""
    if 'ABS NET FLOW' in first_row and pd.notna(first_row['ABS NET FLOW']):
        formatted_flow = f"{first_row['ABS NET FLOW']:,.0f}" if isinstance(first_row['ABS NET FLOW'], (int, float)) else str(first_row['ABS NET FLOW'])
        flow_text = f"(ABS NET FLOW: {formatted_flow})"
    
    # Create ticker link
    ticker_html = format_ticker_with_link(ticker)
    
    # Format impact emoji
    impact_emoji = {
        'HIGH': '🔴',
        'MEDIUM': '🟡',
        'LOW': '🟢',
        'UNDEFINED': '⚪'
    }.get(market_impact, '⚪')
    
    html = [f"""
        <div style="border: 1px solid #e0e0e0; border-radius: 5px; padding: 15px; margin-bottom: 20px; background-color: white;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div>
                    <span style="font-size: 1.2em; font-weight: bold; color: #2c3e50;">{ticker_html}</span>
                    <span style="margin-left: 10px; color: #7f8c8d;">{company_name}</span>
                </div>
                <div>
                    <span style="background-color: {impact_color}; color: {'white' if market_impact in ['HIGH', 'MEDIUM'] else 'black'}; padding: 3px 8px; border-radius: 3px; font-weight: bold;">
                        {impact_emoji} {market_impact}
                    </span>
                    <span style="margin-left: 5px; font-size: 0.9em; color: #666;">{flow_text}</span>
                </div>
            </div>
    """]
    
    # Format all rows for this ticker
    for _, row in ticker_group.iterrows():
        html.append(format_row_details_html(row))
    
    html.append('</div>')
    return '\n'.join(html)

def format_group_section_html(group_df):
    """Format a group of corporate actions by ACTION TYPE in HTML."""
    ticker_groups = []
    for ticker, ticker_group in group_df.groupby('CURRENT TICKER'):
        ticker_text = format_ticker_sequence_html(ticker_group)
        ticker_groups.append(ticker_text)
    
    return '\n\n'.join(ticker_groups)

def format_ticker_with_link(ticker):
    """Format ticker as a link to stock profile page."""
    if pd.isna(ticker):
        return 'N/A'
    
    base_url = CONFIG.get("stock_profile_base_url", "https://finance.yahoo.com/quote/")
    ticker_clean = str(ticker).strip()
    
    return f'<a href="{base_url}{ticker_clean}" target="_blank" style="color: #3498db; text-decoration: none;">{ticker_clean}</a>'

def main():
    try:
        input_dir = Path(__file__).parent.parent / 'input'
        excel_path = input_dir / CONFIG["input_file"]
        
        if not excel_path.exists():
            raise FileNotFoundError(f"Input file not found: {excel_path}")
            
        # Read Excel file with proper type conversion
        df = pd.read_excel(excel_path)
        
        # Ensure EFFECTIVE DATE is datetime
        if 'EFFECTIVE DATE' in df.columns:
            df['EFFECTIVE DATE'] = pd.to_datetime(df['EFFECTIVE DATE'], errors='coerce')
        
        # Convert numeric columns to appropriate types
        numeric_columns = [
            'CURRENT IWF', 'NEW IWF', 'CURRENT AWF', 'NEW AWF', 
            'CURRENT INDEX SHARES', 'NEW INDEX SHARES', 'INDEX SHARES PRE EVENTS',
            'SHARES POST EVENTS', 'ABS NET FLOW', 'GROWTH', 'VALUE'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                # Try to convert to integer first
                try:
                    # Check if all values (excluding NaN) can be integers
                    non_na_values = df[col].dropna()
                    if all(non_na_values == non_na_values.astype(int)):
                        df[col] = df[col].fillna(pd.NA).astype('Int64')
                    else:
                        # If not all values can be integers, use float
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    # Fallback to float if conversion fails
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add market impact analysis
        df = add_market_impact(df)
        
        # Calculate additional insights
        insights = calculate_insights(df)
        
        # Generate charts and visualizations
        charts = generate_charts(df, insights)
        
        # Sort by impact and type
        sort_columns = ['Market Impact', 'ACTION TYPE', 'CURRENT TICKER']
        if 'Sequence' in df.columns:
            sort_columns.append('Sequence')
        df = df.sort_values(sort_columns)
        
        # Create HTML email body
        html_body = ["""
            <html>
            <head>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
                    body {
                        font-family: 'Roboto', Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 1000px;
                        margin: 0 auto;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }
                    h1, h2, h3, h4 {
                        color: #2c3e50;
                        margin-top: 1.5em;
                        margin-bottom: 0.5em;
                    }
                    h1 {
                        text-align: center;
                        border-bottom: 3px solid #3498db;
                        padding-bottom: 15px;
                        margin-top: 0;
                    }
                    .card {
                        background-color: white;
                        border-radius: 8px;
                        padding: 20px;
                        margin-bottom: 20px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    .section-header {
                        background-color: #f8f9fa;
                        padding: 10px 15px;
                        border-left: 4px solid #3498db;
                        margin-bottom: 15px;
                        border-radius: 4px;
                    }
                    a:hover {
                        text-decoration: underline !important;
                    }
                </style>
            </head>
            <body>
            <h1>
                S&P 500 Corporate Actions Report
                <br>
                <span style="font-size: 0.8em; color: #666;">
                    {date}
                </span>
            </h1>
        """]
        
        # Add interactive dashboard
        html_body.append(create_html_dashboard(df, insights, charts))
        
        # Add HTML summary section
        html_body.append(create_html_summary(df))
        
        # Add detailed sections
        for type_name, group in df.groupby('ACTION TYPE'):
            html_body.append(f"""
                <div class="card">
                    <div class="section-header">
                        <h2 style="margin: 0;">{type_name}</h2>
                    </div>
                    {format_group_section_html(group)}
                </div>
            """)
        
        html_body.append('</body></html>')
        final_html = '\n'.join(html_body)
        
        # Save HTML report
        output_dir = Path(__file__).parent.parent / 'output'
        output_dir.mkdir(exist_ok=True)
        html_path = output_dir / f'corporate_actions_report_{datetime.now().strftime("%Y%m%d")}.html'
        html_path.write_text(final_html)
        
        # Setup email manager with HTML content
        email_mgr = EmailManager()
        email_mgr.register_template(
            'corporate_actions',
            'S&P 500 Corporate Actions Report - {date}',
            final_html,
            CONFIG["email_recipients"],  # Use recipients from config
            is_html=True
        )
        
        email_mgr.send_email(
            'corporate_actions',
            data={'date': datetime.now().strftime('%Y-%m-%d')},
            attachments=[excel_path, html_path]
        )
        
        print(f"Corporate actions report sent successfully at {datetime.now()}")
        print(f"Report saved to: {html_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 