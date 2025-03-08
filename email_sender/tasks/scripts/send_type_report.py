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
    """Generate visualization charts for the report."""
    charts = {}
    
    # 1. Type Distribution Pie Chart
    try:
        if not df.empty and 'ACTION TYPE' in df.columns:
            type_counts = df['ACTION TYPE'].value_counts()
            
            # Create the pie chart figure
            fig, ax = plt.subplots(figsize=(6, 4))
            wedges, texts, autotexts = ax.pie(
                type_counts, 
                labels=type_counts.index, 
                autopct='%1.1f%%',
                startangle=90,
                colors=COLORS[:len(type_counts)]
            )
            
            # Style the chart
            ax.axis('equal')
            plt.setp(autotexts, size=9, weight="bold", color="white")
            plt.setp(texts, size=10)
            
            # Add count to each label
            for i, text in enumerate(texts):
                text.set_text(f"{text.get_text()} ({type_counts.values[i]})")
            
            plt.title('Distribution of Action Types')
            
            # Save to base64 for embedding in HTML
            buffer = io.BytesIO()
            plt.tight_layout()
            plt.savefig(buffer, format='png', dpi=100)
            buffer.seek(0)
            img_str = base64.b64encode(buffer.read()).decode()
            charts['type_distribution'] = img_str
            plt.close(fig)
    except Exception as e:
        print(f"Error generating type distribution chart: {e}")
    
    # 2. Net Flow by Action Type - Only generate if we have flow data
    try:
        if 'ABS NET FLOW' in df.columns and not df['ABS NET FLOW'].dropna().empty:
            # Group by action type and sum the net flow
            flow_by_type = df.groupby('ACTION TYPE')['ABS NET FLOW'].sum().sort_values(ascending=False)
            
            # Skip chart generation if all flow values are NaN or zero
            if flow_by_type.sum() > 0:
                # Create the bar chart
                fig, ax = plt.subplots(figsize=(7, 5))
                bars = ax.bar(
                    flow_by_type.index,
                    flow_by_type.values,
                    color=COLORS[:len(flow_by_type)]
                )
                
                # Add value labels on top of bars
                for bar in bars:
                    height = bar.get_height()
                    if not pd.isna(height) and height > 0:
                        ax.text(
                            bar.get_x() + bar.get_width() / 2.,
                            height * 1.01,
                            f'${height/1e6:.1f}M',
                            ha='center', va='bottom', 
                            fontsize=8, rotation=0
                        )
                
                plt.title('Net Flow by Action Type')
                plt.xticks(rotation=45, ha='right')
                plt.ylabel('Absolute Net Flow')
                plt.tight_layout()
                
                # Save to base64
                buffer = io.BytesIO()
                plt.savefig(buffer, format='png', dpi=100)
                buffer.seek(0)
                img_str = base64.b64encode(buffer.read()).decode()
                charts['flow_by_type'] = img_str
                plt.close(fig)
            else:
                print("Skipping net flow chart - no positive flow values found")
    except Exception as e:
        print(f"Error generating net flow chart: {e}")
    
    # 3. Timeline Chart (Days to Effective Date)
    try:
        # Filter out entries with invalid effective dates
        timeline_df = df[df['EFFECTIVE DATE'].notna()].copy()
        
        if not timeline_df.empty:
            # Calculate business days to effective date
            timeline_df['Business Days'] = timeline_df['EFFECTIVE DATE'].apply(get_business_days_to_effective)
            timeline_df = timeline_df[timeline_df['Business Days'].notna()]
            
            if not timeline_df.empty:
                # Group by days and count
                days_counts = timeline_df.groupby('Business Days').size()
                
                # Create figure
                fig, ax = plt.subplots(figsize=(8, 4))
                
                # Color urgent actions differently
                colors = []
                for day in days_counts.index:
                    if day <= CONFIG["thresholds"]["urgent_days"]:
                        colors.append(CONFIG["visualization"]["high_impact_color"])
                    elif day <= CONFIG["thresholds"]["warning_days"]:
                        colors.append(CONFIG["visualization"]["medium_impact_color"])
                    else:
                        colors.append(CONFIG["visualization"]["low_impact_color"])
                
                bars = ax.bar(days_counts.index, days_counts.values, color=colors)
                
                # Add count labels above bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.,
                        height * 1.01,
                        f'{int(height)}',
                        ha='center', va='bottom', 
                        fontsize=9
                    )
                
                # Add vertical lines for thresholds
                ax.axvline(x=CONFIG["thresholds"]["urgent_days"] + 0.5, color='#e74c3c', linestyle='--', alpha=0.5)
                ax.axvline(x=CONFIG["thresholds"]["warning_days"] + 0.5, color='#f39c12', linestyle='--', alpha=0.5)
                
                plt.title('Timeline of Upcoming Corporate Actions')
                plt.xlabel('Business Days Until Effective')
                plt.ylabel('Number of Actions')
                plt.tight_layout()
                
                # Save to base64
                buffer = io.BytesIO()
                plt.savefig(buffer, format='png', dpi=100)
                buffer.seek(0)
                img_str = base64.b64encode(buffer.read()).decode()
                charts['timeline'] = img_str
                plt.close(fig)
    except Exception as e:
        print(f"Error generating timeline chart: {e}")
    
    return charts

def create_placeholder_chart(message):
    """Create a simple placeholder chart when data is missing."""
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.text(0.5, 0.5, message, ha='center', va='center', fontsize=12)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # Save to base64
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    buffer.seek(0)
    img_str = base64.b64encode(buffer.read()).decode()
    plt.close(fig)
    return img_str

def create_error_chart(error_message):
    """Create a chart indicating an error occurred."""
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.text(0.5, 0.5, error_message, ha='center', va='center', fontsize=10, color='red')
    ax.text(0.5, 0.4, "Chart generation failed", ha='center', va='center', fontsize=8)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # Save to base64
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    buffer.seek(0)
    img_str = base64.b64encode(buffer.read()).decode()
    plt.close(fig)
    return img_str

def create_html_dashboard(df, insights, charts):
    """Create HTML dashboard with charts and key insights."""
    
    dashboard = ['<div class="card"><h2>Dashboard</h2><div style="display: flex; flex-wrap: wrap;">']
    
    # Add summary stats
    dashboard.append(f"""
        <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 0;">
                Summary
            </h3>
            <ul style="list-style-type: none; padding-left: 0;">
                <li style="margin-bottom: 10px; display: flex; align-items: center;">
                    <span style="background-color: {CONFIG["visualization"]["high_impact_color"]}; color: white; border-radius: 50%; width: 24px; height: 24px; display: inline-flex; justify-content: center; align-items: center; margin-right: 10px;">!</span>
                    <strong>High Impact Actions:</strong> {insights.get('high_impact_count', 0)}
                </li>
                <li style="margin-bottom: 10px; display: flex; align-items: center;">
                    <span style="background-color: {CONFIG["visualization"]["medium_impact_color"]}; color: white; border-radius: 50%; width: 24px; height: 24px; display: inline-flex; justify-content: center; align-items: center; margin-right: 10px;">•</span>
                    <strong>Urgent Actions:</strong> {insights.get('urgent_count', 0)} (due within {CONFIG["thresholds"]["urgent_days"]} trading days)
                </li>
                <li style="margin-bottom: 10px; display: flex; align-items: center;">
                    <span style="background-color: #3498db; color: white; border-radius: 50%; width: 24px; height: 24px; display: inline-flex; justify-content: center; align-items: center; margin-right: 10px;">•</span>
                    <strong>Total Actions:</strong> {len(df)}
                </li>
                <li style="margin-bottom: 10px; display: flex; align-items: center;">
                    <span style="background-color: #95a5a6; color: white; border-radius: 50%; width: 24px; height: 24px; display: inline-flex; justify-content: center; align-items: center; margin-right: 10px;">•</span>
                    <strong>Unique Action Types:</strong> {df['ACTION TYPE'].nunique()}
                </li>
            </ul>
        </div>
    """)
    
    # Types Distribution Chart
    if 'type_distribution' in charts:
        dashboard.append(f"""
            <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 0;">
                    Action Types
                </h3>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="data:image/png;base64,{charts['type_distribution']}" style="max-width: 100%; height: auto;">
                </div>
            </div>
        """)
    
    # Flow Distribution Chart - only include if exists
    if 'flow_by_type' in charts:
        dashboard.append(f"""
            <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 0;">
                    Net Flow by Action Type
                </h3>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="data:image/png;base64,{charts['flow_by_type']}" style="max-width: 100%; height: auto;">
                </div>
            </div>
        """)
    
    # Timeline Chart - only include if exists
    if 'timeline' in charts:
        dashboard.append(f"""
            <div style="flex: 1; min-width: 300px; margin: 10px; background-color: white; border-radius: 8px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <h3 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 0;">
                    Action Timeline
                </h3>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="data:image/png;base64,{charts['timeline']}" style="max-width: 100%; height: auto;">
                </div>
            </div>
        """)
    
    dashboard.append('</div></div>')
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

def format_row_details_html(row):
    """Format the details of a single row in HTML with all columns."""
    effective_date = pd.to_datetime(row['EFFECTIVE DATE']) if pd.notna(row['EFFECTIVE DATE']) else None
    business_days = get_business_days_to_effective(effective_date) if effective_date else None
    
    urgency_style = ''
    if business_days is not None:
        if business_days <= CONFIG["thresholds"]["urgent_days"]:
            urgency_style = 'background-color: #ffebee; border-left: 3px solid #e74c3c;'
        elif business_days <= CONFIG["thresholds"]["warning_days"]:
            urgency_style = 'background-color: #fff8e1; border-left: 3px solid #f39c12;'
    
    # Format the effective date nicely
    formatted_date = "Not specified"
    business_days_text = ""
    if effective_date is not None:
        formatted_date = effective_date.strftime('%B %d, %Y')
        if business_days is not None:
            if business_days == 0:
                business_days_text = "Today"
            elif business_days == 1:
                business_days_text = "1 trading day"
            else:
                business_days_text = f"{business_days} trading days"
    
    # Get impact color
    impact = row.get('Market Impact', 'UNDEFINED')
    impact_colors = {
        'HIGH': CONFIG["visualization"]["high_impact_color"],
        'MEDIUM': CONFIG["visualization"]["medium_impact_color"],
        'LOW': CONFIG["visualization"]["low_impact_color"],
        'UNDEFINED': CONFIG["visualization"]["undefined_impact_color"]
    }
    impact_color = impact_colors.get(impact, impact_colors['UNDEFINED'])
    impact_text_color = 'white' if impact in ['HIGH', 'MEDIUM'] else 'black'
    
    # Format ticker with link
    ticker_html = format_ticker_with_link(row.get('CURRENT TICKER', ''))
    
    html = [f"""
        <div style="padding: 15px; margin-bottom: 15px; border-radius: 5px; {urgency_style}">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div>
                    <span style="font-size: 1.1em; font-weight: bold;">{ticker_html}</span>
                    <span style="margin-left: 8px; color: #7f8c8d;">{row.get('CURRENT COMPANY NAME', '')}</span>
                </div>
                <div>
                    <span style="background-color: {impact_color}; color: {impact_text_color}; padding: 3px 8px; border-radius: 3px; font-weight: bold;">
                        {impact}
                    </span>
                </div>
            </div>
            
            <div style="display: flex; margin-bottom: 15px; flex-wrap: wrap;">
                <div style="flex: 1; min-width: 250px; margin-right: 15px;">
                    <div style="margin: 5px 0;">
                        <span style="color: #666;">Action Type:</span> <strong>{row.get('ACTION TYPE', 'Unknown')}</strong>
                    </div>
                    <div style="margin: 5px 0;">
                        <span style="color: #666;">Status:</span> <span>{row.get('STATUS', 'Unknown')}</span>
                    </div>
                    <div style="margin: 5px 0;">
                        <span style="color: #666;">Effective Date:</span> {formatted_date}
                        <span style="margin-left: 10px; font-size: 0.9em; color: #666;">
                            ({business_days_text} until effective)
                        </span>
                    </div>
                </div>
            </div>
            
            <div style="display: flex; flex-wrap: wrap; gap: 15px; margin-top: 10px;">
    """]

    # Format comparison fields (like CURRENT IWF → NEW IWF) in a clean layout
    comparison_fields = [
        ('IWF', 'CURRENT IWF', 'NEW IWF'),
        ('AWF', 'CURRENT AWF', 'NEW AWF'),
        ('INDEX SHARES', 'CURRENT INDEX SHARES', 'NEW INDEX SHARES')
    ]
    
    html.append('<div style="display: flex; flex-wrap: wrap; gap: 20px; margin: 10px 0;">')
    
    for field_name, current_field, new_field in comparison_fields:
        if current_field in row and new_field in row and (pd.notna(row[current_field]) or pd.notna(row[new_field])):
            current_val = row[current_field] if pd.notna(row[current_field]) else "N/A"
            new_val = row[new_field] if pd.notna(row[new_field]) else "N/A"
            
            # Format numbers with commas if they're numeric
            if isinstance(current_val, (int, float)):
                current_val = f"{current_val:,}" if field_name != 'IWF' and field_name != 'AWF' else f"{current_val:.2f}"
            if isinstance(new_val, (int, float)):
                new_val = f"{new_val:,}" if field_name != 'IWF' and field_name != 'AWF' else f"{new_val:.2f}"
            
            html.append(f"""
                <div style="background-color: #f8f9fa; padding: 8px 12px; border-radius: 4px; min-width: 200px;">
                    <div style="font-weight: bold; color: #2c3e50; margin-bottom: 5px;">{field_name}</div>
                    <div style="display: flex; align-items: center;">
                        <span style="color: #7f8c8d;">{current_val}</span>
                        <span style="margin: 0 8px; color: #95a5a6;">→</span>
                        <span style="font-weight: bold;">{new_val}</span>
                    </div>
                </div>
            """)
    
    # Add net flow with special formatting
    if 'ABS NET FLOW' in row and pd.notna(row['ABS NET FLOW']):
        formatted_flow = f"{row['ABS NET FLOW']:,.0f}" if isinstance(row['ABS NET FLOW'], (int, float)) else str(row['ABS NET FLOW'])
        html.append(f"""
            <div style="margin: 5px 0; display: flex;">
                <span style="width: 120px; color: #666;">ABS NET FLOW:</span>
                <span style="font-weight: bold;">{formatted_flow}</span>
            </div>
        """)
    
    html.append('</div>')  # Close comparison fields div
    
    # Special handling for Comments field (if present and not empty)
    if 'Comments' in row and pd.notna(row['Comments']) and row['Comments'].strip():
        # Process comments - handle potential long text
        comments = str(row['Comments'])
        
        html.append(f"""
            <div style="margin: 15px 0; padding: 10px; background-color: #e8f4f8; border-radius: 3px; border-left: 3px solid #3498db;">
                <div style="font-weight: bold; color: #2980b9; margin-bottom: 5px;">Comments</div>
                <div style="padding: 8px; background-color: white; border-radius: 3px; white-space: pre-wrap; line-height: 1.5;">
                    {comments}
                </div>
            </div>
        """)
    
    # Add other relevant fields in a flex container
    excluded_cols = ['ACTION TYPE', 'ACTION GROUP', 'STATUS', 'EFFECTIVE DATE', 'CURRENT TICKER', 
                     'CURRENT COMPANY NAME', 'Market Impact', 'Sequence', 'Comments',
                     'CURRENT IWF', 'NEW IWF', 'CURRENT AWF', 'NEW AWF',
                     'CURRENT INDEX SHARES', 'NEW INDEX SHARES', 'ABS NET FLOW']
    
    other_cols = [col for col in row.index if col not in excluded_cols and pd.notna(row[col])]
    
    if other_cols:
        html.append('<div style="display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px;">')
        for col in other_cols:
            value = row[col]
            html.append(f"""
                <div style="background-color: #e9ecef; padding: 3px 8px; border-radius: 3px;">
                    <span style="color: #666;">{col}:</span> {value}
                </div>
            """)
        html.append('</div>')
    
    html.append('</div>')  # Close main row div
    return '\n'.join(html)

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