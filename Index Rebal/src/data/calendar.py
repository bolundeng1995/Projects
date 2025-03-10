import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import re
from src.data.bloomberg_client import BloombergClient

class RebalanceCalendar:
    """
    Manages and tracks upcoming index rebalance events.
    Provides methods to query, add, and update rebalance dates.
    """
    
    def __init__(self, database, bloomberg_client: BloombergClient):
        self.db = database
        self.bloomberg = bloomberg_client
        self.logger = logging.getLogger(__name__)
        self._initialize_calendar()
    
    def _initialize_calendar(self):
        """Create or load the rebalance event calendar"""
        try:
            # Ensure the rebalance_events table exists (handled by database class)
            self.logger.info("Rebalance calendar initialized")
        except Exception as e:
            self.logger.error(f"Error initializing rebalance calendar: {e}")
    
    def get_upcoming_events(self, days_ahead: int = 30) -> pd.DataFrame:
        """
        Get all rebalance events in the next N days
        
        Args:
            days_ahead: Number of days to look ahead
            
        Returns:
            DataFrame with upcoming events
        """
        try:
            # Get events from database
            events = self.db.get_upcoming_rebalance_events(days_ahead)
            
            if events.empty:
                return pd.DataFrame()
            
            # Convert dates to datetime
            events['announcement_date'] = pd.to_datetime(events['announcement_date'])
            events['implementation_date'] = pd.to_datetime(events['implementation_date'])
            
            # Calculate days until events
            today = pd.Timestamp.now().normalize()
            events['days_to_announcement'] = (events['announcement_date'] - today).dt.days
            events['days_to_implementation'] = (events['implementation_date'] - today).dt.days
            
            # Filter for future events
            future_events = events[(events['days_to_announcement'] >= 0) | 
                                   (events['days_to_implementation'] >= 0)]
                                   
            return future_events.sort_values(by=[
                'days_to_announcement', 
                'days_to_implementation'
            ]).reset_index(drop=True)
            
        except Exception as e:
            self.logger.error(f"Error getting upcoming events: {e}")
            return pd.DataFrame()
    
    def add_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Add a new rebalance event to the calendar
        
        Args:
            event_data: Dictionary with event details
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate required fields
            required_fields = ['index_id', 'event_type', 'announcement_date', 'implementation_date']
            for field in required_fields:
                if field not in event_data:
                    self.logger.error(f"Missing required field: {field}")
                    return False
            
            # Format dates if they're not already strings
            if not isinstance(event_data['announcement_date'], str):
                event_data['announcement_date'] = event_data['announcement_date'].strftime('%Y-%m-%d')
                
            if not isinstance(event_data['implementation_date'], str):
                event_data['implementation_date'] = event_data['implementation_date'].strftime('%Y-%m-%d')
            
            # Add to database
            result = self.db.add_rebalance_event(
                index_id=event_data['index_id'],
                event_type=event_data['event_type'],
                announcement_date=event_data['announcement_date'],
                implementation_date=event_data['implementation_date'],
                description=event_data.get('description', ''),
                status=event_data.get('status', 'SCHEDULED'),
                notes=event_data.get('notes', '')
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error adding event: {e}")
            return False
    
    def update_sp_calendar(self) -> int:
        """
        Update the calendar with S&P rebalance dates
        
        Returns:
            Number of events added
        """
        try:
            # Get all S&P indices
            query = "SELECT index_id, bloomberg_ticker FROM index_metadata WHERE index_id LIKE 'SP%'"
            sp_indices = pd.read_sql_query(query, self.db.conn)
            
            if sp_indices.empty:
                self.logger.info("No S&P indices found in database")
                return 0
            
            # S&P rebalance fields 
            fields = [
                "INDX_REBALANCE_ANNOUNCE_DT",
                "INDX_MAINT_NEXT_EFFECTIVE_DT",
                "INDX_REBAL_PERIOD",
                "INDX_MAINT_NEXT_DATE"
            ]
            
            event_count = 0
            
            for _, row in sp_indices.iterrows():
                index_id = row['index_id']
                ticker = row['bloomberg_ticker']
                
                # Get rebalance dates
                results = self.bloomberg.get_security_info([ticker], fields)
                
                if results.empty:
                    self.logger.info(f"No rebalance dates found for {index_id}")
                    continue
                
                # Extract dates from results
                dates_row = results.iloc[0]
                
                announce_date = dates_row.get('INDX_REBALANCE_ANNOUNCE_DT')
                impl_date = dates_row.get('INDX_MAINT_NEXT_EFFECTIVE_DT')
                
                if not announce_date or not impl_date:
                    self.logger.info(f"Missing dates for {index_id}")
                    continue
                
                # Add to calendar
                event_data = {
                    'index_id': index_id,
                    'event_type': 'REBALANCE',
                    'announcement_date': announce_date,
                    'implementation_date': impl_date,
                    'description': f"{index_id} Quarterly Rebalance",
                    'status': 'SCHEDULED'
                }
                
                if self.add_event(event_data):
                    event_count += 1
                    self.logger.info(f"Added rebalance event for {index_id}")
            
            return event_count
            
        except Exception as e:
            self.logger.error(f"Error updating S&P calendar: {e}")
            return 0
    
    def update_russell_calendar(self) -> int:
        """
        Update the calendar with Russell reconstitution dates
        
        Returns:
            Number of events added
        """
        try:
            # Get all Russell indices
            query = "SELECT index_id, bloomberg_ticker FROM index_metadata WHERE index_id LIKE 'RUSSELL%'"
            russell_indices = pd.read_sql_query(query, self.db.conn)
            
            if russell_indices.empty:
                self.logger.info("No Russell indices found in database")
                return 0
            
            # For Russell, we need to look up the annual reconstitution schedule
            # This is typically in June with published dates
            
            # Get dates from Bloomberg
            event_count = 0
            current_year = datetime.now().year
            
            # Russell rebalance fields
            fields = [
                "INDX_REBALANCE_ANNOUNCE_DT",
                "INDX_MAINT_NEXT_EFFECTIVE_DT"
            ]
            
            for _, row in russell_indices.iterrows():
                index_id = row['index_id']
                ticker = row['bloomberg_ticker']
                
                # Get reconstitution dates  
                results = self.bloomberg.get_security_info([ticker], fields)
                
                if results.empty:
                    # Russell typically publishes dates on their website
                    # Fallback to approximate dates for Russell
                    # This is a crude estimate - real implementation would use actual dates
                    june_last_friday = self._get_last_friday_of_month(current_year, 6)
                    announce_date = self._get_friday_before(june_last_friday, 2)  # 2 weeks before
                    
                    event_data = {
                        'index_id': index_id,
                        'event_type': 'RECONSTITUTION',
                        'announcement_date': announce_date.strftime('%Y-%m-%d'),
                        'implementation_date': june_last_friday.strftime('%Y-%m-%d'),
                        'description': f"{index_id} Annual Reconstitution (estimated)",
                        'status': 'ESTIMATED'
                    }
                else:
                    # Extract dates from results
                    dates_row = results.iloc[0]
                    
                    announce_date = dates_row.get('INDX_REBALANCE_ANNOUNCE_DT')
                    impl_date = dates_row.get('INDX_MAINT_NEXT_EFFECTIVE_DT')
                    
                    if not announce_date or not impl_date:
                        self.logger.info(f"Missing dates for {index_id}")
                        continue
                    
                    event_data = {
                        'index_id': index_id,
                        'event_type': 'RECONSTITUTION',
                        'announcement_date': announce_date,
                        'implementation_date': impl_date,
                        'description': f"{index_id} Annual Reconstitution",
                        'status': 'SCHEDULED'
                    }
                
                if self.add_event(event_data):
                    event_count += 1
                    self.logger.info(f"Added reconstitution event for {index_id}")
            
            return event_count
            
        except Exception as e:
            self.logger.error(f"Error updating Russell calendar: {e}")
            return 0
    
    def update_nasdaq_calendar(self) -> int:
        """
        Update the calendar with Nasdaq rebalance dates
        
        Returns:
            Number of events added
        """
        try:
            # Get all Nasdaq indices
            query = "SELECT index_id, bloomberg_ticker FROM index_metadata WHERE index_id LIKE 'NASDAQ%'"
            nasdaq_indices = pd.read_sql_query(query, self.db.conn)
            
            if nasdaq_indices.empty:
                self.logger.info("No Nasdaq indices found in database")
                return 0
            
            # Nasdaq rebalance fields
            fields = [
                "INDX_REBALANCE_ANNOUNCE_DT",
                "INDX_MAINT_NEXT_EFFECTIVE_DT"
            ]
            
            event_count = 0
            
            for _, row in nasdaq_indices.iterrows():
                index_id = row['index_id']
                ticker = row['bloomberg_ticker']
                
                # Get rebalance dates
                results = self.bloomberg.get_security_info([ticker], fields)
                
                if results.empty:
                    self.logger.info(f"No rebalance dates found for {index_id}")
                    continue
                
                # Extract dates from results
                dates_row = results.iloc[0]
                
                announce_date = dates_row.get('INDX_REBALANCE_ANNOUNCE_DT')
                impl_date = dates_row.get('INDX_MAINT_NEXT_EFFECTIVE_DT')
                
                if not announce_date or not impl_date:
                    self.logger.info(f"Missing dates for {index_id}")
                    continue
                
                # Add to calendar
                event_data = {
                    'index_id': index_id,
                    'event_type': 'REBALANCE',
                    'announcement_date': announce_date,
                    'implementation_date': impl_date,
                    'description': f"{index_id} Quarterly Rebalance",
                    'status': 'SCHEDULED'
                }
                
                if self.add_event(event_data):
                    event_count += 1
                    self.logger.info(f"Added rebalance event for {index_id}")
            
            return event_count
            
        except Exception as e:
            self.logger.error(f"Error updating Nasdaq calendar: {e}")
            return 0
    
    def update_all_calendars(self) -> int:
        """
        Update all rebalance calendars
        
        Returns:
            Total number of events added
        """
        total_events = 0
        
        try:
            # Update each calendar type
            sp_events = self.update_sp_calendar()
            russell_events = self.update_russell_calendar()
            nasdaq_events = self.update_nasdaq_calendar()
            
            total_events = sp_events + russell_events + nasdaq_events
            
            self.logger.info(f"Added {total_events} calendar events")
            return total_events
            
        except Exception as e:
            self.logger.error(f"Error updating all calendars: {e}")
            return total_events
    
    def _get_last_friday_of_month(self, year: int, month: int) -> datetime:
        """Helper to get last Friday of a month"""
        # Get last day of month
        if month == 12:
            last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Find last Friday
        days_to_subtract = (last_day.weekday() - 4) % 7
        last_friday = last_day - timedelta(days=days_to_subtract)
        
        return last_friday
    
    def _get_friday_before(self, date: datetime, weeks_before: int) -> datetime:
        """Get Friday that is n weeks before given date"""
        return date - timedelta(days=(7 * weeks_before)) 