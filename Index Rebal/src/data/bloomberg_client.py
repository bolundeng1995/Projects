import blpapi
import pandas as pd
from typing import List, Dict, Any, Optional
import datetime
import logging

class BloombergClient:
    """Client for connecting to Bloomberg and retrieving data"""
    
    def __init__(self, host='localhost', port=8194):
        self.host = host
        self.port = port
        self.session = None
        self.logger = logging.getLogger(__name__)
        
    def start_session(self):
        """Start a Bloomberg API session"""
        try:
            # If we already have a session, check if it's working by testing service availability
            if self.session is not None:
                try:
                    # Try to get the service - this will fail if session is not active
                    self.session.getService("//blp/refdata")
                    return True
                except Exception:
                    # If failed, session is likely inactive, recreate it
                    self.logger.info("Existing session is inactive, creating a new one")
                    self.session = None
            
            # Initialize session options
            session_options = blpapi.SessionOptions()
            session_options.setServerHost(self.host)
            session_options.setServerPort(self.port)
            
            # Create a session
            self.session = blpapi.Session(session_options)
            
            # Start the session
            if not self.session.start():
                self.logger.error("Failed to start Bloomberg API session")
                return False
                
            # Open reference data service
            if not self.session.openService("//blp/refdata"):
                self.logger.error("Failed to open reference data service")
                return False
                
            self.logger.info("Bloomberg API session started successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error starting Bloomberg API session: {e}")
            return False
            
    def stop_session(self):
        """Stop the Bloomberg API session"""
        if self.session:
            self.session.stop()
            self.session = None
            self.logger.info("Bloomberg API session stopped")
    
    def get_historical_data(self, securities: List[str], fields: List[str],
                          start_date: str, end_date: str,
                          periodicity: str = "DAILY") -> Dict[str, pd.DataFrame]:
        """
        Get historical data for a list of securities
        
        Args:
            securities: List of Bloomberg security identifiers
            fields: List of fields to retrieve (e.g., PX_LAST, VOLUME)
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD
            periodicity: Data frequency (DAILY, WEEKLY, MONTHLY, etc.)
            
        Returns:
            Dictionary of DataFrames with historical data for each security
        """
        if not self.start_session():
            return {}
            
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for historical data
            request = refdata_service.createRequest("HistoricalDataRequest")
            
            # Set request parameters
            for security in securities:
                request.append("securities", security)
                
            for field in fields:
                request.append("fields", field)
                
            request.set("startDate", start_date.replace("-", ""))
            request.set("endDate", end_date.replace("-", ""))
            request.set("periodicitySelection", periodicity)
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            result = {}
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("HistoricalDataResponse"):
                        security = msg.getElement("securityData").getElementAsString("security")
                        field_data = msg.getElement("securityData").getElement("fieldData")
                        
                        data_list = []
                        for i in range(field_data.numValues()):
                            field_values = field_data.getValue(i)
                            data_point = {"date": field_values.getElementAsDatetime("date")}
                            
                            for field in fields:
                                if field_values.hasElement(field):
                                    data_point[field] = field_values.getElementAsFloat(field)
                                    
                            data_list.append(data_point)
                            
                        result[security] = pd.DataFrame(data_list).set_index("date")
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return result
            
        except Exception as e:
            self.logger.error(f"Error retrieving historical data: {e}")
            return {}
    
    def get_index_members(self, index_ticker: str) -> List[str]:
        """
        Get current members of an index
        
        Args:
            index_ticker: Bloomberg index identifier (e.g., "SPX Index")
            
        Returns:
            List of member tickers
        """
        if not self.start_session():
            return []
            
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for index members
            request = refdata_service.createRequest("ReferenceDataRequest")
            request.append("securities", index_ticker)
            request.append("fields", "INDX_MEMBERS")
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            members = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        security_data = msg.getElement("securityData").getValue(0)
                        
                        if security_data.hasElement("fieldData"):
                            field_data = security_data.getElement("fieldData")
                            
                            if field_data.hasElement("INDX_MEMBERS"):
                                member_data = field_data.getElement("INDX_MEMBERS")
                                
                                for i in range(member_data.numValues()):
                                    member = member_data.getValue(i)
                                    members.append(member.getElementAsString("Member Ticker and Exchange Code"))
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return members
            
        except Exception as e:
            self.logger.error(f"Error retrieving index members: {e}")
            return []
            
    def get_index_member_weights(self, index_ticker: str) -> pd.DataFrame:
        """
        Get current constituents and their weights for an index
        
        Args:
            index_ticker: Bloomberg index ticker (e.g., "SPX Index")
            
        Returns:
            DataFrame containing member tickers, names, and weights
        """
        self.logger.info(f"Getting member weights for {index_ticker}")
        
        try:
            # Initialize a session if needed
            if not self.start_session():
                return pd.DataFrame()
            
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request
            request = refdata_service.createRequest("ReferenceDataRequest")
            
            # Set the security
            request.append("securities", index_ticker)
            
            # Set the fields for index members and weights
            request.append("fields", "INDX_MWEIGHT")
            request.append("fields", "INDX_MEMBERS")
            
            # Send request
            self.session.sendRequest(request)
            
            # Process response
            members = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        security_data = msg.getElement("securityData").getValue(0)
                        
                        if security_data.hasElement("fieldData"):
                            field_data = security_data.getElement("fieldData")
                            
                            # Process member weights
                            if field_data.hasElement("INDX_MWEIGHT"):
                                member_weights = field_data.getElement("INDX_MWEIGHT")
                                
                                for i in range(member_weights.numValues()):
                                    member_data = member_weights.getValue(i)
                                    
                                    # Get member ticker and weight
                                    member = {}
                                    
                                    if member_data.hasElement("Member Ticker and Exchange Code"):
                                        member["member_ticker"] = member_data.getElementAsString("Member Ticker and Exchange Code")
                                    else:
                                        continue  # Skip if no ticker                      
                                        
                                    if member_data.hasElement("Percentage Weight"):
                                        member["weight"] = member_data.getElementAsFloat("Percentage Weight")
                                    else:
                                        member["weight"] = 0.0
                                        
                                    members.append(member)
                            
                            # If INDX_MWEIGHT not available, try INDX_MEMBERS
                            elif field_data.hasElement("INDX_MEMBERS"):
                                member_list = field_data.getElement("INDX_MEMBERS")
                                
                                for i in range(member_list.numValues()):
                                    member_ticker = member_list.getValue(i)
                                    members.append({
                                        "member_ticker": member_ticker,
                                        "member_name": "",  # We don't have the name in this case
                                        "weight": 0.0       # We don't have weights in this case
                                    })
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            df = pd.DataFrame(members)
            
            # If we have a dataframe with weights but no names, try to get the names
            if not df.empty and "member_ticker" in df.columns and df["member_name"].isna().all():
                # Get security info for all members
                member_tickers = [f"{ticker} Equity" for ticker in df["member_ticker"].unique()]
                security_info = self.get_security_info(member_tickers, ["SECURITY_NAME"])
                
                # Create ticker to name mapping
                name_map = {}
                for _, row in security_info.iterrows():
                    ticker = row["ticker"].replace(" Equity", "")
                    name_map[ticker] = row.get("SECURITY_NAME", "")
                    
                # Update member names
                df["member_name"] = df["member_ticker"].map(name_map)
                
            # Sort by weight descending
            if not df.empty and "weight" in df.columns:
                df = df.sort_values("weight", ascending=False)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting member weights for {index_ticker}: {e}")
            return pd.DataFrame()
    
    def get_security_info(self, securities: List[str], fields: List[str]) -> pd.DataFrame:
        """
        Get reference data for a list of securities
        
        Args:
            securities: List of Bloomberg security identifiers
            fields: List of fields to retrieve (e.g., SECURITY_NAME, GICS_SECTOR_NAME)
            
        Returns:
            DataFrame with security reference data
        """
        if not self.start_session():
            return pd.DataFrame()
            
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for reference data
            request = refdata_service.createRequest("ReferenceDataRequest")
            
            for security in securities:
                request.append("securities", security)
                
            for field in fields:
                request.append("fields", field)
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            security_info = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        security_data_array = msg.getElement("securityData")
                        
                        for i in range(security_data_array.numValues()):
                            security_data = security_data_array.getValue(i)
                            ticker = security_data.getElementAsString("security")
                            
                            if security_data.hasElement("fieldData"):
                                field_data = security_data.getElement("fieldData")
                                
                                security_record = {"ticker": ticker}
                                
                                for field in fields:
                                    if field_data.hasElement(field):
                                        try:
                                            value = field_data.getElementAsString(field)
                                        except:
                                            try:
                                                value = field_data.getElementAsFloat(field)
                                            except:
                                                value = None
                                                
                                        security_record[field] = value
                                        
                                security_info.append(security_record)
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return pd.DataFrame(security_info)
            
        except Exception as e:
            self.logger.error(f"Error retrieving security info: {e}")
            return pd.DataFrame()

    def get_index_changes(self, index_ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Calculate index constituent changes by comparing members and weights between two dates
        
        Args:
            index_ticker: Bloomberg index ticker (e.g., "SPX Index")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing index changes (additions, deletions, weight changes)
        """
        self.logger.info(f"Calculating index changes for {index_ticker} from {start_date} to {end_date}")
        
        try:
            import xbbg.blp as blp
            from datetime import datetime
            
            # Import our mappings
            from data.config.bloomberg_mappings import INDEX_CONSTITUENT_FIELDS
            
            # Convert dates to Bloomberg format (YYYYMMDD)
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            bloomberg_start_date = start_dt.strftime("%Y%m%d")
            bloomberg_end_date = end_dt.strftime("%Y%m%d")
            
            # Get index member weights at start date
            start_result = blp.bds(
                tickers=index_ticker,
                flds="INDX_MWEIGHT", 
                END_DATE_OVERRIDE=bloomberg_start_date
            )
            
            # Get index member weights at end date
            end_result = blp.bds(
                tickers=index_ticker,
                flds="INDX_MWEIGHT", 
                END_DATE_OVERRIDE=bloomberg_end_date
            )
            
            # If no data returned for either date, return empty DataFrame
            if (start_result is None or start_result.empty) and (end_result is None or end_result.empty):
                self.logger.warning(f"No index data found for {index_ticker} on the specified dates")
                return pd.DataFrame()
            
            # Process start date members with standardized column names
            start_members = {}
            if start_result is not None and not start_result.empty:
                # Reset index and standardize column names
                start_df = start_result.reset_index().rename(columns=INDEX_CONSTITUENT_FIELDS)
                
                # Extract ticker and weight info
                for _, row in start_df.iterrows():
                    ticker = row.get('ticker', None)
                    if ticker:
                        start_members[ticker] = {
                            'weight': float(row.get('weight', 0.0)) if pd.notna(row.get('weight', 0.0)) else 0.0,
                            'name': row.get('name', '')
                        }
                    
            # Process end date members with standardized column names
            end_members = {}
            if end_result is not None and not end_result.empty:
                # Reset index and standardize column names
                end_df = end_result.reset_index().rename(columns=INDEX_CONSTITUENT_FIELDS)
                
                # Extract ticker and weight info
                for _, row in end_df.iterrows():
                    ticker = row.get('ticker', None)
                    if ticker:
                        end_members[ticker] = {
                            'weight': float(row.get('weight', 0.0)) if pd.notna(row.get('weight', 0.0)) else 0.0,
                            'name': row.get('name', '')
                        }
            
            # Calculate changes
            changes = []
            
            # Find additions, deletions, and weight changes
            for ticker in end_members:
                if ticker not in start_members:
                    # Addition
                    changes.append({
                        'effective_date': end_date,
                        'announcement_date': None,
                        'ticker': ticker,
                        'bloomberg_ticker': f"{ticker} Equity",
                        'change_type': 'ADD',
                        'old_weight': 0.0,
                        'new_weight': end_members[ticker]['weight'],
                        'reason': 'Index addition'
                    })
                else:
                    # Weight change
                    old_weight = start_members[ticker]['weight']
                    new_weight = end_members[ticker]['weight']
                    if abs(new_weight - old_weight) > 0.001:
                        changes.append({
                            'effective_date': end_date,
                            'announcement_date': None,
                            'ticker': ticker,
                            'bloomberg_ticker': f"{ticker} Equity",
                            'change_type': 'WEIGHT_CHG',
                            'old_weight': old_weight,
                            'new_weight': new_weight,
                            'reason': 'Weight change'
                        })
            
            # Find deletions
            for ticker in start_members:
                if ticker not in end_members:
                    changes.append({
                        'effective_date': end_date,
                        'announcement_date': None,
                        'ticker': ticker,
                        'bloomberg_ticker': f"{ticker} Equity",
                        'change_type': 'DELETE',
                        'old_weight': start_members[ticker]['weight'],
                        'new_weight': 0.0,
                        'reason': 'Index deletion'
                    })
            
            # Create DataFrame and sort by change_type
            df = pd.DataFrame(changes)
            if not df.empty:
                # Define custom sort order for change_type
                change_type_order = {'ADD': 0, 'DELETE': 1, 'WEIGHT_CHG': 2}
                df['change_type_order'] = df['change_type'].map(change_type_order)
                df = df.sort_values(['change_type_order', 'ticker']).drop('change_type_order', axis=1)
            
            return df
            
        except ImportError:
            self.logger.error("xbbg package not installed. Install with 'pip install xbbg'")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error calculating index changes for {index_ticker}: {e}")
            return pd.DataFrame()

    def _standardize_date(self, date_str: str) -> str:
        """Convert a date string to YYYY-MM-DD format"""
        from datetime import datetime
        
        try:
            # Try parsing as MM/DD/YYYY first (Bloomberg format)
            date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            try:
                # Try parsing as YYYY-MM-DD
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                # Return original if parsing fails
                return date_str

    def get_current_data(self, securities: List[str], fields: List[str]) -> pd.DataFrame:
        """
        Get current market data for a list of securities
        
        Args:
            securities: List of Bloomberg security identifiers
            fields: List of Bloomberg field identifiers
            
        Returns:
            DataFrame containing current market data
        """
        self.logger.info(f"Getting current market data for {len(securities)} securities")
        
        try:
            # Initialize a session if needed
            if not self.start_session():
                return pd.DataFrame()
            
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request
            request = refdata_service.createRequest("ReferenceDataRequest")
            
            # Add securities to request
            for security in securities:
                request.append("securities", security)
            
            # Add fields to request
            for field in fields:
                request.append("fields", field)
            
            # Send request
            self.session.sendRequest(request)
            
            # Process response
            data = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        security_data_array = msg.getElement("securityData")
                        
                        for i in range(security_data_array.numValues()):
                            security_data = security_data_array.getValue(i)
                            ticker = security_data.getElementAsString("security")
                            
                            if security_data.hasElement("fieldData"):
                                field_data = security_data.getElement("fieldData")
                                
                                row = {"ticker": ticker}
                                
                                for field in fields:
                                    if field_data.hasElement(field):
                                        try:
                                            value = field_data.getElementAsFloat(field)
                                        except:
                                            try:
                                                value = field_data.getElementAsString(field)
                                            except:
                                                value = None
                                                
                                        row[field] = value
                                        
                                data.append(row)
                            
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return pd.DataFrame(data)
            
        except Exception as e:
            self.logger.error(f"Error retrieving current market data: {e}")
            return pd.DataFrame() 