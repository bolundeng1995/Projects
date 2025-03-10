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
                        security_data = msg.getElement("securityData")
                        
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

    def get_corporate_actions(self, securities: List[str], 
                             start_date: str, 
                             end_date: str) -> pd.DataFrame:
        """
        Get corporate actions for a list of securities
        
        Args:
            securities: List of Bloomberg security identifiers
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD
            
        Returns:
            DataFrame with corporate action data
        """
        if not self.start_session():
            return pd.DataFrame()
        
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for corporate actions
            request = refdata_service.createRequest("ReferenceDataRequest")
            
            for security in securities:
                request.append("securities", security)
                
            # Add fields for different corporate actions
            request.append("fields", "CORP_ACTION_START_DT")
            request.append("fields", "CORP_ACTION_TYPE")
            request.append("fields", "CORP_ACTION_STATUS")
            request.append("fields", "CORP_ACTION_DESC")
            
            # Set date range for corporate actions
            overrides = request.getElement("overrides")
            start_override = overrides.appendElement()
            start_override.setElement("fieldId", "START_DT")
            start_override.setElement("value", start_date)
            
            end_override = overrides.appendElement()
            end_override.setElement("fieldId", "END_DT")
            end_override.setElement("value", end_date)
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            corporate_actions = []
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
                                
                                if field_data.hasElement("CORP_ACTION_START_DT") and field_data.hasElement("CORP_ACTION_TYPE"):
                                    action_date = field_data.getElementAsString("CORP_ACTION_START_DT")
                                    action_type = field_data.getElementAsString("CORP_ACTION_TYPE")
                                    action_status = field_data.getElementAsString("CORP_ACTION_STATUS") if field_data.hasElement("CORP_ACTION_STATUS") else ""
                                    action_desc = field_data.getElementAsString("CORP_ACTION_DESC") if field_data.hasElement("CORP_ACTION_DESC") else ""
                                    
                                    corporate_actions.append({
                                        "ticker": ticker,
                                        "action_date": action_date,
                                        "action_type": action_type,
                                        "action_status": action_status,
                                        "action_description": action_desc
                                    })
                    
            if event.eventType() == blpapi.Event.RESPONSE:
                end_reached = True
                
            return pd.DataFrame(corporate_actions)
            
        except Exception as e:
            self.logger.error(f"Error retrieving corporate actions: {e}")
            return pd.DataFrame()
        
    def get_mergers_acquisitions(self, start_date: str, end_date: str, 
                               status: str = "ANNOUNCED") -> pd.DataFrame:
        """
        Get merger and acquisition data
        
        Args:
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD
            status: Status of M&A deals to retrieve (ANNOUNCED, COMPLETED, PENDING, etc.)
            
        Returns:
            DataFrame with M&A data
        """
        if not self.start_session():
            return pd.DataFrame()
        
        try:
            # For M&A data, we use the EQS (Equity Screening) service
            if not self.session.openService("//blp/exrsvc"):
                self.logger.error("Failed to open EQS service")
                return pd.DataFrame()
            
            exrsvc = self.session.getService("//blp/exrsvc")
            
            # Create request for EQS
            request = exrsvc.createRequest("BeqsRequest")
            
            # Set universe parameters (Public companies in given markets)
            universe = request.getElement("universe")
            universe.setElement("stringValue", "PRIVATE_ASSETS_GLOBAL")
            
            # Set screening criteria for M&A deals
            screen = request.getElement("screeningCriteria")
            
            # Add filter for deal type
            filter1 = screen.appendElement("filter")
            filter1.setElement("name", "Deal Type")
            filter1.setElement("value", "M&A")
            
            # Add filter for date range
            filter2 = screen.appendElement("filter")
            filter2.setElement("name", "Announcement Date")
            filter2.setElement("greaterThanEq", start_date)
            filter2.setElement("lessThanEq", end_date)
            
            # Add filter for status
            filter3 = screen.appendElement("filter")
            filter3.setElement("name", "Deal Status")
            filter3.setElement("value", status)
            
            # Select fields to retrieve
            fields = request.getElement("fields")
            
            # Deal details
            fields.appendValue("DealStatus")
            fields.appendValue("AnnouncementDate")
            fields.appendValue("ExpectedCompletionDate")
            fields.appendValue("DealValue")
            fields.appendValue("DealValueCurrency")
            
            # Target info
            fields.appendValue("TargetName")
            fields.appendValue("TargetTicker")
            fields.appendValue("TargetIndustry")
            fields.appendValue("TargetCountry")
            
            # Acquirer info
            fields.appendValue("AcquirerName")
            fields.appendValue("AcquirerTicker")
            fields.appendValue("AcquirerIndustry")
            fields.appendValue("AcquirerCountry")
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            ma_deals = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("BeqsResponse"):
                        if msg.hasElement("data"):
                            data = msg.getElement("data")
                            for i in range(data.numValues()):
                                field_data = data.getValue(i)
                                
                                deal = {}
                                for j in range(field_data.numElements()):
                                    element = field_data.getElement(j)
                                    name = element.name()
                                    value = element.getValueAsString()
                                    deal[name] = value
                                    
                                ma_deals.append(deal)
                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return pd.DataFrame(ma_deals)
            
        except Exception as e:
            self.logger.error(f"Error retrieving M&A data: {e}")
            return pd.DataFrame()

    def get_ticker_changes(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get ticker changes data
        
        Args:
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD
            
        Returns:
            DataFrame with ticker change data
        """
        if not self.start_session():
            return pd.DataFrame()
        
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for CHNG_TICKER_EXCH query
            request = refdata_service.createRequest("BeqsRequest")
            
            # Set universe parameters
            universe = request.getElement("universe")
            universe.setElement("stringValue", "EQY_TICKER_CHANGES")
            
            # Set screening criteria for ticker changes
            screen = request.getElement("screeningCriteria")
            
            # Add filter for date range
            filter1 = screen.appendElement("filter")
            filter1.setElement("name", "Change Date")
            filter1.setElement("greaterThanEq", start_date)
            filter1.setElement("lessThanEq", end_date)
            
            # Select fields to retrieve
            fields = request.getElement("fields")
            fields.appendValue("Old Ticker")
            fields.appendValue("New Ticker")
            fields.appendValue("Old Name")
            fields.appendValue("New Name")
            fields.appendValue("Change Date")
            fields.appendValue("Change Reason")
            fields.appendValue("Security Type")
            fields.appendValue("Country")
            fields.appendValue("Exchange")
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            ticker_changes = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("BeqsResponse"):
                        if msg.hasElement("data"):
                            data = msg.getElement("data")
                            for i in range(data.numValues()):
                                field_data = data.getValue(i)
                                
                                change = {}
                                for j in range(field_data.numElements()):
                                    element = field_data.getElement(j)
                                    name = element.name()
                                    value = element.getValueAsString()
                                    change[name] = value
                                    
                                ticker_changes.append(change)
                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return pd.DataFrame(ticker_changes)
            
        except Exception as e:
            self.logger.error(f"Error retrieving ticker changes: {e}")
            return pd.DataFrame()

    def get_index_changes(self, index_ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get historical index constituent changes using xbbg
        
        Args:
            index_ticker: Bloomberg index ticker (e.g., "SPX Index")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing index changes
        """
        self.logger.info(f"Getting index changes for {index_ticker} from {start_date} to {end_date}")
        
        try:
            import xbbg.blp as blp
            
            # Use xbbg to get index weight history
            result = blp.bds(
                tickers=index_ticker,
                flds='INDX_MWEIGHT_HIST',
                overrides={
                    'START_DT': start_date,
                    'END_DT': end_date
                }
            )
            
            # If no data returned, return empty DataFrame
            if result is None or result.empty:
                self.logger.warning(f"No index changes found for {index_ticker} in date range")
                return pd.DataFrame()
            
            # Reset index to make it easier to iterate through rows
            df = result.reset_index()
            
            # Extract the relevant columns and rename them to our expected format
            changes = []
            for _, row in df.iterrows():
                change = {
                    'effective_date': row.get('Effective Date', None),
                    'announcement_date': row.get('Announcement Date', None),
                    'ticker': row.get('Ticker', None),
                    'bloomberg_ticker': row.get('ID_BB_SEC', None),
                    'change_type': row.get('Type', None),
                    'old_weight': float(row.get('Old Weight', 0.0)) if pd.notna(row.get('Old Weight', 0.0)) else 0.0,
                    'new_weight': float(row.get('New Weight', 0.0)) if pd.notna(row.get('New Weight', 0.0)) else 0.0,
                    'reason': row.get('Reason', None)
                }
                changes.append(change)
            
            return pd.DataFrame(changes)
        
        except ImportError:
            self.logger.error("xbbg package not installed. Install with 'pip install xbbg'")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error getting index changes for {index_ticker}: {e}")
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