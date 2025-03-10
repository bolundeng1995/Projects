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
        Get historical index constituent changes
        
        Args:
            index_ticker: Bloomberg index ticker (e.g., "SPX Index")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing index changes
        """
        self.logger.info(f"Getting index changes for {index_ticker} from {start_date} to {end_date}")
        
        try:
            # Initialize a session if needed
            if not self.start_session():
                return pd.DataFrame()
            
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # First, check if the ticker is valid
            validate_request = refdata_service.createRequest("ReferenceDataRequest")
            validate_request.append("securities", index_ticker)
            validate_request.append("fields", "ID_BB_COMPANY")
            
            self.session.sendRequest(validate_request)
            valid_ticker = False
            
            # Process validation response
            event = self.session.nextEvent(500)
            for msg in event:
                if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                    if not msg.hasElement("responseError"):
                        valid_ticker = True
                    else:
                        error_msg = msg.getElement("responseError").getElementAsString("message")
                        self.logger.error(f"Bloomberg API error: {error_msg}")
            
            if not valid_ticker:
                self.logger.error(f"Invalid index ticker: {index_ticker}")
                return pd.DataFrame()
            
            # Create request for index changes
            request = refdata_service.createRequest("ReferenceDataRequest")
            request.append("securities", index_ticker)
            
            # Try the INDX_MWEIGHT_HIST field
            request.append("fields", "INDX_MWEIGHT_HIST")
            
            # Set date range as override
            overrides = request.getElement("overrides")
            
            start_date_override = overrides.appendElement()
            start_date_override.setElement("fieldId", "START_DT")
            start_date_override.setElement("value", start_date)
            
            end_date_override = overrides.appendElement()
            end_date_override.setElement("fieldId", "END_DT")
            end_date_override.setElement("value", end_date)
            
            # Send request and check for bad request first
            self.session.sendRequest(request)
            event = self.session.nextEvent(500)
            has_error = False
            
            for msg in event:
                if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                    if msg.hasElement("responseError"):
                        error_element = msg.getElement("responseError")
                        error_msg = error_element.getElementAsString("message")
                        self.logger.error(f"Bloomberg API error: {error_msg}")
                        has_error = True
                    
                    if msg.toString().find("bad request") >= 0:
                        self.logger.error(f"Bad request detected: {msg.toString()}")
                        has_error = True
            
            if has_error:
                # Try an alternative approach for index changes
                self.logger.info("Using alternative approach for index changes")
                return self._get_index_changes_alternative(index_ticker, start_date, end_date)
            
            # Process remaining responses for the normal approach
            # Process response
            changes = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        # Check for response errors
                        if msg.hasElement("responseError"):
                            error_element = msg.getElement("responseError")
                            error_msg = error_element.getElementAsString("message")
                            self.logger.error(f"Bloomberg API error: {error_msg}")
                            continue
                        
                        # Get the security data array
                        security_data_array = msg.getElement("securityData")
                        
                        # Iterate through all securities in the response
                        for i in range(security_data_array.numValues()):
                            security_data = security_data_array.getValue(i)
                            
                            # Check for security-level errors
                            if security_data.hasElement("securityError"):
                                error = security_data.getElement("securityError")
                                self.logger.error(f"Security error for {index_ticker}: {error.getElementAsString('message')}")
                                continue
                            
                            # Process field data
                            if security_data.hasElement("fieldData"):
                                field_data = security_data.getElement("fieldData")
                                
                                if field_data.hasElement("INDX_MWEIGHT_HIST"):
                                    weight_hist = field_data.getElement("INDX_MWEIGHT_HIST")
                                    
                                    # Process historical weights
                                    for j in range(weight_hist.numValues()):
                                        weight_data = weight_hist.getValue(j)
                                        
                                        # Extract change data
                                        change = {
                                            'effective_date': weight_data.getElementAsString("Effective Date") if weight_data.hasElement("Effective Date") else None,
                                            'announcement_date': weight_data.getElementAsString("Announcement Date") if weight_data.hasElement("Announcement Date") else None,
                                            'ticker': weight_data.getElementAsString("Ticker") if weight_data.hasElement("Ticker") else None,
                                            'bloomberg_ticker': weight_data.getElementAsString("ID_BB_SEC") if weight_data.hasElement("ID_BB_SEC") else None,
                                            'change_type': weight_data.getElementAsString("Type") if weight_data.hasElement("Type") else None,
                                            'old_weight': weight_data.getElementAsFloat("Old Weight") if weight_data.hasElement("Old Weight") else 0.0,
                                            'new_weight': weight_data.getElementAsFloat("New Weight") if weight_data.hasElement("New Weight") else 0.0,
                                            'reason': weight_data.getElementAsString("Reason") if weight_data.hasElement("Reason") else None
                                        }
                                        
                                        changes.append(change)
                                else:
                                    # Check for field-level errors
                                    if security_data.hasElement("fieldExceptions"):
                                        field_exceptions = security_data.getElement("fieldExceptions")
                                        for j in range(field_exceptions.numValues()):
                                            field_exception = field_exceptions.getValue(j)
                                            field_id = field_exception.getElementAsString("fieldId")
                                            error_info = field_exception.getElement("errorInfo")
                                            error_message = error_info.getElementAsString("message")
                                            self.logger.error(f"Field error for {index_ticker} field {field_id}: {error_message}")
            
            if event.eventType() == blpapi.Event.RESPONSE:
                end_reached = True
            
            return pd.DataFrame(changes)
            
        except Exception as e:
            self.logger.error(f"Error getting index changes for {index_ticker}: {e}")
            return pd.DataFrame()

    def _get_index_changes_alternative(self, index_ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Alternative method to get index changes when INDX_MWEIGHT_HIST is not available"""
        self.logger.info(f"Using alternative method to get changes for {index_ticker}")
        
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request
            request = refdata_service.createRequest("HistoricalDataRequest")
            
            # Set the security
            request.append("securities", index_ticker)
            
            # Set fields for constituents at different points in time
            request.append("fields", "INDX_MEMBERS")
            
            # Set date range
            request.set("startDate", start_date)
            request.set("endDate", end_date)
            request.set("periodicitySelection", "MONTHLY")  # Get monthly snapshots
            
            # Send request
            self.session.sendRequest(request)
            
            # Process response
            all_members = {}  # Dict mapping dates to sets of members
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("HistoricalDataResponse"):
                        security_data = msg.getElement("securityData")
                        field_data_array = security_data.getElement("fieldData")
                        
                        for i in range(field_data_array.numValues()):
                            field_data = field_data_array.getValue(i)
                            date = field_data.getElementAsDatetime("date").strftime("%Y-%m-%d")
                            
                            if field_data.hasElement("INDX_MEMBERS"):
                                members_data = field_data.getElement("INDX_MEMBERS")
                                members = set()
                                
                                for j in range(members_data.numValues()):
                                    member = members_data.getValueAsString(j)
                                    members.add(member)
                                
                                all_members[date] = members
            
            if event.eventType() == blpapi.Event.RESPONSE:
                end_reached = True
            
            # Analyze changes between snapshots
            changes = []
            dates = sorted(all_members.keys())
            
            for i in range(1, len(dates)):
                prev_date = dates[i-1]
                curr_date = dates[i]
                
                prev_members = all_members[prev_date]
                curr_members = all_members[curr_date]
                
                # Find additions
                additions = curr_members - prev_members
                for ticker in additions:
                    changes.append({
                        'effective_date': curr_date,
                        'announcement_date': None,
                        'ticker': ticker,
                        'bloomberg_ticker': f"{ticker} Equity",
                        'change_type': 'ADD',
                        'old_weight': 0.0,
                        'new_weight': 0.0,
                        'reason': 'Detected by comparison'
                    })
                
                # Find deletions
                deletions = prev_members - curr_members
                for ticker in deletions:
                    changes.append({
                        'effective_date': curr_date,
                        'announcement_date': None,
                        'ticker': ticker,
                        'bloomberg_ticker': f"{ticker} Equity",
                        'change_type': 'DELETE',
                        'old_weight': 0.0,
                        'new_weight': 0.0,
                        'reason': 'Detected by comparison'
                    })
            
            return pd.DataFrame(changes)
            
        except Exception as e:
            self.logger.error(f"Error in alternative method for index changes: {e}")
            return pd.DataFrame()

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