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
        if self.session is not None and self.session.openService():
            return True
            
        try:
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
                            data_point = {"date": field_values.getElementAsDatetime("date").date()}
                            
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
        Get current members and their weights in an index
        
        Args:
            index_ticker: Bloomberg index identifier (e.g., "SPX Index")
            
        Returns:
            DataFrame with member tickers and weights
        """
        if not self.start_session():
            return pd.DataFrame()
            
        try:
            # Get reference data service
            refdata_service = self.session.getService("//blp/refdata")
            
            # Create request for index members and weights
            request = refdata_service.createRequest("ReferenceDataRequest")
            request.append("securities", index_ticker)
            request.append("fields", "INDX_MWEIGHT_HIST")
            request.append("fields", "INDX_MEMBERS")
            
            # Send the request
            self.session.sendRequest(request)
            
            # Process the response
            members_data = []
            end_reached = False
            
            while not end_reached:
                event = self.session.nextEvent(500)
                
                for msg in event:
                    if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                        security_data = msg.getElement("securityData")
                        
                        if security_data.hasElement("fieldData"):
                            field_data = security_data.getElement("fieldData")
                            
                            if field_data.hasElement("INDX_MWEIGHT_HIST"):
                                weight_data = field_data.getElement("INDX_MWEIGHT_HIST")
                                
                                for i in range(weight_data.numValues()):
                                    member_info = weight_data.getValue(i)
                                    member = {
                                        "ticker": member_info.getElementAsString("Index Member"),
                                        "weight": member_info.getElementAsFloat("Percent Weight"),
                                        "index": index_ticker
                                    }
                                    members_data.append(member)
                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    end_reached = True
                    
            return pd.DataFrame(members_data)
            
        except Exception as e:
            self.logger.error(f"Error retrieving index member weights: {e}")
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