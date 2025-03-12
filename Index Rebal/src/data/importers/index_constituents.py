import pandas as pd
import os
import zipfile
import glob
import logging
from datetime import datetime
from typing import List, Dict, Optional, Union, Set

# Define the constant data folder path
DEFAULT_DATA_FOLDER = r"\\deai.us.world.socgen\ebtsadm\indexman\PROD\RAW_FILES"

class FileBasedConstituentProvider:
    """Provider that loads constituent data from text files"""
    
    def __init__(self, database, data_folder: str = DEFAULT_DATA_FOLDER):
        self.data_folder = data_folder
        self.logger = logging.getLogger(__name__)
        self.db = database
        self.logger.info(f"Using constituent data folder: {self.data_folder}")
        
    def get_constituents(self, index_id: str, reference_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get constituents for an index as of a specific date
        
        Args:
            index_id: Index identifier
            reference_date: Optional reference date (defaults to latest available)
            
        Returns:
            DataFrame with constituent information
        """
        # Get the index ticker from the database
        index_ticker = self._get_index_ticker(index_id)
        if not index_ticker:
            self.logger.error(f"No ticker found for index_id: {index_id}")
            return pd.DataFrame()
            
        # If no reference date provided, get the latest file
        if not reference_date:
            return self._get_latest_constituents(index_id, index_ticker)
        else:
            return self._get_historical_constituents(index_id, index_ticker, reference_date)
    
    def _get_index_ticker(self, index_id: str) -> str:
        """Get the index ticker (without 'Index') from index_id"""
        try:
            # Query the database for the Bloomberg ticker
            query = "SELECT bloomberg_ticker FROM index_metadata WHERE index_id = ?"
            result = pd.read_sql_query(query, self.db.conn, params=(index_id,))
            
            if result.empty:
                self.logger.error(f"No Bloomberg ticker found for index_id: {index_id}")
                return ""
                
            # Get the Bloomberg ticker and remove the 'Index' suffix if present
            bloomberg_ticker = result.iloc[0]['bloomberg_ticker']
            index_ticker = bloomberg_ticker.replace(" Index", "")
            
            return index_ticker
            
        except Exception as e:
            self.logger.error(f"Error retrieving index ticker for {index_id}: {e}")
            return ""
    
    def _get_latest_constituents(self, index_id: str, index_ticker: str) -> pd.DataFrame:
        """Get the most recent constituent file for an index"""
        # Get the parent folder for this index family
        index_folder = self._get_index_folder_path(index_id)
        
        if not os.path.exists(index_folder):
            self.logger.warning(f"No data folder found for {index_id} at {index_folder}")
            return pd.DataFrame()
            
        # Get all constituent files in the folder that match the index ticker
        file_pattern = os.path.join(index_folder, f"*_{index_ticker}_WSOD_01.txt")
        files = glob.glob(file_pattern)
        
        if not files:
            self.logger.warning(f"No constituent files found for {index_id} with ticker {index_ticker}")
            return pd.DataFrame()
            
        # Sort by date (files are named: {ref_date}_{index_ticker}_WSOD_01.txt)
        # If multiple files exist for the same date, we'll just take the first one
        files.sort(reverse=True)
        latest_file = files[0]
        
        # Extract reference date from filename
        file_basename = os.path.basename(latest_file)
        reference_date = file_basename.split('_')[0]
        
        self.logger.info(f"Using constituent file: {latest_file} for {index_id} ({index_ticker})")
        return self._parse_constituent_file(latest_file, index_id, reference_date)
    
    def _get_historical_constituents(self, index_id: str, index_ticker: str, reference_date: str) -> pd.DataFrame:
        """Get constituents for a specific historical date"""
        # Get the parent folder for this index family
        index_folder = self._get_index_folder_path(index_id)
        
        # First, check if we have a direct file for this date and ticker
        specific_file = os.path.join(index_folder, f"{reference_date}_{index_ticker}_WSOD_01.txt")
        direct_files = glob.glob(specific_file)
        
        if direct_files:
            self.logger.info(f"Using direct constituent file: {direct_files[0]}")
            return self._parse_constituent_file(direct_files[0], index_id, reference_date)
        
        # If no direct file, check zip archives
        year, month, day = reference_date.split('-')
        zip_pattern = os.path.join(self.data_folder, f"{year}_{month}_{day}_RAW_FILES.zip")
        zip_files = glob.glob(zip_pattern)
        
        if zip_files:
            # Use the exact date zip file
            zip_file = zip_files[0]
            return self._extract_from_zip(zip_file, index_id, index_ticker, reference_date)
        else:
            # Find the closest archive before the requested date
            all_zips = glob.glob(os.path.join(self.data_folder, '*_RAW_FILES.zip'))
            valid_zips = []
            
            for zip_file in all_zips:
                zip_basename = os.path.basename(zip_file)
                zip_date = zip_basename.split('_')[0:3]
                zip_date_str = '-'.join(zip_date)
                
                if zip_date_str <= reference_date:
                    valid_zips.append((zip_date_str, zip_file))
            
            if not valid_zips:
                self.logger.warning(f"No historical data found for {index_id} before {reference_date}")
                return pd.DataFrame()
                
            # Get the most recent zip file
            valid_zips.sort(reverse=True)
            closest_zip = valid_zips[0][1]
            closest_date = valid_zips[0][0]
            
            return self._extract_from_zip(closest_zip, index_id, index_ticker, closest_date)
    
    def _extract_from_zip(self, zip_file: str, index_id: str, index_ticker: str, reference_date: str) -> pd.DataFrame:
        """Extract constituent file from a zip archive"""
        try:
            # Determine the folder name for this index family
            folder_name = self._get_index_folder_path(index_id).split('/')[-1]
            
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Look specifically for the file matching this index ticker
                target_file = None
                expected_filename = f"{reference_date}_{index_ticker}_WSOD_01.txt"
                expected_path = f"{folder_name}/{expected_filename}"
                
                # Check for different potential path structures
                for file in zip_ref.namelist():
                    # Look for exact match with or without folder path
                    if (file == expected_filename or 
                        file == expected_path or 
                        file.endswith(f"/{expected_filename}")):
                        target_file = file
                        break
                
                # If not found with exact match, try a more flexible search but still specific to ticker
                if not target_file:
                    for file in zip_ref.namelist():
                        # Match files in the correct folder with the correct ticker
                        if (f"{folder_name}/" in file and 
                            f"_{index_ticker}_WSOD_01.txt" in file):
                            target_file = file
                            break
                
                if not target_file:
                    self.logger.warning(f"No constituent file for {index_ticker} found in archive {zip_file}")
                    return pd.DataFrame()
                
                # Extract to a temp location
                temp_dir = os.path.join(self.data_folder, 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                zip_ref.extract(target_file, temp_dir)
                
                # Parse the extracted file
                extracted_path = os.path.join(temp_dir, target_file)
                self.logger.info(f"Extracted constituent file: {target_file} from {zip_file}")
                result = self._parse_constituent_file(extracted_path, index_id, reference_date)
                
                # Clean up
                os.remove(extracted_path)
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error extracting from zip {zip_file}: {e}")
            return pd.DataFrame()
    
    def _parse_constituent_file(self, file_path: str, index_id: str, reference_date: str) -> pd.DataFrame:
        """Parse a constituent file and return a DataFrame"""
        try:
            # Read the constituent file
            # Assuming a standard format - adjust parsing logic as needed for your files
            df = pd.read_csv(file_path, delimiter='\t')
            
            # Standard field mapping - adjust based on your file format
            field_mapping = {
                'Symbol': 'symbol',
                'Index Shares': 'index_shares',
                'Index Weight': 'index_weight',
                'Closing Price': 'closing_price',
                'Market Value': 'market_value',
                'SEDOL': 'sedol',
                'CUSIP': 'cusip',
                'ISIN': 'isin'
            }
            
            # Rename columns based on mapping
            df = df.rename(columns={k: v for k, v in field_mapping.items() if k in df.columns})
            
            # Add reference date and index_id
            df['reference_date'] = reference_date
            df['index_id'] = index_id
            
            # Clean up and convert data types
            for col in ['index_shares', 'index_weight', 'closing_price', 'market_value']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Ensure all required columns exist
            required_columns = ['symbol', 'index_weight', 'reference_date', 'index_id']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None if col != 'index_weight' else 0.0
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing constituent file {file_path}: {e}")
            return pd.DataFrame()

    def _get_index_folder_path(self, index_id: str) -> str:
        """
        Maps the index_id to the correct folder path based on index family
        
        Args:
            index_id: Index identifier (e.g., 'SP500', 'RUSSELL2000')
            
        Returns:
            Path to the folder containing constituent data
        """
        # Map index ID prefixes to folder names
        if index_id.startswith('SP'):
            folder_name = 'SP'
        elif index_id.startswith('RUSSELL'):
            folder_name = 'RUSSELL'
        elif index_id.startswith('NASDAQ'):
            folder_name = 'NASDAQ'
        elif index_id.startswith('MSCI'):
            folder_name = 'MSCI'
        else:
            # Use the index_id itself as the folder name for other indices
            folder_name = index_id
        
        return os.path.join(self.data_folder, folder_name)

class IndexConstituentImporter:
    def __init__(self, database, data_folder: str = DEFAULT_DATA_FOLDER):
        self.db = database
        self.logger = logging.getLogger(__name__)
        self.provider = FileBasedConstituentProvider(database, data_folder)
    
    def import_current_constituents(self, index_id: str) -> int:
        """
        Import current constituents for a specific index
        
        Args:
            index_id: Index identifier
            
        Returns:
            Number of constituents imported
        """
        constituents = self.provider.get_constituents(index_id)
        
        if constituents.empty:
            self.logger.warning(f"No constituents found for {index_id}")
            return 0
        
        # Add constituents to database
        count = 0
        for _, row in constituents.iterrows():
            if self.db.add_index_constituent(row.to_dict()):
                count += 1
        
        self.logger.info(f"Imported {count} constituents for {index_id}")
        return count
    
    def import_historical_constituents(self, index_id: str, reference_date: str) -> int:
        """
        Import historical constituents for a specific index and date
        
        Args:
            index_id: Index identifier
            reference_date: Date in YYYY-MM-DD format
            
        Returns:
            Number of constituents imported
        """
        constituents = self.provider.get_constituents(index_id, reference_date)
        
        if constituents.empty:
            self.logger.warning(f"No historical constituents found for {index_id} on {reference_date}")
            return 0
        
        # Add constituents to database
        count = 0
        for _, row in constituents.iterrows():
            if self.db.add_index_constituent(row.to_dict()):
                count += 1
        
        self.logger.info(f"Imported {count} historical constituents for {index_id} as of {reference_date}")
        return count
    
    def import_all_available_history(self, index_id: str) -> int:
        """
        Import all available historical data for an index
        
        Args:
            index_id: Index identifier
            
        Returns:
            Number of dates imported
        """
        # Get list of all zip files
        zip_files = glob.glob(os.path.join(self.provider.data_folder, '*_RAW_FILES.zip'))
        
        if not zip_files:
            self.logger.warning(f"No historical archives found for {index_id}")
            return 0
        
        date_count = 0
        total_records = 0
        
        for zip_file in sorted(zip_files):
            # Extract date from zip filename
            zip_basename = os.path.basename(zip_file)
            date_parts = zip_basename.split('_')[0:3]
            reference_date = f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}"
            
            # Import data for this date
            count = self.import_historical_constituents(index_id, reference_date)
            
            if count > 0:
                date_count += 1
                total_records += count
        
        self.logger.info(f"Imported {total_records} constituents across {date_count} dates for {index_id}")
        return date_count
    
    def detect_changes(self, index_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Detect changes in index constituents between two dates
        
        Args:
            index_id: Index identifier
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with detected changes
        """
        # Get constituents for both dates
        start_constituents = self.provider.get_constituents(index_id, start_date)
        end_constituents = self.provider.get_constituents(index_id, end_date)
        
        if start_constituents.empty or end_constituents.empty:
            self.logger.warning(f"Missing data for comparison between {start_date} and {end_date}")
            return pd.DataFrame()
        
        # Extract symbols from both dates
        start_symbols = set(start_constituents['symbol'].unique())
        end_symbols = set(end_constituents['symbol'].unique())
        
        # Find additions and deletions
        additions = end_symbols - start_symbols
        deletions = start_symbols - end_symbols
        
        # Find weight changes for symbols in both sets
        common_symbols = start_symbols & end_symbols
        weight_changes = []
        
        for symbol in common_symbols:
            start_weight = start_constituents[start_constituents['symbol'] == symbol]['index_weight'].values[0]
            end_weight = end_constituents[end_constituents['symbol'] == symbol]['index_weight'].values[0]
            
            if abs(end_weight - start_weight) > 0.0001:  # Use a small threshold to account for floating point errors
                weight_changes.append({
                    'symbol': symbol,
                    'old_weight': start_weight,
                    'new_weight': end_weight,
                    'change': end_weight - start_weight
                })
        
        # Create a DataFrame with all changes
        changes = []
        
        # Add additions
        for symbol in additions:
            row = end_constituents[end_constituents['symbol'] == symbol].iloc[0]
            changes.append({
                'index_id': index_id,
                'symbol': symbol,
                'event_type': 'ADDITION',
                'start_date': start_date,
                'end_date': end_date,
                'old_weight': 0,
                'new_weight': row['index_weight'],
                'change': row['index_weight']
            })
        
        # Add deletions
        for symbol in deletions:
            row = start_constituents[start_constituents['symbol'] == symbol].iloc[0]
            changes.append({
                'index_id': index_id,
                'symbol': symbol,
                'event_type': 'DELETION',
                'start_date': start_date,
                'end_date': end_date,
                'old_weight': row['index_weight'],
                'new_weight': 0,
                'change': -row['index_weight']
            })
        
        # Add weight changes
        for change in weight_changes:
            changes.append({
                'index_id': index_id,
                'symbol': change['symbol'],
                'event_type': 'WEIGHT_CHANGE',
                'start_date': start_date,
                'end_date': end_date,
                'old_weight': change['old_weight'],
                'new_weight': change['new_weight'],
                'change': change['change']
            })
        
        return pd.DataFrame(changes) 