#!/usr/bin/env python
"""
Russell US Index Eligibility Screener

This module implements the FTSE Russell Index methodology to identify securities eligible
for inclusion in Russell US equity indexes (Russell 1000, Russell 2000, Russell 3000).
It uses Bloomberg data to apply screening criteria according to the official methodology.

References:
- Russell US Indexes Construction and Methodology Document
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Set, Any

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Import project modules
from src.data.bloomberg_client import BloombergClient
from src.data.database import IndexDatabase
from src.detection.scanners import BaseScanner

class RussellEligibilityScreener(BaseScanner):
    """
    Implementation of Russell US Indexes eligibility screening based on the official
    FTSE Russell methodology document.
    
    This class applies the multi-step screening process to identify securities eligible
    for inclusion in Russell US equity indexes during the annual reconstitution.
    """
    
    def __init__(self, 
                 bloomberg_client: Optional[BloombergClient] = None, 
                 db: Optional[IndexDatabase] = None,
                 rank_date: Optional[date] = None,
                 recon_year: int = None):
        """
        Initialize the Russell eligibility screener
        
        Args:
            bloomberg_client: Bloomberg client for data retrieval
            db: Database connection for storing/retrieving results
            rank_date: Rank date (first Friday in May, or specified date)
            recon_year: Reconstitution year (current year if None)
        """
        super().__init__(name="russell_screener")
        
        # Initialize Bloomberg client
        self.bbg = bloomberg_client or BloombergClient()
        
        # Initialize database connection
        self.db = db or IndexDatabase()
        
        # Set up logger
        self.logger = logging.getLogger('russell_screener')
        
        # Set reconstitution parameters
        self._set_recon_parameters(rank_date, recon_year)
        
        # Cache for data
        self.data_cache = {}
        
    def _set_recon_parameters(self, rank_date: Optional[date], recon_year: Optional[int]):
        """
        Set reconstitution parameters including key dates
        
        Args:
            rank_date: Rank date (first Friday in May, or specified date)
            recon_year: Reconstitution year
        """
        # Set reconstitution year (current year if not specified)
        current_year = datetime.now().year
        self.recon_year = recon_year or current_year
        
        # Set rank date (first Friday in May by default)
        if rank_date:
            self.rank_date = rank_date
        else:
            # Calculate first Friday in May for the reconstitution year
            may_1 = date(self.recon_year, 5, 1)
            days_to_friday = (4 - may_1.weekday()) % 7  # 4 = Friday
            self.rank_date = may_1 + timedelta(days=days_to_friday)
        
        # Calculate other important dates:
        # - Reconstitution effective date (last Friday in June)
        june_30 = date(self.recon_year, 6, 30)
        last_friday_offset = (june_30.weekday() - 4) % 7
        self.recon_date = june_30 - timedelta(days=last_friday_offset)
        
        # - Preliminary add/delete lists (typically first Friday in June)
        june_1 = date(self.recon_year, 6, 1)
        days_to_friday = (4 - june_1.weekday()) % 7
        self.prelim_date = june_1 + timedelta(days=days_to_friday)
        
        # - Final add/delete lists (typically second or third Friday in June)
        self.final_date = self.prelim_date + timedelta(days=14)  # Two weeks after prelim
        
        # Market cap thresholds (these are determined after ranking)
        # Setting initial placeholders - actual values determined during screening
        self.russell1000_threshold = None
        self.russell2000_threshold = None
        self.russell3000_threshold = None
        
        self.logger.info(f"Russell Reconstitution {self.recon_year} parameters set:")
        self.logger.info(f"  - Rank Date: {self.rank_date}")
        self.logger.info(f"  - Prelim Date: {self.prelim_date}")
        self.logger.info(f"  - Final Date: {self.final_date}")
        self.logger.info(f"  - Recon Date: {self.recon_date}")
    
    def get_us_equity_universe(self) -> pd.DataFrame:
        """
        Get the universe of US-listed equities as the starting point for screening
        
        This implements the first step of Russell methodology - identifying securities
        listed on eligible US exchanges.
        
        Returns:
            DataFrame with the initial universe of US-listed securities
        """
        self.logger.info("Fetching initial US equity universe")
        
        # Use Bloomberg EQS to get all US-listed equities
        # Per Russell methodology, securities must be listed on eligible US exchanges
        tickers = self.bbg.execute_eqs_query(screen_name="RUSSELL UNIVERSE")
        
        if not tickers:
            self.logger.error("Failed to retrieve US equity universe")
            return pd.DataFrame()
        
        self.logger.info(f"Initial universe contains {len(tickers)} securities")
        
        # Get basic security info
        fields = [
            'SECURITY_NAME',        # Company name
            'CRNCY',                # Currency
            'EQY_SH_OUT',           # Shares outstanding
            'CUR_MKT_CAP',          # Current market cap
            'PX_LAST',              # Last price
            'PX_VOLUME',            # Trading volume
            'VOLUME_AVG_30D',       # 30-day average volume
            'EQY_FLOAT',            # Float percentage
            'SECURITY_TYP',         # Security type
            'CNTRY_OF_RISK',        # Country of risk
            'ID_ISIN',              # ISIN
            'INDUSTRY_SECTOR',      # Sector
            'EQY_FLOAT_PCT',        # Float percentage
            'COMPANY_TYPE',         # Company type (REIT, etc.)
            'PE_RATIO'              # P/E ratio
        ]
        
        # Get the data from Bloomberg
        securities_df = self.bbg.get_reference_data(tickers, fields, date=self.rank_date)
        
        if securities_df.empty:
            self.logger.error("Failed to retrieve security data from Bloomberg")
            return pd.DataFrame()
        
        # Cache the data
        self.data_cache['initial_universe'] = securities_df
        
        return securities_df
    
    def apply_eligibility_criteria(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Russell eligibility criteria to the universe
        
        This implements the multi-step screening process according to the Russell methodology:
        1. Exclude securities with ineligible company structures
        2. Apply minimum price criterion ($1.00)
        3. Apply market capitalization ranking
        4. Apply liquidity requirements
        5. Apply float criteria
        
        Args:
            universe_df: DataFrame with the initial universe
            
        Returns:
            DataFrame with eligible securities
        """
        self.logger.info("Applying Russell eligibility criteria")
        
        if universe_df.empty:
            self.logger.error("Empty universe provided")
            return pd.DataFrame()
        
        # Make a copy to avoid modifying the original
        df = universe_df.copy()
        
        # 1. Filter by security type (per Russell methodology)
        self.logger.info("Step 1: Filtering by security type and structure")
        
        # Exclude ineligible securities
        excluded_types = [
            'PREFERRED',            # Preferred shares
            'ETF',                  # ETFs
            'CLOSED-END FUND',      # Closed-end funds
            'OPEN-END FUND',        # Open-end funds
            'UNIT INVESTMENT TRUST',# UITs
            'ADR',                  # ADRs
            'GDR',                  # GDRs
            'REIT',                 # REITs (special handling per methodology)
            'LIMITED PARTNERSHIP'   # LPs
        ]
        
        # Count before filtering
        initial_count = len(df)
        
        # Filter out excluded types
        for ex_type in excluded_types:
            df = df[~df['SECURITY_TYP'].str.contains(ex_type, case=False, na=False)]
        
        # Count after filtering
        step1_count = len(df)
        self.logger.info(f"  - Removed {initial_count - step1_count} securities with ineligible types")
        
        # 2. Apply minimum price criterion
        self.logger.info("Step 2: Applying minimum price criterion ($1.00)")
        
        # Russell requires price >= $1.00 on rank date
        df = df[df['PX_LAST'] >= 1.0]
        
        step2_count = len(df)
        self.logger.info(f"  - Removed {step1_count - step2_count} securities below minimum price")
        
        # 3. Apply market capitalization requirements and ranking
        self.logger.info("Step 3: Applying market capitalization ranking")
        
        # Sort by market cap (descending)
        df = df.sort_values('CUR_MKT_CAP', ascending=False)
        
        # Add rank column
        df['RANK'] = range(1, len(df) + 1)
        
        # Calculate Russell 1000/2000/3000 thresholds based on cumulative market cap
        self._calculate_index_thresholds(df)
        
        # 4. Apply liquidity requirements
        self.logger.info("Step 4: Applying liquidity requirements")
        
        # Russell requires ADVT (Avg Daily Value Traded) > 0
        df['ADVT'] = df['VOLUME_AVG_30D'] * df['PX_LAST']
        df = df[df['ADVT'] > 0]
        
        step4_count = len(df)
        self.logger.info(f"  - Removed {step3_count - step4_count} securities failing liquidity test")
        
        # 5. Apply float criteria
        self.logger.info("Step 5: Applying float criteria")
        
        # Russell requires 5% minimum float
        df = df[df['EQY_FLOAT_PCT'] >= 5.0]
        
        step5_count = len(df)
        self.logger.info(f"  - Removed {step4_count - step5_count} securities failing float test")
        
        # Classify securities into Russell indexes based on market cap
        df['RUSSELL_INDEX'] = self._classify_by_market_cap(df)
        
        self.logger.info(f"Eligibility screening complete. {len(df)} eligible securities identified.")
        
        # Cache the results
        self.data_cache['eligible_universe'] = df
        
        return df
    
    def _calculate_index_thresholds(self, ranked_df: pd.DataFrame) -> None:
        """
        Calculate market cap thresholds for Russell indexes
        
        Args:
            ranked_df: DataFrame with securities ranked by market cap
        """
        # Russell 3000: top 3000 by market cap
        if len(ranked_df) >= 3000:
            self.russell3000_threshold = ranked_df.iloc[2999]['CUR_MKT_CAP']
        else:
            self.russell3000_threshold = ranked_df.iloc[-1]['CUR_MKT_CAP']
        
        # Russell 1000: top 1000 by market cap
        if len(ranked_df) >= 1000:
            self.russell1000_threshold = ranked_df.iloc[999]['CUR_MKT_CAP']
        else:
            self.russell1000_threshold = ranked_df.iloc[-1]['CUR_MKT_CAP']
        
        # Russell 2000: next 2000 after Russell 1000
        self.russell2000_threshold = self.russell3000_threshold
        
        # Set as class variables
        self.russell1000_count = min(1000, len(ranked_df))
        self.russell2000_count = min(2000, max(0, len(ranked_df) - 1000))
        self.russell3000_count = min(3000, len(ranked_df))
        
        self.logger.info(f"Market cap thresholds calculated:")
        self.logger.info(f"  - Russell 1000: ${self.russell1000_threshold/1e9:.2f}B (rank {self.russell1000_count})")
        self.logger.info(f"  - Russell 2000: ${self.russell2000_threshold/1e9:.2f}B (rank {self.russell1000_count+1}-{self.russell3000_count})")
        self.logger.info(f"  - Russell 3000: ${self.russell3000_threshold/1e9:.2f}B (rank {self.russell3000_count})")
    
    def _classify_by_market_cap(self, df: pd.DataFrame) -> pd.Series:
        """
        Classify securities into Russell indexes based on market cap
        
        Args:
            df: DataFrame with securities
            
        Returns:
            Series with index classification
        """
        # Create a Series to hold the index classification
        classification = pd.Series(index=df.index, dtype='object')
        
        # Classify based on rank
        for idx, row in df.iterrows():
            rank = row['RANK']
            
            if rank <= self.russell1000_count:
                classification[idx] = 'Russell 1000'
            elif rank <= self.russell3000_count:
                classification[idx] = 'Russell 2000'
            else:
                classification[idx] = 'Not Included'
        
        return classification
    
    def get_current_index_constituents(self, index_name: str) -> Set[str]:
        """
        Get the current constituents of a Russell index
        
        Args:
            index_name: Name of the index ('Russell 1000', 'Russell 2000', 'Russell 3000')
            
        Returns:
            Set of ticker symbols
        """
        self.logger.info(f"Getting current constituents for {index_name}")
        
        # Map index names to Bloomberg tickers
        index_ticker_map = {
            'Russell 1000': 'RIY Index',
            'Russell 2000': 'RTY Index',
            'Russell 3000': 'RAY Index'
        }
        
        if index_name not in index_ticker_map:
            self.logger.error(f"Unknown index: {index_name}")
            return set()
        
        bloomberg_ticker = index_ticker_map[index_name]
        
        # Try to get constituents from database first
        db_constituents = self._get_db_constituents(index_name)
        if db_constituents:
            return db_constituents
        
        # Otherwise get from Bloomberg
        return self._get_bbg_constituents(bloomberg_ticker)
    
    def _get_db_constituents(self, index_name: str) -> Set[str]:
        """Get index constituents from database"""
        try:
            # Map index name to index_id used in database
            index_id_map = {
                'Russell 1000': 'RUSSELL1000',
                'Russell 2000': 'RUSSELL2000',
                'Russell 3000': 'RUSSELL3000'
            }
            
            index_id = index_id_map.get(index_name)
            if not index_id:
                return set()
                
            # Query database for constituents
            constituents_df = self.db.get_index_constituents(index_id)
            
            if not constituents_df.empty:
                # Convert to set of tickers
                return set(constituents_df['symbol'].unique())
            
            return set()
            
        except Exception as e:
            self.logger.error(f"Error getting constituents from database: {e}")
            return set()
    
    def _get_bbg_constituents(self, bloomberg_ticker: str) -> Set[str]:
        """Get index constituents from Bloomberg"""
        try:
            # Get index members from Bloomberg
            constituents = self.bbg.get_index_members(bloomberg_ticker)
            
            # Convert to set
            return set(constituents)
            
        except Exception as e:
            self.logger.error(f"Error getting constituents from Bloomberg: {e}")
            return set()
    
    def predict_index_changes(self, eligible_universe: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Predict additions and deletions for Russell indexes
        
        Args:
            eligible_universe: DataFrame with eligible securities
            
        Returns:
            Dictionary with DataFrames for additions and deletions for each index
        """
        self.logger.info("Predicting Russell index changes")
        
        if eligible_universe.empty:
            self.logger.error("Empty eligible universe")
            return {}
        
        # Get list of relevant index names
        index_names = ['Russell 1000', 'Russell 2000', 'Russell 3000']
        
        # Dictionary to store results
        results = {}
        
        for index_name in index_names:
            # Get current constituents
            current_constituents = self.get_current_index_constituents(index_name)
            
            # Get eligible securities for this index
            eligible_for_index = eligible_universe[eligible_universe['RUSSELL_INDEX'] == index_name]
            eligible_tickers = set(eligible_for_index.index)
            
            # Identify additions and deletions
            additions = eligible_tickers - current_constituents
            deletions = current_constituents - eligible_tickers
            
            # Create DataFrame for additions
            additions_df = pd.DataFrame()
            if additions:
                # Get data for additions
                additions_df = eligible_for_index.loc[list(additions)].copy()
                additions_df['ACTION'] = 'Addition'
                additions_df['INDEX'] = index_name
            
            # Create DataFrame for deletions
            deletions_df = pd.DataFrame()
            if deletions:
                # Get data for deletions (with minimal info as they may not be in our universe)
                deletion_data = []
                for ticker in deletions:
                    deletion_data.append({
                        'TICKER': ticker,
                        'ACTION': 'Deletion',
                        'INDEX': index_name
                    })
                
                if deletion_data:
                    deletions_df = pd.DataFrame(deletion_data)
                    deletions_df.set_index('TICKER', inplace=True)
            
            # Store in results
            results[f"{index_name}_additions"] = additions_df
            results[f"{index_name}_deletions"] = deletions_df
            
            # Log summary
            self.logger.info(f"{index_name} changes: {len(additions)} additions, {len(deletions)} deletions")
        
        return results
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete Russell eligibility screening process
        
        Returns:
            Dictionary with screening results
        """
        self.logger.info(f"Running Russell eligibility screening for {self.recon_year}")
        
        # Step 1: Get US equity universe
        universe = self.get_us_equity_universe()
        
        if universe.empty:
            self.logger.error("Failed to get universe - aborting")
            return {"status": "error", "message": "Failed to get universe"}
        
        # Step 2: Apply eligibility criteria
        eligible_universe = self.apply_eligibility_criteria(universe)
        
        if eligible_universe.empty:
            self.logger.error("No eligible securities found - aborting")
            return {"status": "error", "message": "No eligible securities found"}
        
        # Step 3: Predict index changes
        index_changes = self.predict_index_changes(eligible_universe)
        
        # Step 4: Save results
        results = {
            "status": "success",
            "rank_date": self.rank_date.isoformat(),
            "recon_date": self.recon_date.isoformat(),
            "russell1000_threshold": self.russell1000_threshold,
            "russell2000_threshold": self.russell2000_threshold,
            "russell3000_threshold": self.russell3000_threshold,
            "initial_universe_count": len(universe),
            "eligible_universe_count": len(eligible_universe),
            "eligible_universe": eligible_universe,
            "index_changes": index_changes
        }
        
        # Save to files
        self._save_results(results)
        
        return results
    
    def _save_results(self, results: Dict[str, Any]) -> None:
        """
        Save screening results to files
        
        Args:
            results: Dictionary with screening results
        """
        # Create output directory
        output_dir = f"russell_screening_{self.recon_year}_{self.rank_date.strftime('%Y%m%d')}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save eligible universe
        results["eligible_universe"].to_excel(
            f"{output_dir}/russell_eligible_universe.xlsx"
        )
        
        # Save index changes
        with pd.ExcelWriter(f"{output_dir}/russell_index_changes.xlsx") as writer:
            # For each index, save additions and deletions
            for index_name in ['Russell 1000', 'Russell 2000', 'Russell 3000']:
                additions_df = results["index_changes"].get(f"{index_name}_additions", pd.DataFrame())
                deletions_df = results["index_changes"].get(f"{index_name}_deletions", pd.DataFrame())
                
                if not additions_df.empty:
                    additions_df.to_excel(writer, sheet_name=f"{index_name.replace(' ', '')}_Additions")
                
                if not deletions_df.empty:
                    deletions_df.to_excel(writer, sheet_name=f"{index_name.replace(' ', '')}_Deletions")
            
            # Summary sheet
            summary_data = {
                "Parameter": [
                    "Rank Date", 
                    "Reconstitution Date",
                    "Russell 1000 Threshold",
                    "Russell 2000 Threshold",
                    "Russell 3000 Threshold",
                    "Initial Universe Count",
                    "Eligible Universe Count"
                ],
                "Value": [
                    results["rank_date"],
                    results["recon_date"],
                    f"${results['russell1000_threshold']/1e9:.2f}B",
                    f"${results['russell2000_threshold']/1e9:.2f}B",
                    f"${results['russell3000_threshold']/1e9:.2f}B",
                    results["initial_universe_count"],
                    results["eligible_universe_count"]
                ]
            }
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)
        
        self.logger.info(f"Results saved to {output_dir}/")


def main():
    """Main function to run Russell eligibility screening"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('russell_screening.log')
        ]
    )
    
    logger = logging.getLogger('russell_screener.main')
    logger.info("Starting Russell eligibility screening")
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Russell US Index Eligibility Screener")
    
    parser.add_argument("--year", type=int, default=datetime.now().year,
                       help="Reconstitution year (default: current year)")
    
    parser.add_argument("--rank-date", type=str,
                       help="Rank date (YYYY-MM-DD, default: first Friday in May)")
    
    parser.add_argument("--db-path", type=str, default="index_data.db",
                       help="Path to database file (default: index_data.db)")
    
    args = parser.parse_args()
    
    # Set up rank date if provided
    rank_date = None
    if args.rank_date:
        try:
            rank_date = datetime.strptime(args.rank_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid rank date format: {args.rank_date}")
            return 1
    
    try:
        # Initialize database
        db = IndexDatabase(args.db_path)
        
        # Initialize Bloomberg client
        bbg = BloombergClient()
        
        # Initialize and run screener
        screener = RussellEligibilityScreener(
            bloomberg_client=bbg,
            db=db,
            rank_date=rank_date,
            recon_year=args.year
        )
        
        # Run screening
        results = screener.run()
        
        if results["status"] == "success":
            logger.info("Russell eligibility screening completed successfully")
            
            # Print summary
            print("\nRussell Index Screening Summary:")
            print(f"Rank Date: {results['rank_date']}")
            print(f"Initial Universe: {results['initial_universe_count']} securities")
            print(f"Eligible Universe: {results['eligible_universe_count']} securities")
            
            # Print index thresholds
            print(f"\nMarket Cap Thresholds:")
            print(f"Russell 1000: ${results['russell1000_threshold']/1e9:.2f}B")
            print(f"Russell 2000: ${results['russell2000_threshold']/1e9:.2f}B")
            print(f"Russell 3000: ${results['russell3000_threshold']/1e9:.2f}B")
            
            # Print change summary for each index
            for index_name in ['Russell 1000', 'Russell 2000', 'Russell 3000']:
                additions = results["index_changes"].get(f"{index_name}_additions", pd.DataFrame())
                deletions = results["index_changes"].get(f"{index_name}_deletions", pd.DataFrame())
                
                print(f"\n{index_name} Changes:")
                print(f"  Additions: {len(additions)}")
                print(f"  Deletions: {len(deletions)}")
            
            return 0
        else:
            logger.error(f"Error during screening: {results['message']}")
            return 1
            
    except Exception as e:
        logger.error(f"Error during screening: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main()) 