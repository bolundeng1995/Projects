import logging
import os
from datetime import datetime, timedelta

# Import core classes
from src.data.database import IndexDatabase
from src.data.bloomberg_client import BloombergClient
from src.data.importers.price_data import PriceDataImporter
from src.data.importers.index_constituents import IndexConstituentImporter
from src.data.importers.index_constituent_analyzer import IndexConstituentAnalyzer
from src.data.corporate_action_handler import CorporateActionHandler
from src.data.calendar import RebalanceCalendar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rebalance_strategy.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def initialize_database(db_path='index_rebalance.db'):
    """Initialize the database with initial indices to track"""
    db = IndexDatabase(db_path)
    
    # Add indices to track
    logger.info("Adding indices to track...")
    
    # S&P indices
    db.add_index('SP500', 'S&P 500', 'SPX Index', 'Quarterly', 
                description='Large-cap US equities')
    db.add_index('SP400', 'S&P MidCap 400', 'MID Index', 'Quarterly',
                description='Mid-cap US equities')
    db.add_index('SP600', 'S&P SmallCap 600', 'SML Index', 'Quarterly',
                description='Small-cap US equities')
    
    # Russell indices
    db.add_index('RUSSELL1000', 'Russell 1000', 'RIY Index', 'Annual',
                description='Large-cap US equities')
    db.add_index('RUSSELL2000', 'Russell 2000', 'RTY Index', 'Annual',
                description='Small-cap US equities')
    
    # Nasdaq indices
    db.add_index('NDX', 'Nasdaq 100', 'NDX Index', 'Quarterly',
                description='Large-cap tech-focused equities')
    
    logger.info("Database initialized with indices")
    return db

def main():
    """Main execution function demonstrating data management workflow"""
    
    # Initialize database and Bloomberg client
    db = initialize_database()
    bloomberg = BloombergClient()
    
    try:
        # Verify Bloomberg connection
        if not bloomberg.start_session():
            logger.error("Failed to connect to Bloomberg. Exiting.")
            return
            
        logger.info("Successfully connected to Bloomberg API")
        
        # Initialize data importers and handlers
        constituent_importer = IndexConstituentImporter(db, bloomberg)
        price_importer = PriceDataImporter(db, bloomberg)
        constituent_analyzer = IndexConstituentAnalyzer(db, bloomberg)
        corporate_handler = CorporateActionHandler(db, bloomberg)
        calendar = RebalanceCalendar(db, bloomberg)
        
        # Step 1: Import current constituents for all indices
        logger.info("Importing current constituents for all indices...")
        indices = db.get_all_indices()
        
        for index_id in indices['index_id']:
            logger.info(f"Importing constituents for {index_id}...")
            constituent_importer.import_current_constituents(index_id)
            
        # Step 2: Import historical price data (last 1 year)
        logger.info("Importing 1 year of historical price data...")
        lookback_days = 365
        
        # Import price data for indices
        price_importer.update_index_prices(lookback_days=lookback_days)
        
        # Import price data for all constituents
        price_importer.update_all_constituent_prices(lookback_days=lookback_days)
        
        # Step 3: Detect constituent changes (comparing current with historical)
        logger.info("Detecting recent constituent changes...")
        for index_id in indices['index_id']:
            changes = constituent_analyzer.detect_constituent_changes(index_id, lookback_days=30)
            
            if not changes.empty:
                logger.info(f"Detected {len(changes)} constituent changes for {index_id}")
                
                # Store changes in the database
                for _, change in changes.iterrows():
                    db.add_constituent_change(
                        index_id=change['index_id'],
                        ticker=change['ticker'],
                        bloomberg_ticker=change['bloomberg_ticker'],
                        event_type=change['event_type'],
                        detection_date=change['detection_date']
                    )
            else:
                logger.info(f"No constituent changes detected for {index_id}")
                
        # Step 4: Fetch rebalance announcement dates
        logger.info("Fetching upcoming rebalance announcement dates...")
        for index_id in indices['index_id']:
            dates = constituent_analyzer.fetch_announcement_dates(index_id)
            
            if dates:
                logger.info(f"Fetched rebalance dates for {index_id}: {dates}")
                
                # Add to calendar
                for event_type, event_dates in dates.items():
                    if 'announcement_date' in event_dates and 'implementation_date' in event_dates:
                        calendar.add_event({
                            'index_id': index_id,
                            'event_type': event_type,
                            'announcement_date': event_dates['announcement_date'],
                            'implementation_date': event_dates['implementation_date'],
                            'description': f"{index_id} {event_type}"
                        })
            else:
                logger.info(f"No rebalance dates found for {index_id}")
                
        # Step 5: Check for corporate actions
        logger.info("Checking for corporate actions...")
        
        # Update corporate actions
        corporate_handler.update_corporate_actions()
        
        # Specifically check for M&A activity
        ma_count = corporate_handler.update_mergers_acquisitions()
        logger.info(f"Found {ma_count} relevant M&A events")
        
        # Check for ticker changes
        ticker_changes = corporate_handler.update_ticker_changes()
        logger.info(f"Found {ticker_changes} ticker changes")
        
        # Step 6: Display upcoming rebalance events
        upcoming_events = calendar.get_upcoming_events(days_ahead=60)
        
        if not upcoming_events.empty:
            logger.info("Upcoming rebalance events:")
            for _, event in upcoming_events.iterrows():
                days_to_announce = event.get('days_to_announcement')
                days_to_implement = event.get('days_to_implementation')
                
                logger.info(f"  {event['index_id']} {event['event_type']}:")
                
                if days_to_announce is not None and days_to_announce > 0:
                    logger.info(f"    Announcement in {days_to_announce} days ({event['announcement_date']})")
                
                if days_to_implement is not None:
                    logger.info(f"    Implementation in {days_to_implement} days ({event['implementation_date']})")
        else:
            logger.info("No upcoming rebalance events found")
            
        # Step 7: Analyze historical patterns
        logger.info("Analyzing historical patterns for indices...")
        for index_id in indices['index_id']:
            patterns = constituent_analyzer.analyze_historical_patterns(index_id)
            if patterns:
                logger.info(f"Historical pattern analysis for {index_id}:")
                logger.info(f"  Total changes: {patterns.get('total_changes', 0)}")
                logger.info(f"  Additions: {patterns.get('addition_count', 0)}")
                logger.info(f"  Deletions: {patterns.get('deletion_count', 0)}")
                logger.info(f"  Avg days to implementation: {patterns.get('avg_days_to_implementation')}")
                
        logger.info("Data management workflow completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data management workflow: {e}", exc_info=True)
    finally:
        # Clean up Bloomberg session
        bloomberg.stop_session()
        logger.info("Bloomberg session closed")

if __name__ == "__main__":
    main() 