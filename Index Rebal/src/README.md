# Index Rebalance Strategy System

## System Architecture

This directory contains the implementation of an index rebalance trading strategy system. The system is organized into several components for data management, signal generation, and strategy execution.

## Directory Structure

## Core Components

### Data Layer

- **`IndexDatabase`** (`database.py`): SQLite-based data persistence with tables for indices, constituents, prices, and events.
  
- **`BloombergClient`** (`bloomberg_client.py`): Interface to Bloomberg API for retrieving market data, corporate actions, and index information.
  
- **`RebalanceCalendar`** (`calendar.py`): Manages upcoming rebalance events for different indices with notification system.
  
- **`CorporateActionHandler`** (`corporate_action_handler.py`): Processes corporate actions (mergers, spinoffs, ticker changes) affecting index constituents.

### Importers

- **`PriceDataImporter`**: Imports historical price data for indices and constituents with proper handling of date formatting.
  
- **`IndexConstituentImporter`**: Manages the import of current index constituents with specific providers for different index families:
  - `SPConstituentProvider`: S&P indices
  - `RussellConstituentProvider`: Russell indices
  - `NasdaqConstituentProvider`: Nasdaq indices
  
- **`IndexConstituentAnalyzer`**: Analyzes patterns in index constituent changes over time.

### Signal Generation

- **`SignalGenerator`**: Takes calendar events, price data, and constituent information to generate trading signals.

### Strategy Implementation

- **`BaseStrategy`**: Abstract base class for implementing specific trading strategies around index rebalances.

## Database Schema

The system uses an SQLite database with 6 primary tables:

1. **`index_metadata`**: Information about tracked indices
2. **`current_constituents`**: Current index constituent data
3. **`constituent_changes`**: Historical record of index changes
4. **`price_data`**: Time series of price data
5. **`rebalance_events`**: Calendar of rebalance events
6. **`corporate_actions`**: Corporate actions affecting constituents

## Using the Components

### Example: Setting up data management workflow 

```python
from src.data.database import IndexDatabase
from src.data.bloomberg_client import BloombergClient
from src.data.importers.index_constituents import IndexConstituentImporter
from src.data.importers.price_data import PriceDataImporter
from src.data.calendar import RebalanceCalendar

# Initialize core components
db = IndexDatabase('index_rebalance.db')
bloomberg = BloombergClient()
constituent_importer = IndexConstituentImporter(db, bloomberg)
price_importer = PriceDataImporter(db, bloomberg)
calendar = RebalanceCalendar(db, bloomberg)

# Import constituents
indices = db.get_all_indices()
for index_id in indices['index_id']:
    constituent_importer.import_current_constituents(index_id)

# Update price data
price_importer.update_index_prices(indices['index_id'].tolist(), lookback_days=365)
price_importer.update_all_constituent_prices(lookback_days=365)

# Update rebalance calendar
calendar.update_all_calendars()
```

### Example: Analyzing rebalance events

```python
from src.data.importers.index_constituent_analyzer import IndexConstituentAnalyzer

# Initialize analyzer
analyzer = IndexConstituentAnalyzer(db, bloomberg)

# Get patterns for an index
patterns = analyzer.analyze_historical_patterns('SP500', lookback_years=3)

# Key metrics
print(f"Average additions per rebalance: {patterns['avg_additions_per_rebalance']}")
print(f"Average price impact: {patterns['avg_price_impact']}%")
```

## Integration with Bloomberg API

The system is designed to work with the Bloomberg Terminal API (BLPAPI) for data retrieval. A valid Bloomberg Terminal subscription is required to use the full functionality. The `BloombergClient` class provides methods to:

1. Retrieve historical price data
2. Get index constituent information
3. Query upcoming rebalance dates
4. Fetch corporate action data

For development without Bloomberg access, mock implementations can be created.

## Further Development

Potential enhancements to the system:

1. Add more index families (MSCI, FTSE, etc.)
2. Implement specific rebalance trading strategies
3. Add performance tracking and backtesting
4. Develop order execution integration
5. Create dashboard for monitoring signals and trades