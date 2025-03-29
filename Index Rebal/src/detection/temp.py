from src.detection.russell_screener import RussellEligibilityScreener
from src.data.bloomberg_client import BloombergClient
from src.data.database import IndexDatabase

# Initialize dependencies
bbg = BloombergClient()
db = IndexDatabase()

# Create the screener
screener = RussellEligibilityScreener(
    bloomberg_client=bbg,
    db=db,
    recon_year=2023
)

# Run the full screening process
results = screener.run()

# Access specific results
eligible_universe = results["eligible_universe"]
russell1000_additions = results["index_changes"]["Russell 1000_additions"] 
russell2000_deletions = results["index_changes"]["Russell 2000_deletions"]

# Export results for further analysis
eligible_universe.to_excel("eligible_universe.xlsx")