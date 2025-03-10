"""
Configuration file for Bloomberg field mappings and standardization
"""

# Index constituent field mappings 
# Maps Bloomberg fields to our standardized field names
INDEX_CONSTITUENT_FIELDS = {
    # Bloomberg field name -> Our standardized name
    "member_ticker_and_exchange_code": "ticker",
    "percentage_weight": "weight",
    "security_name": "name",
    "Member Ticker and Exchange Code": "ticker",
    "Percentage Weight": "weight", 
    "Security Name": "name",
    "Member Short Name": "name",
    "Ticker": "ticker",
    "Weight": "weight",
    "Name": "name"
}

# Index change field mappings
INDEX_CHANGE_FIELDS = {
    "effective_date": "effective_date",
    "Effective Date": "effective_date",
    "announcement_date": "announcement_date", 
    "Announcement Date": "announcement_date",
    "ticker": "ticker",
    "Ticker": "ticker",
    "ID_BB_SEC": "bloomberg_ticker",
    "change_type": "change_type",
    "Type": "change_type",
    "old_weight": "old_weight",
    "Old Weight": "old_weight",
    "new_weight": "new_weight",
    "New Weight": "new_weight",
    "reason": "reason",
    "Reason": "reason"
}

# Security info field mappings
SECURITY_INFO_FIELDS = {
    "security_name": "name",
    "SECURITY_NAME": "name",
    "gics_sector_name": "sector",
    "GICS_SECTOR_NAME": "sector",
    "gics_industry_name": "industry",
    "GICS_INDUSTRY_NAME": "industry",
    "security_type": "security_type",
    "SECURITY_TYP": "security_type"
} 