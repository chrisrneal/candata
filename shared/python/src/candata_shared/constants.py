"""
constants.py — shared constants used across the pipeline and API.

All province SGC codes, CMA codes, indicator IDs, and typed literals
are defined here so they stay in sync between Python packages.
"""

from __future__ import annotations

from typing import Final, Literal

# ---------------------------------------------------------------------------
# Provinces and territories: SGC code -> name
# ---------------------------------------------------------------------------
PROVINCES: Final[dict[str, str]] = {
    "10": "Newfoundland and Labrador",
    "11": "Prince Edward Island",
    "12": "Nova Scotia",
    "13": "New Brunswick",
    "24": "Quebec",
    "35": "Ontario",
    "46": "Manitoba",
    "47": "Saskatchewan",
    "48": "Alberta",
    "59": "British Columbia",
    "60": "Yukon",
    "61": "Northwest Territories",
    "62": "Nunavut",
}

PROVINCE_ABBREVIATIONS: Final[dict[str, str]] = {
    "10": "NL",
    "11": "PE",
    "12": "NS",
    "13": "NB",
    "24": "QC",
    "35": "ON",
    "46": "MB",
    "47": "SK",
    "48": "AB",
    "59": "BC",
    "60": "YT",
    "61": "NT",
    "62": "NU",
}

# Reverse maps
PROVINCE_NAME_TO_CODE: Final[dict[str, str]] = {v: k for k, v in PROVINCES.items()}
ABBREVIATION_TO_CODE: Final[dict[str, str]] = {
    v: k for k, v in PROVINCE_ABBREVIATIONS.items()
}

# ---------------------------------------------------------------------------
# Top CMAs: SGC code -> name
# ---------------------------------------------------------------------------
CMA_CODES: Final[dict[str, str]] = {
    "505": "St. John's",
    "225": "Halifax",
    "305": "Moncton",
    "310": "Saint John",
    "408": "Québec",
    "462": "Sherbrooke",
    "433": "Trois-Rivières",
    "505": "Ottawa-Gatineau",  # Note: straddles ON/QC; listed under QC side
    "535": "Kingston",
    "537": "Belleville-Quinte West",
    "541": "Peterborough",
    "543": "Oshawa",
    "535": "Ottawa-Gatineau (Ontario part)",
    "568": "Hamilton",
    "570": "St. Catharines-Niagara",
    "580": "Kitchener-Cambridge-Waterloo",
    "590": "Brantford",
    "595": "Guelph",
    "596": "Barrie",
    "598": "Orillia",
    "602": "Greater Sudbury",
    "612": "Thunder Bay",
    "505": "Ottawa-Gatineau",
    "462": "Sherbrooke",
    "505": "Toronto",
    "933": "Vancouver",
    "915": "Victoria",
    "825": "Calgary",
    "835": "Edmonton",
    "725": "Winnipeg",
    "705": "Regina",
    "725": "Saskatoon",
    "996": "Kelowna",
    "975": "Abbotsford-Mission",
    "952": "Chilliwack",
    "944": "Nanaimo",
}

# Canonical top-35 CMAs with unique codes
CMA_CODES_CANONICAL: Final[dict[str, str]] = {
    "001": "Toronto",
    "002": "Montréal",
    "003": "Vancouver",
    "004": "Calgary",
    "005": "Edmonton",
    "006": "Ottawa-Gatineau",
    "007": "Winnipeg",
    "008": "Québec",
    "009": "Hamilton",
    "010": "Kitchener-Cambridge-Waterloo",
    "011": "Abbotsford-Mission",
    "012": "Halifax",
    "013": "Oshawa",
    "014": "London",
    "015": "Victoria",
    "016": "St. Catharines-Niagara",
    "017": "Windsor",
    "018": "Saskatoon",
    "019": "Regina",
    "020": "Sherbrooke",
    "021": "St. John's",
    "022": "Barrie",
    "023": "Kelowna",
    "024": "Abbotsford-Mission",
    "025": "Greater Sudbury",
    "026": "Kingston",
    "027": "Saguenay",
    "028": "Trois-Rivières",
    "029": "Guelph",
    "030": "Moncton",
    "031": "Brantford",
    "032": "Thunder Bay",
    "033": "Saint John",
    "034": "Peterborough",
    "035": "Lethbridge",
}

# ---------------------------------------------------------------------------
# Indicator IDs — must match seeds/indicators.sql
# ---------------------------------------------------------------------------
INDICATOR_IDS: Final[list[str]] = [
    "gdp_monthly",
    "cpi_monthly",
    "unemployment_rate",
    "employment_monthly",
    "retail_sales_monthly",
    "overnight_rate",
    "prime_rate",
    "mortgage_5yr_fixed",
    "usdcad",
    "vacancy_rate",
    "average_rent",
    "housing_starts",
]

# ---------------------------------------------------------------------------
# Typed literals
# ---------------------------------------------------------------------------
Frequency = Literal["daily", "weekly", "monthly", "quarterly", "semi-annual", "annual"]
DataSource = Literal["StatCan", "BoC", "CMHC", "CanadaBuys"]
Tier = Literal["free", "starter", "pro", "business", "enterprise"]
GeographyLevel = Literal["country", "pr", "cd", "csd", "cma", "ca", "fsa"]
TradeDirection = Literal["import", "export"]
BedroomType = Literal["bachelor", "1br", "2br", "3br+", "total"]
DwellingType = Literal["single", "semi", "row", "apartment", "total"]
PipelineStatus = Literal["running", "success", "partial_failure", "failure"]
