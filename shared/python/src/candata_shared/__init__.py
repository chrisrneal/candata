"""
candata_shared — shared utilities, models, and configuration for the candata platform.

Usage:
    from candata_shared.config import settings
    from candata_shared.db import get_supabase_client, get_duckdb_connection
    from candata_shared.models.indicators import Indicator, IndicatorValue
    from candata_shared.geo import normalize_statcan_geo
    from candata_shared.constants import PROVINCES, INDICATOR_IDS
"""

__version__ = "0.1.0"
