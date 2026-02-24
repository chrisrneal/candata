"""
candata_pipeline.sources — data source adapters.

Each source wraps one external data provider:
  StatCanSource     — Statistics Canada WDS (CSV bulk downloads)
  BankOfCanadaSource — Bank of Canada Valet API (JSON observations)
  CMHCSource        — CMHC Housing Market Information Portal
  OpenCanadaSource  — open.canada.ca CKAN API (generic)
  ProcurementSource — Federal proactive-disclosure contract CSV
  CRACharitiesSource — CRA T3010 charity data
"""

from candata_pipeline.sources.bankofcanada import BankOfCanadaSource
from candata_pipeline.sources.cmhc import CMHCSource
from candata_pipeline.sources.cra_charities import CRACharitiesSource
from candata_pipeline.sources.opencanada import OpenCanadaSource
from candata_pipeline.sources.procurement import ProcurementSource
from candata_pipeline.sources.statcan import StatCanSource

__all__ = [
    "StatCanSource",
    "BankOfCanadaSource",
    "CMHCSource",
    "OpenCanadaSource",
    "ProcurementSource",
    "CRACharitiesSource",
]
