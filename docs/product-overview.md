# Candata — Canadian Public Data Intelligence

## The Problem

Canadian public data is a mess. Housing statistics live in CMHC's portal. Trade figures are scattered across Statistics Canada's labyrinthine SDMX feeds. Building permits require navigating municipal-level datasets with inconsistent formats. Price indices come from yet another source with different geography codes, different date conventions, and different access methods.

If you need to answer a simple question — "Which Canadian cities had the fastest growth in housing starts last year?" — you're looking at days of work: finding the right datasets, parsing proprietary formats, reconciling geography codes, and stitching together time series by hand. Organizations that need this data regularly either pay for expensive proprietary platforms or build fragile internal pipelines that break every time a government agency changes their API.

## The Solution

Candata consolidates Canada's most important public data sources into a single, clean, queryable platform. We pull from CMHC, Statistics Canada, UN Comtrade, and Teranet on a regular schedule, normalize everything into consistent formats, and serve it through a fast API and interactive dashboards.

No more scraping. No more CSV wrangling. No more broken links. Just ask the question and get the answer.

## Data Products

### Canadian Housing Market Intelligence

Track housing starts, completions, and units under construction across all 35 Census Metropolitan Areas. Break down by dwelling type (single-detached, semi-detached, row, apartment) and intended market (freehold, condo, rental). Monthly data from 2015 to present.

**Use case:** A real estate investment firm evaluating whether to enter the Halifax market pulls up the CMA summary for Halifax, compares 12-month rolling starts against Toronto and Calgary, and spots that Halifax apartment starts have grown 40% year-over-year while vacancy remains tight — signaling strong demand fundamentals.

### New Housing Price Index

Statistics Canada's NHPI tracks the price of new homes by CMA, broken into land and building cost components. Monthly updates reveal whether rising prices are driven by land scarcity or construction costs.

**Use case:** A policy analyst preparing a housing affordability brief isolates the land component of the NHPI for Vancouver and shows that land costs account for 70% of new home price growth since 2020, supporting the case for zoning reform over construction subsidies.

### Building Permits

Municipal-level building permit data broken down by structure type and work type. A leading indicator of future housing supply.

**Use case:** A construction materials supplier monitors building permit trends across Ontario municipalities to forecast demand for lumber and concrete, adjusting inventory orders three to six months ahead of actual construction activity.

### Teranet House Price Index

The benchmark resale house price index for Canada's major markets, tracking actual transaction prices rather than list prices.

**Use case:** A mortgage lender uses Teranet HPI trends across markets to calibrate loan-to-value ratios, tightening in markets where price growth has stalled and loosening where fundamentals remain strong.

### Canadian Trade Flows

Import and export data at the NAPCS and HS6 product code level, broken down by province and trading partner. Monthly data from 2019 to present.

**Use case:** A supply chain manager at an automotive parts manufacturer tracks monthly import volumes of HS code 8708 (motor vehicle parts) from China, Mexico, and Germany to anticipate supply disruptions and negotiate procurement contracts ahead of seasonal demand shifts.

### UN Comtrade Bilateral Trade

Annual bilateral trade data between Canada and its top 10 trading partners at the HS2 chapter level, sourced from the United Nations Comtrade database.

**Use case:** A journalist investigating Canada-China trade dependency pulls the top 20 import categories from China for the past five years and shows that critical minerals imports have tripled, providing data-backed evidence for a story on supply chain vulnerability.

## Get Access

### Free

Access the public dashboard with pre-built visualizations of housing and trade data. Limited API access for exploration and prototyping.

### Pro — $49/month

Full API access with generous rate limits. Data exports in CSV and JSON. Priority support. Ideal for analysts, researchers, and small teams.

### Enterprise — Custom Pricing

Dedicated API endpoints, custom data pipelines, SLA guarantees, and direct integration support. Built for organizations that need Canadian data infrastructure they can depend on.

Contact us at hello@candata.ca to get started.
