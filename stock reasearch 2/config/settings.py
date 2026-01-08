"""
Configuration settings for CSE Stock Research Tool
"""
import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = BASE_DIR / "reports"

# Create directories if they don't exist
for dir_path in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# CSE Website URLs
CSE_BASE_URL = "https://www.cse.lk"
CSE_API_BASE = "https://www.cse.lk/api"

# API Endpoints (discovered from CSE website)
ENDPOINTS = {
    "listed_companies": "/api/listingByDate",
    "company_profile": "/api/companyInfoSummery",
    "trade_summary": "/api/tradeSummary",
    "market_data": "/api/marketData",
    "price_list": "/api/priceList",
    "company_financials": "/api/companyFinancials",
    "indices": "/api/indices",
    "announcements": "/api/announcements",
    "historical_data": "/api/historicalData",
}

# Request settings
REQUEST_TIMEOUT = 30  # seconds
REQUEST_DELAY = 1  # seconds between requests (to be respectful)
MAX_RETRIES = 3
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Headers for requests
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": CSE_BASE_URL,
    "Origin": CSE_BASE_URL,
}

# Valuation thresholds (can be customized)
VALUATION_THRESHOLDS = {
    # Value Investing
    "pe_ratio_max": 15,           # P/E ratio should be below this
    "pb_ratio_max": 1.5,          # P/B ratio should be below this
    "debt_equity_max": 0.5,       # Debt to Equity should be below this
    
    # Dividend Investing
    "dividend_yield_min": 4.0,    # Dividend yield should be above this (%)
    "payout_ratio_max": 70,       # Payout ratio should be below this (%)
    
    # Growth Investing
    "eps_growth_min": 10,         # EPS growth should be above this (%)
    "revenue_growth_min": 10,     # Revenue growth should be above this (%)
    
    # Quality Investing
    "roe_min": 15,                # ROE should be above this (%)
    "profit_margin_min": 10,      # Profit margin should be above this (%)
    
    # GARP
    "peg_ratio_max": 1.0,         # PEG ratio should be below this
    
    # General
    "market_cap_min": 100_000_000,  # Minimum market cap (LKR)
    "avg_volume_min": 10000,        # Minimum average daily volume
}

# Scoring weights for overall ranking
SCORING_WEIGHTS = {
    "value_score": 0.25,
    "growth_score": 0.20,
    "quality_score": 0.20,
    "dividend_score": 0.15,
    "momentum_score": 0.10,
    "safety_score": 0.10,
}

# Industry sectors in CSE
CSE_SECTORS = [
    "Banks Finance & Insurance",
    "Beverage Food & Tobacco",
    "Chemicals & Pharmaceuticals",
    "Construction & Engineering",
    "Diversified Holdings",
    "Footwear & Textiles",
    "Healthcare",
    "Hotels & Travel",
    "Information Technology",
    "Investment Trusts",
    "Land & Property",
    "Manufacturing",
    "Motors",
    "Oil Palms",
    "Plantations",
    "Power & Energy",
    "Services",
    "Stores Supplies",
    "Telecommunications",
    "Trading",
]

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = BASE_DIR / "logs" / "cse_research.log"

# Create logs directory
(BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)
