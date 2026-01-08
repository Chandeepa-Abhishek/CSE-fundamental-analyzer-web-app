"""
CSE API Client - Interfaces with CSE website API endpoints
"""
import requests
import json
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import sys
sys.path.append('..')
from config.settings import (
    CSE_BASE_URL, CSE_API_BASE, ENDPOINTS, 
    DEFAULT_HEADERS, REQUEST_TIMEOUT, REQUEST_DELAY, MAX_RETRIES
)

logger = logging.getLogger(__name__)


class CSEAPIClient:
    """Client for interacting with CSE website API endpoints"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Implement rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, method: str = "GET", 
                      params: Dict = None, data: Dict = None,
                      retries: int = MAX_RETRIES) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        self._rate_limit()
        
        for attempt in range(retries):
            try:
                if method == "GET":
                    response = self.session.get(
                        url, params=params, timeout=REQUEST_TIMEOUT
                    )
                else:
                    response = self.session.post(
                        url, json=data, timeout=REQUEST_TIMEOUT
                    )
                
                response.raise_for_status()
                
                # Try to parse JSON
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return {"content": response.text}
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All retries failed for {url}")
                    return None
        
        return None
    
    def get_all_listed_companies(self) -> List[Dict]:
        """
        Fetch all listed companies from CSE
        Returns list of company symbols and basic info
        """
        url = f"{CSE_BASE_URL}/api/listingsAll"
        
        result = self._make_request(url)
        if result and isinstance(result, list):
            logger.info(f"Retrieved {len(result)} listed companies")
            return result
        
        # Alternative endpoint
        url = f"{CSE_BASE_URL}/api/companyList"
        result = self._make_request(url)
        if result:
            return result if isinstance(result, list) else result.get('data', [])
        
        return []
    
    def get_trade_summary(self, date: str = None) -> List[Dict]:
        """
        Get trade summary for all stocks
        Contains: Symbol, Price, Change, Volume, Turnover, etc.
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        url = f"{CSE_BASE_URL}/api/tradeSummary"
        params = {"date": date}
        
        result = self._make_request(url, params=params)
        if result:
            return result if isinstance(result, list) else result.get('reqTradeSummery', [])
        
        return []
    
    def get_price_list(self) -> List[Dict]:
        """
        Get current price list for all securities
        Contains: Symbol, Last Traded Price, High, Low, Volume, etc.
        """
        url = f"{CSE_BASE_URL}/api/priceList"
        
        result = self._make_request(url)
        if result:
            return result if isinstance(result, list) else result.get('data', [])
        
        return []
    
    def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """
        Get detailed company profile including financials
        Contains: EPS, PE, NAV, Dividend info, etc.
        """
        url = f"{CSE_BASE_URL}/api/companyInfoSummery"
        params = {"symbol": symbol}
        
        result = self._make_request(url, params=params)
        return result
    
    def get_company_financials(self, symbol: str) -> Optional[Dict]:
        """
        Get financial statements data for a company
        """
        url = f"{CSE_BASE_URL}/api/companyFinancials"
        params = {"symbol": symbol}
        
        result = self._make_request(url, params=params)
        return result
    
    def get_historical_data(self, symbol: str, 
                           start_date: str = None, 
                           end_date: str = None) -> List[Dict]:
        """
        Get historical price data for a symbol
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        url = f"{CSE_BASE_URL}/api/historicalData"
        params = {
            "symbol": symbol,
            "startDate": start_date,
            "endDate": end_date
        }
        
        result = self._make_request(url, params=params)
        if result:
            return result if isinstance(result, list) else result.get('data', [])
        
        return []
    
    def get_market_indices(self) -> Dict:
        """
        Get market indices (ASPI, S&P SL20)
        """
        url = f"{CSE_BASE_URL}/api/indices"
        
        result = self._make_request(url)
        return result if result else {}
    
    def get_announcements(self, company: str = None, 
                          days: int = 30) -> List[Dict]:
        """
        Get corporate announcements
        """
        url = f"{CSE_BASE_URL}/api/announcements"
        params = {"days": days}
        if company:
            params["company"] = company
        
        result = self._make_request(url, params=params)
        if result:
            return result if isinstance(result, list) else result.get('data', [])
        
        return []
    
    def get_sector_summary(self) -> List[Dict]:
        """
        Get summary by industry sector
        """
        url = f"{CSE_BASE_URL}/api/sectorSummary"
        
        result = self._make_request(url)
        if result:
            return result if isinstance(result, list) else result.get('data', [])
        
        return []


class CSEDataFetcher:
    """High-level data fetcher that combines API calls"""
    
    def __init__(self):
        self.client = CSEAPIClient()
    
    def fetch_all_companies_with_details(self, 
                                         progress_callback=None) -> List[Dict]:
        """
        Fetch all companies with their detailed financial information
        """
        companies = []
        
        # Get list of all companies
        company_list = self.client.get_all_listed_companies()
        
        if not company_list:
            logger.warning("Could not fetch company list, trying trade summary")
            trade_summary = self.client.get_trade_summary()
            company_list = [{"symbol": item.get("symbol")} for item in trade_summary]
        
        total = len(company_list)
        logger.info(f"Fetching details for {total} companies...")
        
        for i, company in enumerate(company_list):
            symbol = company.get("symbol", company.get("Symbol", ""))
            if not symbol:
                continue
            
            if progress_callback:
                progress_callback(i + 1, total, symbol)
            
            # Fetch detailed profile
            profile = self.client.get_company_profile(symbol)
            
            if profile:
                company_data = {
                    "symbol": symbol,
                    "name": company.get("name", company.get("Name", "")),
                    **self._extract_financial_metrics(profile)
                }
                companies.append(company_data)
            
            # Small delay to be respectful
            time.sleep(0.5)
        
        return companies
    
    def _extract_financial_metrics(self, profile: Dict) -> Dict:
        """Extract key financial metrics from company profile"""
        metrics = {
            "last_traded_price": None,
            "change_percent": None,
            "volume": None,
            "market_cap": None,
            "shares_outstanding": None,
            "eps": None,
            "pe_ratio": None,
            "pb_ratio": None,
            "nav": None,  # Net Asset Value (Book Value per share)
            "dividend_yield": None,
            "dividend_per_share": None,
            "roe": None,
            "debt_equity": None,
            "sector": None,
            "52_week_high": None,
            "52_week_low": None,
        }
        
        if not profile:
            return metrics
        
        # Map API response fields to our metrics
        # Note: Actual field names may vary - these are common patterns
        field_mappings = {
            "last_traded_price": ["lastTradedPrice", "ltp", "price", "closingPrice"],
            "change_percent": ["changePercent", "change", "priceChange"],
            "volume": ["volume", "shareVolume", "tradedVolume"],
            "market_cap": ["marketCap", "marketCapitalization"],
            "shares_outstanding": ["sharesOutstanding", "issuedShares", "totalShares"],
            "eps": ["eps", "earningsPerShare", "EPS"],
            "pe_ratio": ["peRatio", "pe", "priceEarnings", "PER"],
            "pb_ratio": ["pbRatio", "priceToBook", "PBR"],
            "nav": ["nav", "netAssetValue", "bookValue", "NAV"],
            "dividend_yield": ["dividendYield", "divYield", "yield"],
            "dividend_per_share": ["dps", "dividendPerShare", "dividend"],
            "roe": ["roe", "returnOnEquity", "ROE"],
            "debt_equity": ["debtEquity", "debtToEquity", "DE"],
            "sector": ["sector", "industry", "sectorName"],
            "52_week_high": ["high52Week", "yearHigh", "52WeekHigh"],
            "52_week_low": ["low52Week", "yearLow", "52WeekLow"],
        }
        
        for metric, possible_fields in field_mappings.items():
            for field in possible_fields:
                if field in profile and profile[field] is not None:
                    try:
                        value = profile[field]
                        # Convert to float if numeric
                        if metric not in ["sector"]:
                            if isinstance(value, str):
                                value = value.replace(",", "").replace("%", "")
                                value = float(value) if value else None
                        metrics[metric] = value
                        break
                    except (ValueError, TypeError):
                        continue
        
        return metrics
