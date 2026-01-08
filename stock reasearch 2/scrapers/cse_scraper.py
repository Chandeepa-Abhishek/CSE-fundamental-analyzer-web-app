"""
CSE Web Scraper - Scrapes data directly from CSE website pages
Used as fallback when API endpoints don't work
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import sys
sys.path.append('..')
from config.settings import (
    CSE_BASE_URL, DEFAULT_HEADERS, REQUEST_TIMEOUT, 
    RAW_DATA_DIR, PROCESSED_DATA_DIR
)

logger = logging.getLogger(__name__)


class CSEScraper:
    """
    Web scraper for CSE website
    Uses Selenium for JavaScript-rendered pages
    """
    
    def __init__(self, headless: bool = True):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.driver = None
        self.headless = headless
    
    def _init_driver(self):
        """Initialize Selenium WebDriver"""
        if self.driver is None:
            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument(f"user-agent={DEFAULT_HEADERS['User-Agent']}")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.implicitly_wait(10)
        
        return self.driver
    
    def close(self):
        """Close the browser driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def scrape_listed_companies(self) -> List[Dict]:
        """
        Scrape the list of all listed companies from CSE website
        """
        companies = []
        driver = self._init_driver()
        
        try:
            url = f"{CSE_BASE_URL}/pages/listed-company/listed-company.component.html"
            driver.get(url)
            
            # Wait for the table to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table, .company-list"))
            )
            time.sleep(2)  # Additional wait for data
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find company rows (adjust selectors based on actual page structure)
            rows = soup.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    company = {
                        "symbol": cells[0].get_text(strip=True),
                        "name": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                        "sector": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                    }
                    if company["symbol"] and not company["symbol"].startswith(("Symbol", "#")):
                        companies.append(company)
            
            logger.info(f"Scraped {len(companies)} companies from listed companies page")
            
        except Exception as e:
            logger.error(f"Error scraping listed companies: {e}")
        
        return companies
    
    def scrape_trade_summary(self) -> List[Dict]:
        """
        Scrape daily trade summary with prices and volumes
        """
        trade_data = []
        driver = self._init_driver()
        
        try:
            url = f"{CSE_BASE_URL}/pages/trade-summary/trade-summary.component.html"
            driver.get(url)
            
            # Wait for data to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table, .trade-summary"))
            )
            time.sleep(3)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find the trade summary table
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        try:
                            trade = {
                                "symbol": cells[0].get_text(strip=True),
                                "last_traded_price": self._parse_number(cells[1].get_text(strip=True)),
                                "change": self._parse_number(cells[2].get_text(strip=True)),
                                "change_percent": self._parse_number(cells[3].get_text(strip=True)),
                                "volume": self._parse_number(cells[4].get_text(strip=True)),
                                "turnover": self._parse_number(cells[5].get_text(strip=True)) if len(cells) > 5 else None,
                            }
                            if trade["symbol"]:
                                trade_data.append(trade)
                        except (IndexError, ValueError):
                            continue
            
            logger.info(f"Scraped trade data for {len(trade_data)} stocks")
            
        except Exception as e:
            logger.error(f"Error scraping trade summary: {e}")
        
        return trade_data
    
    def scrape_company_profile(self, symbol: str) -> Optional[Dict]:
        """
        Scrape detailed company profile page
        """
        driver = self._init_driver()
        
        try:
            url = f"{CSE_BASE_URL}/pages/company-profile/company-profile.component.html?symbol={symbol}"
            driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            profile = {
                "symbol": symbol,
                "name": "",
                "sector": "",
                "last_traded_price": None,
                "change_percent": None,
                "volume": None,
                "52_week_high": None,
                "52_week_low": None,
                "market_cap": None,
                "shares_outstanding": None,
                "eps": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "nav": None,
                "dividend_yield": None,
                "roe": None,
            }
            
            # Extract data from various elements
            # Company name
            name_elem = soup.find(['h1', 'h2', 'h3'], class_=re.compile('company|name|title', re.I))
            if name_elem:
                profile["name"] = name_elem.get_text(strip=True)
            
            # Look for data in tables or definition lists
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        # Map labels to our fields
                        self._map_profile_field(profile, label, value)
            
            # Also check for definition lists and divs
            for dl in soup.find_all('dl'):
                dts = dl.find_all('dt')
                dds = dl.find_all('dd')
                for dt, dd in zip(dts, dds):
                    label = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    self._map_profile_field(profile, label, value)
            
            return profile
            
        except Exception as e:
            logger.error(f"Error scraping company profile for {symbol}: {e}")
            return None
    
    def _map_profile_field(self, profile: Dict, label: str, value: str):
        """Map scraped labels to profile fields"""
        label_mappings = {
            ("eps", "earnings per share"): "eps",
            ("pe", "p/e", "price earnings", "price/earnings"): "pe_ratio",
            ("pb", "p/b", "price book", "price/book"): "pb_ratio",
            ("nav", "net asset", "book value"): "nav",
            ("dividend yield", "div yield"): "dividend_yield",
            ("roe", "return on equity"): "roe",
            ("market cap", "capitalization"): "market_cap",
            ("shares outstanding", "issued shares", "total shares"): "shares_outstanding",
            ("52 week high", "year high", "52w high"): "52_week_high",
            ("52 week low", "year low", "52w low"): "52_week_low",
            ("sector", "industry"): "sector",
            ("volume", "traded volume"): "volume",
        }
        
        for keywords, field in label_mappings.items():
            if any(kw in label for kw in keywords):
                profile[field] = self._parse_number(value) if field not in ["sector", "name"] else value
                break
    
    def _parse_number(self, value: str) -> Optional[float]:
        """Parse number from string, handling commas and percentages"""
        if not value:
            return None
        try:
            # Remove commas, percentage signs, and other non-numeric chars except decimal and minus
            cleaned = re.sub(r'[^\d.\-]', '', value.replace(',', ''))
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
    
    def scrape_all_companies_data(self, progress_callback=None) -> pd.DataFrame:
        """
        Scrape data for all companies and return as DataFrame
        """
        # First get list of all companies
        companies = self.scrape_listed_companies()
        
        if not companies:
            # Fallback: try trade summary to get symbols
            trade_data = self.scrape_trade_summary()
            companies = [{"symbol": t["symbol"]} for t in trade_data]
        
        all_data = []
        total = len(companies)
        
        for i, company in enumerate(companies):
            symbol = company.get("symbol", "")
            if not symbol:
                continue
            
            if progress_callback:
                progress_callback(i + 1, total, symbol)
            
            logger.info(f"Scraping {symbol} ({i+1}/{total})")
            
            profile = self.scrape_company_profile(symbol)
            if profile:
                all_data.append(profile)
            
            # Be respectful with rate limiting
            time.sleep(1.5)
        
        self.close()
        
        df = pd.DataFrame(all_data)
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(RAW_DATA_DIR / f"cse_all_companies_{timestamp}.csv", index=False)
        df.to_json(RAW_DATA_DIR / f"cse_all_companies_{timestamp}.json", orient="records", indent=2)
        
        return df
    
    def scrape_market_summary(self) -> Dict:
        """
        Scrape market indices and summary data
        """
        driver = self._init_driver()
        
        try:
            driver.get(CSE_BASE_URL)
            time.sleep(3)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            market_data = {
                "aspi": None,
                "aspi_change": None,
                "sp_sl20": None,
                "sp_sl20_change": None,
                "total_turnover": None,
                "total_volume": None,
                "date": datetime.now().strftime("%Y-%m-%d"),
            }
            
            # Extract index values (adjust selectors based on actual page)
            text_content = soup.get_text()
            
            # Look for ASPI value
            aspi_match = re.search(r'ASPI[:\s]*([\d,]+\.?\d*)', text_content)
            if aspi_match:
                market_data["aspi"] = self._parse_number(aspi_match.group(1))
            
            return market_data
            
        except Exception as e:
            logger.error(f"Error scraping market summary: {e}")
            return {}


class CSEDataCollector:
    """
    High-level data collection that tries API first, falls back to scraping
    """
    
    def __init__(self):
        from .api_client import CSEAPIClient, CSEDataFetcher
        self.api_client = CSEAPIClient()
        self.api_fetcher = CSEDataFetcher()
        self.scraper = None  # Lazy initialization
    
    def collect_all_data(self, use_scraper_fallback: bool = True,
                         progress_callback=None) -> pd.DataFrame:
        """
        Collect comprehensive data for all companies
        """
        logger.info("Attempting to fetch data via API...")
        
        # Try API first
        companies = self.api_fetcher.fetch_all_companies_with_details(progress_callback)
        
        if companies:
            df = pd.DataFrame(companies)
            logger.info(f"Successfully fetched {len(companies)} companies via API")
        elif use_scraper_fallback:
            logger.info("API fetch failed, falling back to web scraping...")
            self.scraper = CSEScraper(headless=True)
            df = self.scraper.scrape_all_companies_data(progress_callback)
        else:
            df = pd.DataFrame()
        
        if not df.empty:
            # Save data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(PROCESSED_DATA_DIR / f"cse_companies_{timestamp}.csv", index=False)
            df.to_pickle(PROCESSED_DATA_DIR / f"cse_companies_{timestamp}.pkl")
            
            logger.info(f"Data saved to {PROCESSED_DATA_DIR}")
        
        return df
    
    def get_latest_data(self) -> pd.DataFrame:
        """
        Get the most recently saved data file
        """
        import glob
        
        # Look for pickle files first (faster to load)
        pkl_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.pkl"), reverse=True)
        if pkl_files:
            return pd.read_pickle(pkl_files[0])
        
        # Fall back to CSV
        csv_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.csv"), reverse=True)
        if csv_files:
            return pd.read_csv(csv_files[0])
        
        return pd.DataFrame()
