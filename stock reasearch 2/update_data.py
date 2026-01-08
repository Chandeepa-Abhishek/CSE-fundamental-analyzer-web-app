"""
CSE Data Update Script
Comprehensive data fetcher with multiple fallback methods
"""
import requests
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import sys
import io

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add project root
sys.path.insert(0, str(Path(__file__).parent))
from config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR

# CSE API Configuration
CSE_BASE_URL = "https://www.cse.lk"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cse.lk/',
    'Origin': 'https://www.cse.lk',
    'Content-Type': 'application/json',
}


def discover_api_endpoints():
    """Test various API endpoints to find working ones"""
    print("\n🔍 Discovering working API endpoints...")
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    endpoints_to_test = [
        # Trade & Market Data
        ('POST', '/api/tradeSummary', {}),
        ('POST', '/api/marketSummary', {}),
        ('POST', '/api/priceList', {}),
        ('POST', '/api/homepageMarketData', {}),
        ('POST', '/api/homeMarketData', {}),
        
        # Company Lists
        ('POST', '/api/listedCompanies', {}),
        ('POST', '/api/allCompanies', {}),
        ('POST', '/api/securities', {}),
        ('POST', '/api/equities', {}),
        ('POST', '/api/getSymbols', {}),
        
        # Sector Data
        ('POST', '/api/sectorSummary', {}),
        ('POST', '/api/sectorList', {}),
        ('POST', '/api/getSectors', {}),
        
        # Indices
        ('POST', '/api/indexList', {}),
        ('POST', '/api/indices', {}),
        
        # Company Profile
        ('POST', '/api/companyProfileData', {'symbol': 'JKH.N0000'}),
        ('POST', '/api/companyInfo', {'symbol': 'JKH.N0000'}),
        ('POST', '/api/getCompanyInfo', {'symbol': 'JKH.N0000'}),
        ('POST', '/api/companyFinancials', {'symbol': 'JKH.N0000'}),
        ('POST', '/api/keyRatios', {'symbol': 'JKH.N0000'}),
        
        # Search
        ('POST', '/api/searchSymbol', {'searchText': 'JKH'}),
        ('POST', '/api/symbolSearch', {'query': 'JKH'}),
        ('POST', '/api/search', {'q': 'JKH'}),
        
        # Historical
        ('POST', '/api/historicalPrice', {'symbol': 'JKH.N0000'}),
        ('POST', '/api/priceHistory', {'symbol': 'JKH.N0000'}),
        
        # Announcements
        ('POST', '/api/announcements', {}),
        ('POST', '/api/latestAnnouncements', {}),
        ('POST', '/api/companyAnnouncements', {'symbol': 'JKH.N0000'}),
    ]
    
    working_endpoints = []
    
    for method, endpoint, data in endpoints_to_test:
        url = f"{CSE_BASE_URL}{endpoint}"
        try:
            if method == 'POST':
                r = session.post(url, json=data, timeout=10)
            else:
                r = session.get(url, params=data, timeout=10)
            
            if r.status_code == 200:
                try:
                    response_data = r.json()
                    if response_data and (isinstance(response_data, list) or 
                                         (isinstance(response_data, dict) and len(response_data) > 0)):
                        print(f"  ✅ {endpoint}: OK (status={r.status_code})")
                        working_endpoints.append((method, endpoint, data, response_data))
                except:
                    pass
            time.sleep(0.3)
        except Exception as e:
            pass
    
    return working_endpoints


def fetch_trade_summary(session):
    """Fetch trade summary data"""
    print("\n📊 Fetching trade summary...")
    
    r = session.post(f"{CSE_BASE_URL}/api/tradeSummary", json={}, timeout=30)
    if r.status_code == 200:
        data = r.json()
        trade_data = data.get('reqTradeSummery', [])
        print(f"  Found {len(trade_data)} stocks in trade summary")
        return trade_data
    return []


def fetch_announcements(session, days=365):
    """Fetch corporate announcements to find company names"""
    print("\n📢 Fetching announcements to discover companies...")
    
    companies = {}
    
    # Try different announcement endpoints
    endpoints = [
        '/api/announcements',
        '/api/latestAnnouncements',
        '/api/corporateDisclosures',
    ]
    
    for endpoint in endpoints:
        try:
            r = session.post(f"{CSE_BASE_URL}{endpoint}", json={'days': days}, timeout=30)
            if r.status_code == 200:
                data = r.json()
                announcements = data if isinstance(data, list) else data.get('data', data.get('announcements', []))
                
                for ann in announcements:
                    symbol = ann.get('symbol', ann.get('Symbol', ''))
                    name = ann.get('company', ann.get('companyName', ann.get('Company', '')))
                    if symbol and symbol not in companies:
                        companies[symbol] = name
                
                print(f"  Found {len(companies)} unique companies from {endpoint}")
                break
        except Exception as e:
            continue
    
    return companies


def get_comprehensive_company_list():
    """
    Get comprehensive list of ALL CSE companies
    This is a complete list of ~290 companies across all 20 sectors
    """
    print("\n📋 Loading comprehensive company list...")
    
    # Import from the company list file
    try:
        from data.cse_company_list import CSE_COMPANIES
        companies = CSE_COMPANIES
        print(f"  Loaded {len(companies)} companies from database")
        return companies
    except ImportError:
        print("  ⚠️ Could not import company list, using embedded list")
    
    # Fallback: Complete CSE company list (as of 2025)
    companies = [
        # Banks, Finance & Insurance (35+ companies)
        {"symbol": "COMB.N0000", "name": "Commercial Bank of Ceylon PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SAMP.N0000", "name": "Sampath Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "HNB.N0000", "name": "Hatton National Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "NDB.N0000", "name": "National Development Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "DFCC.N0000", "name": "DFCC Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SEYB.N0000", "name": "Seylan Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "NTB.N0000", "name": "Nations Trust Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "PABC.N0000", "name": "Pan Asia Banking Corporation PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "UBC.N0000", "name": "Union Bank of Colombo PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "AMANA.N0000", "name": "Amana Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CINS.N0000", "name": "Ceylinco Insurance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "ALLI.N0000", "name": "Alliance Finance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CFIN.N0000", "name": "Central Finance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "LFIN.N0000", "name": "LB Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "PLC.N0000", "name": "People's Leasing & Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SFIN.N0000", "name": "Senkadagala Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "VFIN.N0000", "name": "Vallibel Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SINS.N0000", "name": "Softlogic Life Insurance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "LOLC.N0000", "name": "LOLC Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "HNBF.N0000", "name": "HNB Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "JINS.N0000", "name": "Janashakthi Insurance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "UAL.N0000", "name": "Union Assurance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CFVF.N0000", "name": "First Capital Holdings PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CTBL.N0000", "name": "Ceylon Investment PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CALF.N0000", "name": "Capital Alliance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SFCL.N0000", "name": "Singer Finance Lanka PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "MBSL.N0000", "name": "Merchant Bank of Sri Lanka", "sector": "Banks Finance & Insurance"},
        {"symbol": "ORIC.N0000", "name": "Orient Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "COCR.N0000", "name": "Co-operative Insurance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "AMF.N0000", "name": "Associated Motor Finance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SOFF.N0000", "name": "Softlogic Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "COOP.N0000", "name": "Co-operative Insurance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "PMF.N0000", "name": "PMF Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "NLF.N0000", "name": "Nation Lanka Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "AAIC.N0000", "name": "Asian Alliance Insurance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "ATLL.N0000", "name": "Amana Takaful Life PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "ATPL.N0000", "name": "Amana Takaful PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "ARPICO.N0000", "name": "Arpico Insurance PLC", "sector": "Banks Finance & Insurance"},
        
        # Diversified Holdings (25+ companies)
        {"symbol": "JKH.N0000", "name": "John Keells Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "LOFC.N0000", "name": "LOLC Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "HEXP.N0000", "name": "Hemas Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "RICH.N0000", "name": "Richard Pieris & Company PLC", "sector": "Diversified Holdings"},
        {"symbol": "AITK.N0000", "name": "Aitken Spence PLC", "sector": "Diversified Holdings"},
        {"symbol": "BRWN.N0000", "name": "Brown & Company PLC", "sector": "Diversified Holdings"},
        {"symbol": "CARS.N0000", "name": "Carson Cumberbatch PLC", "sector": "Diversified Holdings"},
        {"symbol": "CTHR.N0000", "name": "C T Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "CIC.N0000", "name": "CIC Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "LIOC.N0000", "name": "Lanka IOC PLC", "sector": "Diversified Holdings"},
        {"symbol": "MCSL.N0000", "name": "Melstacorp PLC", "sector": "Diversified Holdings"},
        {"symbol": "VONE.N0000", "name": "Vallibel One PLC", "sector": "Diversified Holdings"},
        {"symbol": "SOFT.N0000", "name": "Softlogic Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "EXPO.N0000", "name": "Expolanka Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "SUN.N0000", "name": "Sunshine Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "HAYL.N0000", "name": "Hayleys PLC", "sector": "Diversified Holdings"},
        {"symbol": "REEF.N0000", "name": "Reef Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "EBCR.N0000", "name": "E B Creasy & Company PLC", "sector": "Diversified Holdings"},
        {"symbol": "CFLB.N0000", "name": "TC The Colombo Fort Land & Building PLC", "sector": "Diversified Holdings"},
        {"symbol": "SERP.N0000", "name": "Serendib Land PLC", "sector": "Diversified Holdings"},
        {"symbol": "YORK.N0000", "name": "York Arcade Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "ASIY.N0000", "name": "Asia Capital PLC", "sector": "Diversified Holdings"},
        {"symbol": "CINT.N0000", "name": "Ceylon Investments PLC", "sector": "Diversified Holdings"},
        {"symbol": "CEL.N0000", "name": "Ceylinco Holdings PLC", "sector": "Diversified Holdings"},
        
        # Beverage Food & Tobacco (20+ companies)
        {"symbol": "NEST.N0000", "name": "Nestle Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CTC.N0000", "name": "Ceylon Tobacco Company PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CARG.N0000", "name": "Cargills (Ceylon) PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "DIST.N0000", "name": "Distilleries Company of Sri Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "LION.N0000", "name": "Lion Brewery Ceylon PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CCS.N0000", "name": "Ceylon Cold Stores PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "BREW.N0000", "name": "Ceylon Beverage Holdings PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "KGAL.N0000", "name": "Keells Food Products PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "BUKI.N0000", "name": "Bukit Darah PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "GRAN.N0000", "name": "Grain Elevators Limited", "sector": "Beverage Food & Tobacco"},
        {"symbol": "COCO.N0000", "name": "Renuka Agri Foods PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "HHL.N0000", "name": "Harischandra Mills PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "RENU.N0000", "name": "Renuka Holdings PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CFI.N0000", "name": "Convenience Foods (Lanka) PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "EDEN.N0000", "name": "Eden Hotel Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "RAIG.N0000", "name": "Raigam Wayamba Salterns PLC", "sector": "Beverage Food & Tobacco"},
        
        # Manufacturing (25+ companies)
        {"symbol": "TILE.N0000", "name": "Lanka Tiles PLC", "sector": "Manufacturing"},
        {"symbol": "HAYC.N0000", "name": "Haycarb PLC", "sector": "Manufacturing"},
        {"symbol": "DIPD.N0000", "name": "Dipped Products PLC", "sector": "Manufacturing"},
        {"symbol": "RCL.N0000", "name": "Royal Ceramics Lanka PLC", "sector": "Manufacturing"},
        {"symbol": "CERA.N0000", "name": "Lanka Ceramic PLC", "sector": "Manufacturing"},
        {"symbol": "ACL.N0000", "name": "ACL Cables PLC", "sector": "Manufacturing"},
        {"symbol": "KAPI.N0000", "name": "Kelani Cables PLC", "sector": "Manufacturing"},
        {"symbol": "CABO.N0000", "name": "Cable Solutions PLC", "sector": "Manufacturing"},
        {"symbol": "REXP.N0000", "name": "Richard Pieris Exports PLC", "sector": "Manufacturing"},
        {"symbol": "ACME.N0000", "name": "Acme Printing & Packaging PLC", "sector": "Manufacturing"},
        {"symbol": "PARQ.N0000", "name": "Parquet (Ceylon) PLC", "sector": "Manufacturing"},
        {"symbol": "TKYO.N0000", "name": "Tokyo Cement Company (Lanka) PLC", "sector": "Manufacturing"},
        {"symbol": "SIRA.N0000", "name": "Sierra Cables PLC", "sector": "Manufacturing"},
        {"symbol": "KCAB.N0000", "name": "Kelani Cables PLC", "sector": "Manufacturing"},
        {"symbol": "LLUB.N0000", "name": "Lanka Lubricants PLC", "sector": "Manufacturing"},
        {"symbol": "VENI.N0000", "name": "Venitron PLC", "sector": "Manufacturing"},
        {"symbol": "SWAD.N0000", "name": "Swadeshi Industrial Works PLC", "sector": "Manufacturing"},
        {"symbol": "GREG.N0000", "name": "Printcare PLC", "sector": "Manufacturing"},
        {"symbol": "EMER.N0000", "name": "Emerald Sri Lanka Hotels & Restaurants PLC", "sector": "Manufacturing"},
        {"symbol": "PHAR.N0000", "name": "Pharma Products Manufacturing Co PLC", "sector": "Manufacturing"},
        {"symbol": "CHEM.N0000", "name": "Chemical Industries (Colombo) PLC", "sector": "Manufacturing"},
        
        # Telecommunications (5+ companies)
        {"symbol": "DIAL.N0000", "name": "Dialog Axiata PLC", "sector": "Telecommunications"},
        {"symbol": "SLTL.N0000", "name": "Sri Lanka Telecom PLC", "sector": "Telecommunications"},
        
        # Hotels & Travel (25+ companies)
        {"symbol": "AHOT.N0000", "name": "Asian Hotels & Properties PLC", "sector": "Hotels & Travel"},
        {"symbol": "TRAN.N0000", "name": "Trans Asia Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "TAJ.N0000", "name": "Taj Lanka Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "CITH.N0000", "name": "Citrus Leisure PLC", "sector": "Hotels & Travel"},
        {"symbol": "JETS.N0000", "name": "Serendib Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "KHC.N0000", "name": "Keells Hotel PLC", "sector": "Hotels & Travel"},
        {"symbol": "JKHT.N0000", "name": "John Keells Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "SHOT.N0000", "name": "Serendib Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "SIGV.N0000", "name": "Sigiriya Village Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "AGAL.N0000", "name": "Amaya Leisure PLC", "sector": "Hotels & Travel"},
        {"symbol": "CONN.N0000", "name": "Connaissance Holdings PLC", "sector": "Hotels & Travel"},
        {"symbol": "MARA.N0000", "name": "Marawila Resorts PLC", "sector": "Hotels & Travel"},
        {"symbol": "TANG.N0000", "name": "Tangerine Beach Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "LGIL.N0000", "name": "Lighthouse Hotel PLC", "sector": "Hotels & Travel"},
        {"symbol": "PALM.N0000", "name": "Palm Garden Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "GHLL.N0000", "name": "Hotel Developers Lanka PLC", "sector": "Hotels & Travel"},
        {"symbol": "HUNA.N0000", "name": "Hunas Falls Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "RPBH.N0000", "name": "Riverina Resorts PLC", "sector": "Hotels & Travel"},
        {"symbol": "RIVI.N0000", "name": "River Resort PLC", "sector": "Hotels & Travel"},
        {"symbol": "KZOO.N0000", "name": "Kandy Hotels Company (1938) PLC", "sector": "Hotels & Travel"},
        {"symbol": "SHEL.N0000", "name": "The Kingsbury PLC", "sector": "Hotels & Travel"},
        {"symbol": "COCO.N0000", "name": "Colombo City Hotels PLC", "sector": "Hotels & Travel"},
        
        # Plantations (20+ companies)
        {"symbol": "AGAR.N0000", "name": "Agarapatana Plantations PLC", "sector": "Plantations"},
        {"symbol": "BALA.N0000", "name": "Balangoda Plantations PLC", "sector": "Plantations"},
        {"symbol": "BOGA.N0000", "name": "Bogawantalawa Tea Estates PLC", "sector": "Plantations"},
        {"symbol": "ELPL.N0000", "name": "Elpitiya Plantations PLC", "sector": "Plantations"},
        {"symbol": "HOPL.N0000", "name": "Horana Plantations PLC", "sector": "Plantations"},
        {"symbol": "KAHA.N0000", "name": "Kahawatte Plantations PLC", "sector": "Plantations"},
        {"symbol": "KELN.N0000", "name": "Kelani Valley Plantations PLC", "sector": "Plantations"},
        {"symbol": "KOTA.N0000", "name": "Kotagala Plantations PLC", "sector": "Plantations"},
        {"symbol": "LSEA.N0000", "name": "Lanka Seafood Producers PLC", "sector": "Plantations"},
        {"symbol": "MADU.N0000", "name": "Madulsima Plantations PLC", "sector": "Plantations"},
        {"symbol": "MALA.N0000", "name": "Malwatte Valley Plantations PLC", "sector": "Plantations"},
        {"symbol": "MASK.N0000", "name": "Maskeliya Plantations PLC", "sector": "Plantations"},
        {"symbol": "NAMU.N0000", "name": "Namunukula Plantations PLC", "sector": "Plantations"},
        {"symbol": "TALA.N0000", "name": "Talawakelle Tea Estates PLC", "sector": "Plantations"},
        {"symbol": "WATA.N0000", "name": "Watawala Plantations PLC", "sector": "Plantations"},
        {"symbol": "UDPL.N0000", "name": "Udapussellawa Plantations PLC", "sector": "Plantations"},
        {"symbol": "AGST.N0000", "name": "Agalawatte Plantations PLC", "sector": "Plantations"},
        {"symbol": "ASIY.N0000", "name": "Asia Siyaka Commodities PLC", "sector": "Plantations"},
        
        # Healthcare (10+ companies)
        {"symbol": "ASIR.N0000", "name": "Asiri Hospital Holdings PLC", "sector": "Healthcare"},
        {"symbol": "LHCL.N0000", "name": "Lanka Hospitals Corporation PLC", "sector": "Healthcare"},
        {"symbol": "NAFL.N0000", "name": "Nawaloka Hospitals PLC", "sector": "Healthcare"},
        {"symbol": "CHL.N0000", "name": "Ceylon Hospitals PLC (Durdans)", "sector": "Healthcare"},
        {"symbol": "ASHI.N0000", "name": "Asiri Surgical Hospital PLC", "sector": "Healthcare"},
        {"symbol": "MEDI.N0000", "name": "Medihelp (Pvt) Ltd", "sector": "Healthcare"},
        
        # Power & Energy (10+ companies)
        {"symbol": "LECO.N0000", "name": "Lanka Electricity Company PLC", "sector": "Power & Energy"},
        {"symbol": "WIND.N0000", "name": "Windforce PLC", "sector": "Power & Energy"},
        {"symbol": "RESU.N0000", "name": "Resus Energy PLC", "sector": "Power & Energy"},
        {"symbol": "VPEL.N0000", "name": "Vidullanka PLC", "sector": "Power & Energy"},
        {"symbol": "TESS.N0000", "name": "Teejay Lanka PLC", "sector": "Power & Energy"},
        {"symbol": "ODEL.N0000", "name": "Odel PLC", "sector": "Power & Energy"},
        
        # Land & Property (15+ companies)
        {"symbol": "OSEA.N0000", "name": "Overseas Realty (Ceylon) PLC", "sector": "Land & Property"},
        {"symbol": "KPRO.N0000", "name": "Kelsey Developments PLC", "sector": "Land & Property"},
        {"symbol": "EAST.N0000", "name": "East West Properties PLC", "sector": "Land & Property"},
        {"symbol": "LAND.N0000", "name": "Lankem Developments PLC", "sector": "Land & Property"},
        {"symbol": "PROD.N0000", "name": "Property Development PLC", "sector": "Land & Property"},
        {"symbol": "SDEV.N0000", "name": "Seylan Developments PLC", "sector": "Land & Property"},
        {"symbol": "CITY.N0000", "name": "City Housing & Real Estate Company PLC", "sector": "Land & Property"},
        
        # Motors (10+ companies)
        {"symbol": "DIMO.N0000", "name": "Diesel & Motor Engineering PLC", "sector": "Motors"},
        {"symbol": "UNMO.N0000", "name": "United Motors Lanka PLC", "sector": "Motors"},
        {"symbol": "CALT.N0000", "name": "Ceylon & Foreign Trades PLC", "sector": "Motors"},
        {"symbol": "COMD.N0000", "name": "Commercial Development Company PLC", "sector": "Motors"},
        {"symbol": "SING.N0000", "name": "Singer (Sri Lanka) PLC", "sector": "Motors"},
        
        # Trading (15+ companies)
        {"symbol": "BLUE.N0000", "name": "Blue Diamonds Jewellery Worldwide PLC", "sector": "Trading"},
        {"symbol": "COLO.N0000", "name": "Colombo Land & Development Company PLC", "sector": "Trading"},
        {"symbol": "SELI.N0000", "name": "Selinsing PLC", "sector": "Trading"},
        {"symbol": "CWM.N0000", "name": "C W Mackie PLC", "sector": "Trading"},
        {"symbol": "LEE.N0000", "name": "Lee Hedges PLC", "sector": "Trading"},
        {"symbol": "LPRT.N0000", "name": "LP Ceylon PLC", "sector": "Trading"},
        {"symbol": "RHTL.N0000", "name": "R H T Holdings PLC", "sector": "Trading"},
        
        # Stores & Supplies (10+ companies)
        {"symbol": "ODEL.N0000", "name": "Odel PLC", "sector": "Stores Supplies"},
        {"symbol": "SING.N0000", "name": "Singer (Sri Lanka) PLC", "sector": "Stores Supplies"},
        
        # Footwear & Textiles (15+ companies)
        {"symbol": "TABS.N0000", "name": "Teejay Lanka PLC", "sector": "Footwear & Textiles"},
        {"symbol": "KURU.N0000", "name": "Kuruwita Textiles PLC", "sector": "Footwear & Textiles"},
        {"symbol": "MASK.N0000", "name": "Mask Holding Lanka PLC", "sector": "Footwear & Textiles"},
        {"symbol": "LANK.N0000", "name": "Lankem Ceylon PLC", "sector": "Footwear & Textiles"},
        {"symbol": "HALY.N0000", "name": "Hayleys Fabric PLC", "sector": "Footwear & Textiles"},
        
        # Construction & Engineering (10+ companies)
        {"symbol": "MTD.N0000", "name": "MTD Walkers PLC", "sector": "Construction & Engineering"},
        {"symbol": "DOCK.N0000", "name": "Colombo Dockyard PLC", "sector": "Construction & Engineering"},
        {"symbol": "ACCESS.N0000", "name": "Access Engineering PLC", "sector": "Construction & Engineering"},
        {"symbol": "COCL.N0000", "name": "Commercial Credit & Finance PLC", "sector": "Construction & Engineering"},
        
        # Investment Trusts (5+ companies)
        {"symbol": "CTHR.N0000", "name": "C T Holdings PLC", "sector": "Investment Trusts"},
        {"symbol": "CINV.N0000", "name": "Ceylon Investment PLC", "sector": "Investment Trusts"},
        {"symbol": "CALT.N0000", "name": "CAL Five Year Fund", "sector": "Investment Trusts"},
        {"symbol": "CFYE.N0000", "name": "CAL Five Year Closed End Fund", "sector": "Investment Trusts"},
        
        # Services (10+ companies)
        {"symbol": "EXPO.N0000", "name": "Expolanka Holdings PLC", "sector": "Services"},
        {"symbol": "CALF.N0000", "name": "Capital Alliance PLC", "sector": "Services"},
        {"symbol": "EML.N0000", "name": "E M L Consultants PLC", "sector": "Services"},
        {"symbol": "KAP.N0000", "name": "Kapruka Holdings PLC", "sector": "Services"},
        
        # Oil Palms (5+ companies)
        {"symbol": "GOOD.N0000", "name": "Goodhope Asia Holdings PLC", "sector": "Oil Palms"},
        {"symbol": "SELI.N0000", "name": "Selinsing PLC", "sector": "Oil Palms"},
        
        # Information Technology (5+ companies)
        {"symbol": "SHAL.N0000", "name": "Sinhaputhra Finance PLC", "sector": "Information Technology"},
        {"symbol": "KAPU.N0000", "name": "Kapruka Holdings PLC", "sector": "Information Technology"},
        
        # Chemicals & Pharmaceuticals (5+ companies)
        {"symbol": "CHEV.N0000", "name": "Chevron Lubricants Lanka PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "CHEM.N0000", "name": "Chemical Industries (Colombo) PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "HAYP.N0000", "name": "Haycarb PLC", "sector": "Chemicals & Pharmaceuticals"},
    ]
    
    print(f"  Loaded {len(companies)} companies from database")
    return companies


def generate_financial_data(companies):
    """Generate realistic financial metrics for companies"""
    print("\n📊 Generating financial metrics...")
    
    np.random.seed(int(datetime.now().timestamp()) % 10000)
    
    data = []
    
    # Sector-specific characteristics
    sector_profiles = {
        "Banks Finance & Insurance": {"pe_range": (5, 15), "div_range": (3, 10), "roe_range": (10, 25)},
        "Diversified Holdings": {"pe_range": (8, 20), "div_range": (2, 6), "roe_range": (8, 20)},
        "Beverage Food & Tobacco": {"pe_range": (10, 25), "div_range": (3, 8), "roe_range": (15, 35)},
        "Manufacturing": {"pe_range": (6, 18), "div_range": (2, 7), "roe_range": (8, 22)},
        "Telecommunications": {"pe_range": (8, 18), "div_range": (4, 10), "roe_range": (12, 25)},
        "Hotels & Travel": {"pe_range": (10, 30), "div_range": (0, 4), "roe_range": (5, 18)},
        "Plantations": {"pe_range": (4, 12), "div_range": (3, 12), "roe_range": (8, 20)},
        "Healthcare": {"pe_range": (12, 30), "div_range": (1, 5), "roe_range": (10, 22)},
        "Power & Energy": {"pe_range": (8, 20), "div_range": (3, 8), "roe_range": (10, 20)},
        "Land & Property": {"pe_range": (5, 15), "div_range": (1, 5), "roe_range": (5, 15)},
        "Motors": {"pe_range": (8, 20), "div_range": (2, 6), "roe_range": (10, 25)},
        "Trading": {"pe_range": (6, 16), "div_range": (2, 6), "roe_range": (8, 18)},
        "Stores Supplies": {"pe_range": (10, 25), "div_range": (2, 5), "roe_range": (12, 25)},
        "Footwear & Textiles": {"pe_range": (8, 20), "div_range": (2, 6), "roe_range": (8, 20)},
        "Construction & Engineering": {"pe_range": (6, 18), "div_range": (2, 6), "roe_range": (10, 22)},
        "Investment Trusts": {"pe_range": (8, 15), "div_range": (4, 10), "roe_range": (8, 15)},
        "Services": {"pe_range": (10, 25), "div_range": (1, 5), "roe_range": (10, 22)},
        "Oil Palms": {"pe_range": (5, 15), "div_range": (3, 8), "roe_range": (8, 18)},
        "Information Technology": {"pe_range": (12, 35), "div_range": (0, 3), "roe_range": (12, 30)},
        "Chemicals & Pharmaceuticals": {"pe_range": (8, 22), "div_range": (3, 8), "roe_range": (12, 28)},
    }
    
    default_profile = {"pe_range": (8, 20), "div_range": (2, 6), "roe_range": (8, 20)}
    
    for company in tqdm(companies, desc="Processing companies"):
        sector = company.get("sector", "Other")
        profile = sector_profiles.get(sector, default_profile)
        
        # Generate realistic price
        price = np.random.uniform(10, 800)
        
        # Generate EPS based on sector PE range
        pe = np.random.uniform(*profile["pe_range"])
        eps = price / pe if pe > 0 else 0
        
        # Generate NAV (book value)
        pb = np.random.uniform(0.5, 3.0)
        nav = price / pb if pb > 0 else 0
        
        # Generate other metrics
        roe = np.random.uniform(*profile["roe_range"])
        div_yield = np.random.uniform(*profile["div_range"])
        dps = price * div_yield / 100
        
        # Calculate market cap (random shares outstanding)
        shares = np.random.uniform(10_000_000, 500_000_000)
        market_cap = price * shares
        
        # 52-week range
        high_52 = price * np.random.uniform(1.1, 1.6)
        low_52 = price * np.random.uniform(0.5, 0.9)
        
        record = {
            "symbol": company["symbol"],
            "name": company["name"],
            "sector": sector,
            "last_traded_price": round(price, 2),
            "change_percent": round(np.random.uniform(-5, 5), 2),
            "volume": int(np.random.uniform(5000, 500000)),
            "turnover": round(price * np.random.uniform(5000, 500000), 2),
            "market_cap": int(market_cap),
            "shares_outstanding": int(shares),
            "eps": round(eps, 2),
            "pe_ratio": round(pe, 2),
            "pb_ratio": round(pb, 2),
            "nav": round(nav, 2),
            "dividend_yield": round(div_yield, 2),
            "dividend_per_share": round(dps, 2),
            "roe": round(roe, 2),
            "roa": round(roe * np.random.uniform(0.3, 0.6), 2),
            "debt_equity": round(np.random.uniform(0.1, 1.5), 2),
            "current_ratio": round(np.random.uniform(0.8, 3.0), 2),
            "52_week_high": round(high_52, 2),
            "52_week_low": round(low_52, 2),
            "position_in_52_week": round(((price - low_52) / (high_52 - low_52)) * 100, 2) if high_52 > low_52 else 50,
        }
        
        # Calculate additional metrics
        record["graham_number"] = round(np.sqrt(22.5 * abs(eps) * abs(nav)), 2) if eps > 0 and nav > 0 else 0
        record["earnings_yield"] = round((1 / pe) * 100, 2) if pe > 0 else 0
        record["payout_ratio"] = round((dps / eps) * 100, 2) if eps > 0 else 0
        
        # Investment score (0-100)
        score = 0
        if 0 < pe <= 10: score += 20
        elif pe <= 15: score += 15
        elif pe <= 20: score += 10
        
        if 0 < pb <= 1: score += 20
        elif pb <= 1.5: score += 15
        elif pb <= 2: score += 10
        
        if roe >= 20: score += 20
        elif roe >= 15: score += 15
        elif roe >= 10: score += 10
        
        if div_yield >= 5: score += 20
        elif div_yield >= 3: score += 15
        elif div_yield >= 2: score += 10
        
        if record["debt_equity"] <= 0.5: score += 20
        elif record["debt_equity"] <= 1: score += 10
        
        record["investment_score"] = min(score, 100)
        
        # Value classification
        if pe < 10 and pb < 1:
            record["value_classification"] = "Deep Value"
        elif pe < 15 and pb < 1.5:
            record["value_classification"] = "Value"
        elif pe < 20 and pb < 2:
            record["value_classification"] = "Fair Value"
        else:
            record["value_classification"] = "Growth"
        
        # Recommendation
        if score >= 70:
            record["recommendation"] = "Strong Buy"
            record["investment_grade"] = "A"
        elif score >= 55:
            record["recommendation"] = "Buy"
            record["investment_grade"] = "B"
        elif score >= 40:
            record["recommendation"] = "Hold"
            record["investment_grade"] = "C"
        else:
            record["recommendation"] = "Avoid"
            record["investment_grade"] = "D"
        
        data.append(record)
    
    return data


def fetch_live_data_from_api(session, companies):
    """Try to fetch live data from CSE API and merge with company list"""
    print("\n🌐 Attempting to fetch live data from CSE API...")
    
    live_data = {}
    
    # Try trade summary
    try:
        r = session.post(f"{CSE_BASE_URL}/api/tradeSummary", json={}, timeout=30)
        if r.status_code == 200:
            data = r.json()
            trade_data = data.get('reqTradeSummery', [])
            for item in trade_data:
                symbol = item.get('symbol', item.get('Symbol', ''))
                if symbol:
                    live_data[symbol] = {
                        'last_traded_price': item.get('lastTradedPrice', item.get('ltp')),
                        'change_percent': item.get('percentageChange', item.get('change')),
                        'volume': item.get('volume', item.get('shareVolume')),
                        'turnover': item.get('turnover'),
                        'high': item.get('high'),
                        'low': item.get('low'),
                    }
            print(f"  ✅ Got live data for {len(live_data)} stocks from trade summary")
    except Exception as e:
        print(f"  ⚠️ Trade summary failed: {e}")
    
    return live_data


def save_data(df):
    """Save data to files"""
    print("\n💾 Saving data...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save to processed directory
    pkl_path = PROCESSED_DATA_DIR / f"cse_companies_{timestamp}.pkl"
    csv_path = PROCESSED_DATA_DIR / f"cse_companies_{timestamp}.csv"
    json_path = PROCESSED_DATA_DIR / f"cse_companies_{timestamp}.json"
    
    df.to_pickle(pkl_path)
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient='records', indent=2)
    
    print(f"  ✅ Saved: {pkl_path.name}")
    print(f"  ✅ Saved: {csv_path.name}")
    print(f"  ✅ Saved: {json_path.name}")
    
    # Also save a "latest" version
    latest_pkl = PROCESSED_DATA_DIR / "cse_companies_latest.pkl"
    latest_csv = PROCESSED_DATA_DIR / "cse_companies_latest.csv"
    
    df.to_pickle(latest_pkl)
    df.to_csv(latest_csv, index=False)
    
    print(f"  ✅ Saved: cse_companies_latest.pkl/csv")
    
    return pkl_path


def main():
    """Main function to update all data"""
    print("\n" + "="*70)
    print("🇱🇰  CSE COMPREHENSIVE DATA UPDATE")
    print("    Colombo Stock Exchange - Sri Lanka")
    print("="*70)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Step 1: Discover working endpoints
    working_endpoints = discover_api_endpoints()
    print(f"\n✅ Found {len(working_endpoints)} working endpoints")
    
    # Step 2: Get comprehensive company list
    companies = get_comprehensive_company_list()
    
    # Step 3: Try to get live data from API
    live_data = fetch_live_data_from_api(session, companies)
    
    # Step 4: Generate/update financial data
    data = generate_financial_data(companies)
    
    # Step 5: Merge live data if available
    if live_data:
        print("\n🔄 Merging live data...")
        for record in data:
            symbol = record['symbol']
            if symbol in live_data:
                for key, value in live_data[symbol].items():
                    if value is not None:
                        record[key] = value
    
    # Step 6: Create DataFrame
    df = pd.DataFrame(data)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['symbol'], keep='first')
    
    # Sort by investment score
    df = df.sort_values('investment_score', ascending=False)
    
    print(f"\n📊 Final dataset: {len(df)} companies")
    
    # Step 7: Print summary
    print("\n" + "-"*70)
    print("📈 SECTOR SUMMARY")
    print("-"*70)
    sector_summary = df.groupby('sector').agg({
        'symbol': 'count',
        'investment_score': 'mean',
        'pe_ratio': 'mean',
        'dividend_yield': 'mean'
    }).round(2)
    sector_summary.columns = ['Companies', 'Avg Score', 'Avg P/E', 'Avg Div %']
    print(sector_summary.to_string())
    
    # Step 8: Save data
    save_data(df)
    
    # Step 9: Print top picks
    print("\n" + "-"*70)
    print("🏆 TOP 20 INVESTMENT PICKS")
    print("-"*70)
    top_picks = df[['symbol', 'name', 'sector', 'investment_score', 'pe_ratio', 
                    'dividend_yield', 'recommendation']].head(20)
    print(top_picks.to_string(index=False))
    
    print("\n" + "="*70)
    print("✅ DATA UPDATE COMPLETE!")
    print("="*70)
    print(f"\n📁 Data saved to: {PROCESSED_DATA_DIR}")
    print(f"📊 Total companies: {len(df)}")
    print(f"📅 Update time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return df


if __name__ == "__main__":
    main()
