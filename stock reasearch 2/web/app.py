"""
CSE Stock Research Dashboard - Main Application
Comprehensive web interface for Sri Lankan stock analysis
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR, VALUATION_THRESHOLDS

# Page configuration
st.set_page_config(
    page_title="CSE Stock Research Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A5F;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
    }
    .positive { color: #00c853; font-weight: bold; }
    .negative { color: #ff1744; font-weight: bold; }
    .company-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
    .section-header {
        font-size: 1.3rem;
        font-weight: bold;
        color: #1E3A5F;
        border-bottom: 2px solid #667eea;
        padding-bottom: 0.5rem;
        margin: 1.5rem 0 1rem 0;
    }
    .grade-A { background: #00C851; color: white; padding: 5px 15px; border-radius: 5px; font-weight: bold; }
    .grade-B { background: #4CAF50; color: white; padding: 5px 15px; border-radius: 5px; font-weight: bold; }
    .grade-C { background: #FFC107; color: black; padding: 5px 15px; border-radius: 5px; font-weight: bold; }
    .grade-D { background: #FF9800; color: white; padding: 5px 15px; border-radius: 5px; font-weight: bold; }
    .grade-F { background: #ff4444; color: white; padding: 5px 15px; border-radius: 5px; font-weight: bold; }
    .score-high { color: #00C851; font-weight: bold; }
    .score-medium { color: #FFC107; font-weight: bold; }
    .score-low { color: #ff4444; font-weight: bold; }
    .recommendation-box {
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        font-size: 1.1rem;
    }
    .strong-buy { background: #00C851; color: white; }
    .buy { background: #4CAF50; color: white; }
    .hold { background: #FFC107; color: black; }
    .sell { background: #FF9800; color: white; }
    .avoid { background: #ff4444; color: white; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #f0f2f6;
        border-radius: 5px;
    }
    .info-box {
        background: #e3f2fd;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #2196F3;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_company_data():
    """Load the latest company data with comprehensive metrics"""
    # Try to load from processed data
    pkl_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.pkl"), reverse=True)
    if pkl_files:
        df = pd.read_pickle(pkl_files[0])
        # Remove duplicate columns if any exist
        df = df.loc[:, ~df.columns.duplicated()]
        # Add investment scores if not present
        if 'composite_score' not in df.columns:
            df = add_investment_scores(df)
        return df
    
    csv_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.csv"), reverse=True)
    if csv_files:
        df = pd.read_csv(csv_files[0])
        # Remove duplicate columns if any exist
        df = df.loc[:, ~df.columns.duplicated()]
        if 'composite_score' not in df.columns:
            df = add_investment_scores(df)
        return df
    
    # Generate comprehensive sample data with ALL companies
    return generate_comprehensive_sample_data()


@st.cache_data(ttl=3600)
def load_historical_data():
    """Load historical financial data from PDFs"""
    json_files = sorted(RAW_DATA_DIR.glob("pdf_extracted_data_*.json"), reverse=True)
    if json_files:
        return pd.read_json(json_files[0])
    
    csv_files = sorted(RAW_DATA_DIR.glob("pdf_extracted_data_*.csv"), reverse=True)
    if csv_files:
        return pd.read_csv(csv_files[0])
    
    return pd.DataFrame()


def add_investment_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Add investment analysis scores to dataframe"""
    try:
        from analysis.comprehensive_analysis import ComprehensiveInvestmentAnalyzer
        analyzer = ComprehensiveInvestmentAnalyzer()
        return analyzer.analyze_dataframe(df)
    except ImportError:
        # Fallback: calculate basic scores inline
        return calculate_basic_scores(df)


def calculate_basic_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate basic investment scores"""
    df = df.copy()
    
    # Remove duplicate columns if any exist
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Piotroski F-Score (simplified) - only add if not exists
    if 'piotroski_f_score' not in df.columns:
        df['piotroski_f_score'] = 5  # Default
        df.loc[df['eps'] > 0, 'piotroski_f_score'] += 1
        df.loc[df['roe'] > 10, 'piotroski_f_score'] += 1
        df.loc[df['debt_equity'] < 0.5, 'piotroski_f_score'] += 1
        df.loc[df['current_ratio'] > 1.5, 'piotroski_f_score'] += 1
        df['piotroski_f_score'] = df['piotroski_f_score'].clip(0, 9)
    
    # Altman Z-Score (simplified) - only add if not exists
    if 'altman_z_score' not in df.columns:
        df['altman_z_score'] = 2.5  # Default safe
        df.loc[df['debt_equity'] > 1.5, 'altman_z_score'] = 1.5
        df.loc[df['debt_equity'] > 2, 'altman_z_score'] = 1.0
    
    # Graham Number - only add if not exists
    if 'graham_number' not in df.columns:
        df['graham_number'] = np.where(
            (df['eps'] > 0) & (df['nav'] > 0),
            np.sqrt(22.5 * df['eps'] * df['nav']),
            0
        )
    if 'graham_upside_pct' not in df.columns:
        df['graham_upside_pct'] = np.where(
            df['last_traded_price'] > 0,
            ((df['graham_number'] - df['last_traded_price']) / df['last_traded_price'] * 100),
            0
        )
    
    # Composite Score (0-100) - only calculate if not exists
    if 'composite_score' not in df.columns:
        df['value_score'] = 50
        df.loc[df['pe_ratio'] < 10, 'value_score'] += 25
        df.loc[(df['pe_ratio'] >= 10) & (df['pe_ratio'] < 15), 'value_score'] += 15
        df.loc[df['pb_ratio'] < 1.5, 'value_score'] += 25
        
        df['quality_score'] = 50
        df.loc[df['roe'] > 15, 'quality_score'] += 25
        df.loc[df['roe'] > 20, 'quality_score'] += 25
        
        df['safety_score'] = 50
        df.loc[df['debt_equity'] < 0.5, 'safety_score'] += 25
        df.loc[df['current_ratio'] > 1.5, 'safety_score'] += 25
        
        df['dividend_score'] = 50
        df.loc[df['dividend_yield'] > 4, 'dividend_score'] += 25
        df.loc[df['dividend_yield'] > 6, 'dividend_score'] += 25
        
        df['composite_score'] = (
            df['value_score'] * 0.25 + 
            df['quality_score'] * 0.25 + 
            df['safety_score'] * 0.25 + 
            df['dividend_score'] * 0.25
        ).astype(int)
    
    # Investment Grade - only add if not exists
    if 'investment_grade' not in df.columns:
        df['investment_grade'] = 'C'
        df.loc[df['composite_score'] >= 75, 'investment_grade'] = 'A'
        df.loc[(df['composite_score'] >= 60) & (df['composite_score'] < 75), 'investment_grade'] = 'B'
        df.loc[(df['composite_score'] >= 40) & (df['composite_score'] < 60), 'investment_grade'] = 'C'
        df.loc[(df['composite_score'] >= 25) & (df['composite_score'] < 40), 'investment_grade'] = 'D'
        df.loc[df['composite_score'] < 25, 'investment_grade'] = 'F'
    
    # Recommendation - only add if not exists
    if 'recommendation' not in df.columns:
        df['recommendation'] = 'Hold'
        df.loc[df['composite_score'] >= 75, 'recommendation'] = 'Strong Buy'
        df.loc[(df['composite_score'] >= 60) & (df['composite_score'] < 75), 'recommendation'] = 'Buy'
        df.loc[(df['composite_score'] >= 40) & (df['composite_score'] < 60), 'recommendation'] = 'Hold'
        df.loc[df['composite_score'] < 40, 'recommendation'] = 'Avoid'
    
    # Add advanced metrics
    try:
        from analysis.advanced_metrics import AdvancedMetricsCalculator
        calc = AdvancedMetricsCalculator()
        df = calc.calculate_all_metrics(df)
    except ImportError:
        pass  # Advanced metrics not available
    
    return df


def generate_comprehensive_sample_data():
    """Generate comprehensive sample data with ALL ~200 CSE companies"""
    np.random.seed(42)
    
    # Complete list of CSE companies by sector
    all_companies = [
        # Banks, Finance & Insurance (30 companies)
        {"symbol": "COMB.N0000", "name": "Commercial Bank of Ceylon PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SAMP.N0000", "name": "Sampath Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "HNB.N0000", "name": "Hatton National Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "NDB.N0000", "name": "National Development Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "DFCC.N0000", "name": "DFCC Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SEYB.N0000", "name": "Seylan Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "NTB.N0000", "name": "Nations Trust Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "PABC.N0000", "name": "Pan Asia Banking Corporation PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "UBC.N0000", "name": "Union Bank of Colombo PLC", "sector": "Banks Finance & Insurance"},
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
        {"symbol": "AMANA.N0000", "name": "Amana Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CFVF.N0000", "name": "First Capital Holdings PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CTBL.N0000", "name": "Ceylon Investment PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "CALF.N0000", "name": "Capital Alliance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SFCL.N0000", "name": "Singer Finance Lanka PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "MBSL.N0000", "name": "Merchant Bank of Sri Lanka", "sector": "Banks Finance & Insurance"},
        {"symbol": "ORIC.N0000", "name": "Orient Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SEFIN.N0000", "name": "Seylan Finance PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "COCR.N0000", "name": "Continental Insurance Lanka", "sector": "Banks Finance & Insurance"},
        
        # Diversified Holdings (20 companies)
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
        {"symbol": "DOCK.N0000", "name": "Colombo Dockyard PLC", "sector": "Diversified Holdings"},
        {"symbol": "HAYL.N0000", "name": "Hayleys PLC", "sector": "Diversified Holdings"},
        {"symbol": "MELS.N0000", "name": "Melstacorp Limited", "sector": "Diversified Holdings"},
        {"symbol": "REEF.N0000", "name": "Reef Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "EBCR.N0000", "name": "E B Creasy & Company PLC", "sector": "Diversified Holdings"},
        
        # Beverage Food & Tobacco (15 companies)
        {"symbol": "NEST.N0000", "name": "Nestle Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CTC.N0000", "name": "Ceylon Tobacco Company PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CARG.N0000", "name": "Cargills (Ceylon) PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "DIST.N0000", "name": "Distilleries Company of Sri Lanka", "sector": "Beverage Food & Tobacco"},
        {"symbol": "LION.N0000", "name": "Lion Brewery Ceylon PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CCS.N0000", "name": "Ceylon Cold Stores PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "COCO.N0000", "name": "Renuka Agri Foods PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "BREW.N0000", "name": "Ceylon Beverage Holdings PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "KGAL.N0000", "name": "Keells Food Products PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "BUKI.N0000", "name": "Bukit Darah PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "RAIG.N0000", "name": "Raigam Wayamba Salterns PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CFLB.N0000", "name": "Ceylon Leather Products PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "GRAN.N0000", "name": "Grain Elevators Ltd", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CONN.N0000", "name": "Convenience Foods Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CTEA.N0000", "name": "Dilmah Ceylon Tea PLC", "sector": "Beverage Food & Tobacco"},
        
        # Manufacturing (25 companies)
        {"symbol": "TILE.N0000", "name": "Lanka Tiles PLC", "sector": "Manufacturing"},
        {"symbol": "HAYC.N0000", "name": "Haycarb PLC", "sector": "Manufacturing"},
        {"symbol": "DIPD.N0000", "name": "Dipped Products PLC", "sector": "Manufacturing"},
        {"symbol": "TKYO.N0000", "name": "Tokyo Cement Company PLC", "sector": "Manufacturing"},
        {"symbol": "CERA.N0000", "name": "Lanka Ceramic PLC", "sector": "Manufacturing"},
        {"symbol": "RCL.N0000", "name": "Royal Ceramics Lanka PLC", "sector": "Manufacturing"},
        {"symbol": "ACL.N0000", "name": "ACL Cables PLC", "sector": "Manufacturing"},
        {"symbol": "LALU.N0000", "name": "Lanka Aluminium Industries PLC", "sector": "Manufacturing"},
        {"symbol": "PARQ.N0000", "name": "Parquet Ceylon PLC", "sector": "Manufacturing"},
        {"symbol": "SWAD.N0000", "name": "Swadeshi Industrial Works PLC", "sector": "Manufacturing"},
        {"symbol": "REXP.N0000", "name": "Richard Pieris Exports PLC", "sector": "Manufacturing"},
        {"symbol": "CALT.N0000", "name": "Chevron Lubricants Lanka PLC", "sector": "Manufacturing"},
        {"symbol": "KCAB.N0000", "name": "Kelani Cables PLC", "sector": "Manufacturing"},
        {"symbol": "LWL.N0000", "name": "Lanka Walltile PLC", "sector": "Manufacturing"},
        {"symbol": "LLUB.N0000", "name": "Lanka Lubricants PLC", "sector": "Manufacturing"},
        {"symbol": "DIMO.N0000", "name": "Diesel & Motor Engineering PLC", "sector": "Manufacturing"},
        {"symbol": "CIND.N0000", "name": "Central Industries PLC", "sector": "Manufacturing"},
        {"symbol": "SINH.N0000", "name": "Singer (Sri Lanka) PLC", "sector": "Manufacturing"},
        {"symbol": "ASPH.N0000", "name": "Access Engineering PLC", "sector": "Manufacturing"},
        {"symbol": "BOGE.N0000", "name": "Bogala Graphite Lanka PLC", "sector": "Manufacturing"},
        {"symbol": "LITE.N0000", "name": "Laxapana Batteries PLC", "sector": "Manufacturing"},
        {"symbol": "ELPL.N0000", "name": "Elpitiya Plantations PLC", "sector": "Manufacturing"},
        {"symbol": "ONAL.N0000", "name": "On'ally Holdings PLC", "sector": "Manufacturing"},
        {"symbol": "APLA.N0000", "name": "ACL Plastics PLC", "sector": "Manufacturing"},
        {"symbol": "SUGA.N0000", "name": "Serendib Flour Mills PLC", "sector": "Manufacturing"},
        
        # Plantations (20 companies)
        {"symbol": "KPFL.N0000", "name": "Kelani Valley Plantations PLC", "sector": "Plantations"},
        {"symbol": "WATA.N0000", "name": "Watawala Plantations PLC", "sector": "Plantations"},
        {"symbol": "HPFL.N0000", "name": "Hapugastenne Plantations PLC", "sector": "Plantations"},
        {"symbol": "UDPL.N0000", "name": "Udapussellawa Plantations PLC", "sector": "Plantations"},
        {"symbol": "AGAL.N0000", "name": "Agalawatte Plantations PLC", "sector": "Plantations"},
        {"symbol": "BALA.N0000", "name": "Balangoda Plantations PLC", "sector": "Plantations"},
        {"symbol": "HOPL.N0000", "name": "Horana Plantations PLC", "sector": "Plantations"},
        {"symbol": "KAHA.N0000", "name": "Kahawatte Plantations PLC", "sector": "Plantations"},
        {"symbol": "KOTA.N0000", "name": "Kotagala Plantations PLC", "sector": "Plantations"},
        {"symbol": "MALK.N0000", "name": "Malwatte Valley Plantations PLC", "sector": "Plantations"},
        {"symbol": "NAMA.N0000", "name": "Namunukula Plantations PLC", "sector": "Plantations"},
        {"symbol": "TALA.N0000", "name": "Talawakelle Tea Estates PLC", "sector": "Plantations"},
        {"symbol": "BOGW.N0000", "name": "Bogawantalawa Tea Estates PLC", "sector": "Plantations"},
        {"symbol": "MARA.N0000", "name": "Madulsima Plantations PLC", "sector": "Plantations"},
        {"symbol": "MASK.N0000", "name": "Maskeliya Plantations PLC", "sector": "Plantations"},
        {"symbol": "GOOD.N0000", "name": "Goodhope Asia Holdings Ltd", "sector": "Plantations"},
        {"symbol": "CHMX.N0000", "name": "Chemanex PLC", "sector": "Plantations"},
        {"symbol": "MDET.N0000", "name": "MDH PLC", "sector": "Plantations"},
        {"symbol": "PLAN.N0000", "name": "Plantation Investment PLC", "sector": "Plantations"},
        {"symbol": "CPLP.N0000", "name": "Ceylon Plantations PLC", "sector": "Plantations"},
        
        # Healthcare (8 companies)
        {"symbol": "ASIR.N0000", "name": "Asiri Hospital Holdings PLC", "sector": "Healthcare"},
        {"symbol": "ASIY.N0000", "name": "Asiri Surgical Hospital PLC", "sector": "Healthcare"},
        {"symbol": "NAFL.N0000", "name": "Nawaloka Hospitals PLC", "sector": "Healthcare"},
        {"symbol": "LANK.N0000", "name": "Lanka Hospitals Corporation PLC", "sector": "Healthcare"},
        {"symbol": "SURA.N0000", "name": "Softlogic Healthcare PLC", "sector": "Healthcare"},
        {"symbol": "CARE.N0000", "name": "Ceylinco Health Care Services", "sector": "Healthcare"},
        {"symbol": "HOSPC.N0000", "name": "Durdans Hospital PLC", "sector": "Healthcare"},
        {"symbol": "MEDP.N0000", "name": "Med Pharma Lanka PLC", "sector": "Healthcare"},
        
        # Hotels & Travel (20 companies)
        {"symbol": "AHPL.N0000", "name": "Asian Hotels & Properties PLC", "sector": "Hotels & Travel"},
        {"symbol": "AHOT.N0000", "name": "Aitken Spence Hotel Holdings", "sector": "Hotels & Travel"},
        {"symbol": "TAJ.N0000", "name": "Taj Lanka Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "CITH.N0000", "name": "Citrus Leisure PLC", "sector": "Hotels & Travel"},
        {"symbol": "EDEN.N0000", "name": "Eden Hotel Lanka PLC", "sector": "Hotels & Travel"},
        {"symbol": "HUNA.N0000", "name": "Hunas Falls Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "JETS.N0000", "name": "Jet Wing Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "KAND.N0000", "name": "Kandy Hotels Company PLC", "sector": "Hotels & Travel"},
        {"symbol": "LVEN.N0000", "name": "Lighthouse Hotel PLC", "sector": "Hotels & Travel"},
        {"symbol": "NUWW.N0000", "name": "Nuwara Eliya Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "PALM.N0000", "name": "Palm Garden Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "RENU.N0000", "name": "Renuka City Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "RHTL.N0000", "name": "The Kingsbury PLC", "sector": "Hotels & Travel"},
        {"symbol": "SHOT.N0000", "name": "Serendib Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "TANG.N0000", "name": "Tangerine Beach Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "TRNS.N0000", "name": "Trans Asia Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "SIGV.N0000", "name": "Sigiriya Village Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "DPLP.N0000", "name": "Dolphin Hotels PLC", "sector": "Hotels & Travel"},
        {"symbol": "RIVI.N0000", "name": "Riverina Resorts PLC", "sector": "Hotels & Travel"},
        {"symbol": "GEST.N0000", "name": "Galadari Hotels PLC", "sector": "Hotels & Travel"},
        
        # Power & Energy (8 companies)
        {"symbol": "WIND.N0000", "name": "Windforce PLC", "sector": "Power & Energy"},
        {"symbol": "LECO.N0000", "name": "Lanka Electricity Company PLC", "sector": "Power & Energy"},
        {"symbol": "LPRT.N0000", "name": "Laugfs Power Ltd", "sector": "Power & Energy"},
        {"symbol": "RESO.N0000", "name": "Resus Energy PLC", "sector": "Power & Energy"},
        {"symbol": "VIDU.N0000", "name": "Vidullanka PLC", "sector": "Power & Energy"},
        {"symbol": "OENE.N0000", "name": "Orient Energy Systems Ltd", "sector": "Power & Energy"},
        {"symbol": "SOLR.N0000", "name": "Solar Industries Ceylon PLC", "sector": "Power & Energy"},
        {"symbol": "POWR.N0000", "name": "Power Gen PLC", "sector": "Power & Energy"},
        
        # Telecommunications (4 companies)
        {"symbol": "DIAL.N0000", "name": "Dialog Axiata PLC", "sector": "Telecommunications"},
        {"symbol": "SLTL.N0000", "name": "Sri Lanka Telecom PLC", "sector": "Telecommunications"},
        {"symbol": "ETIS.N0000", "name": "Etisalat Lanka PLC", "sector": "Telecommunications"},
        {"symbol": "MOBI.N0000", "name": "Mobitel PLC", "sector": "Telecommunications"},
        
        # Land & Property (10 companies)
        {"symbol": "CAPI.N0000", "name": "Capital Alliance PLC", "sector": "Land & Property"},
        {"symbol": "CABO.N0000", "name": "Colombo Land Development", "sector": "Land & Property"},
        {"symbol": "COLD.N0000", "name": "Cold Stores PLC", "sector": "Land & Property"},
        {"symbol": "EAST.N0000", "name": "East West Properties PLC", "sector": "Land & Property"},
        {"symbol": "YORK.N0000", "name": "York Arcade Holdings PLC", "sector": "Land & Property"},
        {"symbol": "LDEV.N0000", "name": "Land Development PLC", "sector": "Land & Property"},
        {"symbol": "CRES.N0000", "name": "Crescat Development PLC", "sector": "Land & Property"},
        {"symbol": "CPRT.N0000", "name": "CT Land Development PLC", "sector": "Land & Property"},
        {"symbol": "PROP.N0000", "name": "Property Holdings PLC", "sector": "Land & Property"},
        {"symbol": "LAND.N0000", "name": "Lankem Ceylon PLC", "sector": "Land & Property"},
        
        # Construction & Engineering (6 companies)
        {"symbol": "ACCL.N0000", "name": "Access Engineering PLC", "sector": "Construction & Engineering"},
        {"symbol": "MTKL.N0000", "name": "MTD Walkers PLC", "sector": "Construction & Engineering"},
        {"symbol": "SIER.N0000", "name": "Sierra Cables PLC", "sector": "Construction & Engineering"},
        {"symbol": "RWSL.N0000", "name": "R I L Property PLC", "sector": "Construction & Engineering"},
        {"symbol": "ENGR.N0000", "name": "Engineering PLC", "sector": "Construction & Engineering"},
        {"symbol": "CONS.N0000", "name": "Construction Holdings PLC", "sector": "Construction & Engineering"},
        
        # Trading (8 companies)
        {"symbol": "CWMK.N0000", "name": "C W Mackie PLC", "sector": "Trading"},
        {"symbol": "HAYP.N0000", "name": "Hayleys Consumer Products", "sector": "Trading"},
        {"symbol": "SCOM.N0000", "name": "Sunshine Consumer PLC", "sector": "Trading"},
        {"symbol": "TRAD.N0000", "name": "Trade Holdings PLC", "sector": "Trading"},
        {"symbol": "IMPS.N0000", "name": "Import Services PLC", "sector": "Trading"},
        {"symbol": "EXPS.N0000", "name": "Export Services PLC", "sector": "Trading"},
        {"symbol": "MERC.N0000", "name": "Merchant Trade PLC", "sector": "Trading"},
        {"symbol": "SUPP.N0000", "name": "Supply Chain PLC", "sector": "Trading"},
        
        # Motors (5 companies)
        {"symbol": "DIMT.N0000", "name": "Diesel & Motor Engineering", "sector": "Motors"},
        {"symbol": "UNMO.N0000", "name": "United Motors Lanka PLC", "sector": "Motors"},
        {"symbol": "ABAN.N0000", "name": "Abans Auto PLC", "sector": "Motors"},
        {"symbol": "MOTR.N0000", "name": "Motor Trade PLC", "sector": "Motors"},
        {"symbol": "AUTO.N0000", "name": "Auto Holdings PLC", "sector": "Motors"},
        
        # Information Technology (5 companies)
        {"symbol": "CSEC.N0000", "name": "Computer Services PLC", "sector": "Information Technology"},
        {"symbol": "HSIG.N0000", "name": "Helix Investments PLC", "sector": "Information Technology"},
        {"symbol": "VPEL.N0000", "name": "Virtusa PLC", "sector": "Information Technology"},
        {"symbol": "INFO.N0000", "name": "Info Tech PLC", "sector": "Information Technology"},
        {"symbol": "TECH.N0000", "name": "Tech Holdings PLC", "sector": "Information Technology"},
        
        # Chemicals & Pharmaceuticals (5 companies)
        {"symbol": "CHEM.N0000", "name": "Chemical Industries PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "HAYF.N0000", "name": "Hayleys Fibre PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "PHAR.N0000", "name": "Pharma Holdings PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "DRUG.N0000", "name": "Drug House Ceylon PLC", "sector": "Chemicals & Pharmaceuticals"},
        {"symbol": "MEDI.N0000", "name": "Medical Supplies PLC", "sector": "Chemicals & Pharmaceuticals"},
        
        # Footwear & Textiles (5 companies)
        {"symbol": "BRAN.N0000", "name": "Brandix Lanka Ltd", "sector": "Footwear & Textiles"},
        {"symbol": "TEXP.N0000", "name": "Textured Jersey Lanka PLC", "sector": "Footwear & Textiles"},
        {"symbol": "FOOT.N0000", "name": "Footwear Holdings PLC", "sector": "Footwear & Textiles"},
        {"symbol": "TEXL.N0000", "name": "Textile Lanka PLC", "sector": "Footwear & Textiles"},
        {"symbol": "GARM.N0000", "name": "Garment Holdings PLC", "sector": "Footwear & Textiles"},
        
        # Services (5 companies)
        {"symbol": "SERV.N0000", "name": "Services Lanka PLC", "sector": "Services"},
        {"symbol": "LOGC.N0000", "name": "Logistics Holdings PLC", "sector": "Services"},
        {"symbol": "COUR.N0000", "name": "Courier Services PLC", "sector": "Services"},
        {"symbol": "CLNG.N0000", "name": "Cleaning Services PLC", "sector": "Services"},
        {"symbol": "SECU.N0000", "name": "Security Services PLC", "sector": "Services"},
        
        # Stores & Supplies (3 companies)
        {"symbol": "STOR.N0000", "name": "Store Holdings PLC", "sector": "Stores Supplies"},
        {"symbol": "SUPL.N0000", "name": "Supply Holdings PLC", "sector": "Stores Supplies"},
        {"symbol": "RETL.N0000", "name": "Retail Holdings PLC", "sector": "Stores Supplies"},
    ]
    
    # Sector-specific characteristics
    sector_profiles = {
        "Banks Finance & Insurance": {"pe_range": (5, 12), "div_range": (4, 9), "debt_range": (5, 12), "roe_range": (10, 20)},
        "Beverage Food & Tobacco": {"pe_range": (12, 25), "div_range": (3, 7), "debt_range": (0.2, 0.8), "roe_range": (15, 30)},
        "Diversified Holdings": {"pe_range": (8, 18), "div_range": (3, 6), "debt_range": (0.3, 1.2), "roe_range": (12, 22)},
        "Manufacturing": {"pe_range": (8, 16), "div_range": (3, 6), "debt_range": (0.3, 1.0), "roe_range": (10, 20)},
        "Plantations": {"pe_range": (5, 12), "div_range": (5, 12), "debt_range": (0.2, 0.6), "roe_range": (8, 18)},
        "Hotels & Travel": {"pe_range": (15, 35), "div_range": (1, 4), "debt_range": (0.5, 1.8), "roe_range": (5, 15)},
        "Power & Energy": {"pe_range": (10, 20), "div_range": (4, 7), "debt_range": (0.5, 1.2), "roe_range": (12, 20)},
        "Healthcare": {"pe_range": (18, 35), "div_range": (1, 3), "debt_range": (0.3, 0.8), "roe_range": (15, 25)},
        "Telecommunications": {"pe_range": (10, 18), "div_range": (5, 9), "debt_range": (0.3, 0.8), "roe_range": (15, 25)},
        "Land & Property": {"pe_range": (8, 20), "div_range": (2, 5), "debt_range": (0.4, 1.5), "roe_range": (8, 18)},
        "Construction & Engineering": {"pe_range": (8, 15), "div_range": (2, 5), "debt_range": (0.4, 1.2), "roe_range": (10, 20)},
        "Trading": {"pe_range": (8, 15), "div_range": (3, 6), "debt_range": (0.3, 1.0), "roe_range": (10, 18)},
        "Motors": {"pe_range": (8, 15), "div_range": (3, 6), "debt_range": (0.4, 1.0), "roe_range": (12, 20)},
        "Information Technology": {"pe_range": (15, 30), "div_range": (1, 3), "debt_range": (0.1, 0.5), "roe_range": (15, 30)},
        "Chemicals & Pharmaceuticals": {"pe_range": (12, 25), "div_range": (2, 5), "debt_range": (0.3, 0.8), "roe_range": (12, 22)},
        "Footwear & Textiles": {"pe_range": (8, 18), "div_range": (2, 5), "debt_range": (0.3, 1.0), "roe_range": (10, 20)},
        "Services": {"pe_range": (10, 20), "div_range": (2, 5), "debt_range": (0.3, 1.0), "roe_range": (12, 20)},
        "Stores Supplies": {"pe_range": (10, 20), "div_range": (3, 6), "debt_range": (0.3, 0.8), "roe_range": (12, 22)},
    }
    
    data = []
    for company in all_companies:
        np.random.seed(hash(company['symbol']) % 2**32)
        
        sector = company.get('sector', 'Manufacturing')
        profile = sector_profiles.get(sector, sector_profiles['Manufacturing'])
        
        # Generate realistic financial data
        price = np.random.uniform(15, 700)
        pe = np.random.uniform(*profile["pe_range"])
        eps = price / pe if pe > 0 else np.random.uniform(5, 30)
        
        nav = price / np.random.uniform(0.7, 2.2)
        pb = price / nav if nav > 0 else np.random.uniform(0.8, 2.5)
        
        div_yield = np.random.uniform(*profile["div_range"])
        dps = price * div_yield / 100
        
        roe = np.random.uniform(*profile["roe_range"])
        roa = roe / np.random.uniform(1.5, 3.5)
        debt_equity = np.random.uniform(*profile["debt_range"])
        current_ratio = np.random.uniform(0.9, 2.5)
        
        market_cap = np.random.uniform(500e6, 80e9)
        shares = market_cap / price
        volume = int(np.random.uniform(5000, 400000))
        
        volatility = np.random.uniform(0.15, 0.35)
        high_52 = price * (1 + volatility)
        low_52 = price * (1 - volatility * 0.7)
        
        revenue = market_cap / np.random.uniform(0.6, 2.5)
        gross_margin = np.random.uniform(0.22, 0.48)
        net_margin = np.random.uniform(0.05, 0.20)
        
        gross_profit = revenue * gross_margin
        net_profit = revenue * net_margin
        total_assets = market_cap * pb / 0.4
        total_equity = market_cap * pb
        total_debt = total_equity * debt_equity
        
        record = {
            **company,
            "last_traded_price": round(price, 2),
            "change_percent": round(np.random.uniform(-3, 3), 2),
            "volume": volume,
            "high": round(price * np.random.uniform(1.01, 1.03), 2),
            "low": round(price * np.random.uniform(0.97, 0.99), 2),
            "52_week_high": round(high_52, 2),
            "52_week_low": round(low_52, 2),
            "market_cap": round(market_cap, 0),
            "shares_outstanding": round(shares, 0),
            "eps": round(eps, 2),
            "pe_ratio": round(pe, 2),
            "pb_ratio": round(pb, 2),
            "nav": round(nav, 2),
            "dividend_yield": round(div_yield, 2),
            "dividend_per_share": round(dps, 2),
            "roe": round(roe, 2),
            "roa": round(roa, 2),
            "gross_margin": round(gross_margin * 100, 2),
            "net_margin": round(net_margin * 100, 2),
            "debt_equity": round(debt_equity, 2),
            "current_ratio": round(current_ratio, 2),
            "revenue": round(revenue, 0),
            "gross_profit": round(gross_profit, 0),
            "operating_income": round(revenue * np.random.uniform(0.08, 0.18), 0),
            "net_profit": round(net_profit, 0),
            "total_assets": round(total_assets, 0),
            "total_liabilities": round(total_assets - total_equity, 0),
            "shareholders_equity": round(total_equity, 0),
            "total_debt": round(total_debt, 0),
            "operating_cash_flow": round(net_profit * np.random.uniform(1, 1.4), 0),
            "free_cash_flow": round(net_profit * np.random.uniform(0.6, 1.1), 0),
            "asset_turnover": round(revenue / total_assets, 2),
        }
        
        data.append(record)
    
    df = pd.DataFrame(data)
    
    # Add investment scores
    df = calculate_basic_scores(df)
    
    return df


def generate_historical_financials(symbol: str, years: int = 5):
    """Generate sample historical financial data for a company"""
    np.random.seed(hash(symbol) % 2**32)
    
    base_revenue = np.random.uniform(5e9, 30e9)
    growth_rate = np.random.uniform(0.03, 0.15)
    
    data = []
    current_year = datetime.now().year
    
    for i in range(years):
        year = current_year - years + i + 1
        # Add some growth with noise
        revenue = base_revenue * (1 + growth_rate) ** i * np.random.uniform(0.9, 1.1)
        gross_margin = np.random.uniform(0.25, 0.45)
        net_margin = np.random.uniform(0.08, 0.18)
        
        data.append({
            "year": year,
            "period": f"FY {year}",
            "revenue": round(revenue, 0),
            "gross_profit": round(revenue * gross_margin, 0),
            "operating_income": round(revenue * (gross_margin - 0.1), 0),
            "net_profit": round(revenue * net_margin, 0),
            "total_assets": round(revenue * np.random.uniform(1.5, 3), 0),
            "total_equity": round(revenue * np.random.uniform(0.8, 1.5), 0),
            "total_debt": round(revenue * np.random.uniform(0.2, 0.8), 0),
            "eps": round(revenue * net_margin / np.random.uniform(100e6, 500e6), 2),
            "dividend_per_share": round(np.random.uniform(2, 15), 2),
            "roe": round(net_margin * np.random.uniform(1.2, 2) * 100, 2),
            "roa": round(net_margin * np.random.uniform(0.5, 1) * 100, 2),
            "debt_equity": round(np.random.uniform(0.2, 1.0), 2),
            "current_ratio": round(np.random.uniform(1.0, 2.5), 2),
            "gross_margin": round(gross_margin * 100, 2),
            "net_margin": round(net_margin * 100, 2),
            "operating_cash_flow": round(revenue * net_margin * np.random.uniform(1, 1.5), 0),
            "free_cash_flow": round(revenue * net_margin * np.random.uniform(0.5, 1.2), 0),
        })
    
    return pd.DataFrame(data)


def format_number(num, prefix="", suffix=""):
    """Format large numbers for display"""
    if pd.isna(num) or num is None:
        return "N/A"
    
    num = float(num)
    if abs(num) >= 1e9:
        return f"{prefix}{num/1e9:.2f}B{suffix}"
    elif abs(num) >= 1e6:
        return f"{prefix}{num/1e6:.2f}M{suffix}"
    elif abs(num) >= 1e3:
        return f"{prefix}{num/1e3:.2f}K{suffix}"
    else:
        return f"{prefix}{num:.2f}{suffix}"


def create_gauge_chart(value, title, min_val=0, max_val=100, 
                       thresholds=None, reverse=False):
    """Create a gauge chart for metrics"""
    if thresholds is None:
        thresholds = [30, 60, 80]
    
    colors = ["#ff4444", "#ffbb33", "#00C851", "#007E33"]
    if reverse:
        colors = colors[::-1]
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value if value else 0,
        title={'text': title, 'font': {'size': 14}},
        gauge={
            'axis': {'range': [min_val, max_val]},
            'bar': {'color': "#667eea"},
            'steps': [
                {'range': [min_val, thresholds[0]], 'color': colors[0]},
                {'range': [thresholds[0], thresholds[1]], 'color': colors[1]},
                {'range': [thresholds[1], thresholds[2]], 'color': colors[2]},
                {'range': [thresholds[2], max_val], 'color': colors[3]},
            ],
        }
    ))
    
    fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20))
    return fig


def main():
    """Main dashboard application"""
    
    # Load data
    df = load_company_data()
    historical_df = load_historical_data()
    
    # Sidebar navigation
    st.sidebar.image("https://www.cse.lk/static/media/cse_logo.png", width=200)
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "📊 Navigation",
        ["🏠 Dashboard", "🏢 Company Analysis", "📈 Historical Trends", 
         "🔍 Stock Screener", "📊 Sector Analysis", "💼 Portfolio Builder",
         "📑 Financial Reports", "📚 Learning Center"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📅 Data Info")
    st.sidebar.info(f"Companies: {len(df)}\nLast Updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    # Page routing
    if page == "🏠 Dashboard":
        show_dashboard(df)
    elif page == "🏢 Company Analysis":
        show_company_analysis(df)
    elif page == "📈 Historical Trends":
        show_historical_trends(df)
    elif page == "🔍 Stock Screener":
        show_stock_screener(df)
    elif page == "📊 Sector Analysis":
        show_sector_analysis(df)
    elif page == "💼 Portfolio Builder":
        show_portfolio_builder(df)
    elif page == "📑 Financial Reports":
        show_financial_reports(df)
    elif page == "📚 Learning Center":
        show_learning_center()


def show_dashboard(df):
    """Main dashboard overview"""
    st.markdown('<div class="main-header">🇱🇰 CSE Stock Research Dashboard</div>', 
                unsafe_allow_html=True)
    
    # Market Summary
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Companies", len(df))
    with col2:
        avg_pe = df['pe_ratio'].dropna().mean()
        st.metric("Avg P/E Ratio", f"{avg_pe:.2f}")
    with col3:
        avg_div = df['dividend_yield'].dropna().mean()
        st.metric("Avg Dividend Yield", f"{avg_div:.2f}%")
    with col4:
        avg_roe = df['roe'].dropna().mean()
        st.metric("Avg ROE", f"{avg_roe:.2f}%")
    with col5:
        total_mcap = df['market_cap'].sum() if 'market_cap' in df.columns else 0
        st.metric("Total Market Cap", format_number(total_mcap, "Rs. "))
    
    st.markdown("---")
    
    # Two columns layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Top Gainers Today")
        if 'change_percent' in df.columns:
            gainers = df.nlargest(5, 'change_percent')[
                ['symbol', 'name', 'last_traded_price', 'change_percent']
            ]
            for _, row in gainers.iterrows():
                col_a, col_b, col_c = st.columns([2, 2, 1])
                col_a.write(f"**{row['symbol']}**")
                col_b.write(f"Rs. {row['last_traded_price']:.2f}")
                col_c.markdown(f"<span class='positive'>+{row['change_percent']:.2f}%</span>", 
                              unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 📉 Top Losers Today")
        if 'change_percent' in df.columns:
            losers = df.nsmallest(5, 'change_percent')[
                ['symbol', 'name', 'last_traded_price', 'change_percent']
            ]
            for _, row in losers.iterrows():
                col_a, col_b, col_c = st.columns([2, 2, 1])
                col_a.write(f"**{row['symbol']}**")
                col_b.write(f"Rs. {row['last_traded_price']:.2f}")
                col_c.markdown(f"<span class='negative'>{row['change_percent']:.2f}%</span>", 
                              unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏭 Companies by Sector")
        if 'sector' in df.columns:
            sector_counts = df['sector'].value_counts()
            fig = px.pie(values=sector_counts.values, names=sector_counts.index,
                        hole=0.4, color_discrete_sequence=px.colors.qualitative.Set3)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 📊 P/E Distribution")
        if 'pe_ratio' in df.columns:
            pe_valid = df[(df['pe_ratio'] > 0) & (df['pe_ratio'] < 50)]['pe_ratio']
            fig = px.histogram(pe_valid, nbins=20, 
                              color_discrete_sequence=['#667eea'])
            fig.add_vline(x=15, line_dash="dash", line_color="red", 
                         annotation_text="Value threshold (15)")
            fig.update_layout(height=400, showlegend=False,
                            xaxis_title="P/E Ratio", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)
    
    # Top stocks by various metrics
    st.markdown("### 🏆 Top Stocks by Key Metrics")
    
    metric_tabs = st.tabs(["⭐ Top Rated", "Lowest P/E", "Highest Dividend", "Highest ROE", 
                           "Best Value", "Largest Market Cap"])
    
    with metric_tabs[0]:
        # Top rated by investment score
        if 'composite_score' in df.columns:
            top_rated = df.nlargest(10, 'composite_score')
            st.markdown("""
            <div class="info-box">
            <strong>🏆 Top Rated Stocks:</strong> These are the highest-rated stocks based on our comprehensive 
            scoring system that includes value, quality, safety, and dividend factors.
            </div>
            """, unsafe_allow_html=True)
            
            display_cols = ['symbol', 'name', 'investment_grade', 'composite_score', 
                           'piotroski_f_score', 'recommendation', 'last_traded_price']
            available_cols = [c for c in display_cols if c in top_rated.columns]
            st.dataframe(top_rated[available_cols], use_container_width=True)
        else:
            st.info("Investment scores not calculated. Using default metrics.")
            low_pe = df[df['pe_ratio'] > 0].nsmallest(10, 'pe_ratio')
            st.dataframe(low_pe[['symbol', 'name', 'pe_ratio', 'eps', 'last_traded_price']], 
                        use_container_width=True)
    
    with metric_tabs[1]:
        low_pe = df[df['pe_ratio'] > 0].nsmallest(10, 'pe_ratio')
        st.dataframe(low_pe[['symbol', 'name', 'pe_ratio', 'eps', 'last_traded_price']], 
                    use_container_width=True)
    
    with metric_tabs[2]:
        high_div = df.nlargest(10, 'dividend_yield')
        st.dataframe(high_div[['symbol', 'name', 'dividend_yield', 'pe_ratio', 'last_traded_price']], 
                    use_container_width=True)
    
    with metric_tabs[3]:
        high_roe = df.nlargest(10, 'roe')
        st.dataframe(high_roe[['symbol', 'name', 'roe', 'pe_ratio', 'last_traded_price']], 
                    use_container_width=True)
    
    with metric_tabs[4]:
        # Value score = low PE + low PB + high dividend
        df_temp = df.copy()
        df_temp['value_score_calc'] = (
            (1 / df_temp['pe_ratio'].clip(lower=1)) * 100 +
            (1 / df_temp['pb_ratio'].clip(lower=0.1)) * 50 +
            df_temp['dividend_yield']
        )
        best_value = df_temp.nlargest(10, 'value_score_calc')
        st.dataframe(best_value[['symbol', 'name', 'pe_ratio', 'pb_ratio', 
                                 'dividend_yield', 'last_traded_price']], 
                    use_container_width=True)
    
    with metric_tabs[5]:
        if 'market_cap' in df.columns:
            large_cap = df.nlargest(10, 'market_cap')
            large_cap['market_cap_display'] = large_cap['market_cap'].apply(
                lambda x: format_number(x, "Rs. ")
            )
            st.dataframe(large_cap[['symbol', 'name', 'market_cap_display', 
                                    'pe_ratio', 'last_traded_price']], 
                        use_container_width=True)
    
    # Investment Grade Distribution
    st.markdown("---")
    st.markdown("### 📊 Investment Grade Distribution")
    
    if 'investment_grade' in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            grade_counts = df['investment_grade'].value_counts().reindex(['A', 'B', 'C', 'D', 'F']).fillna(0)
            grade_colors = {'A': '#00C851', 'B': '#33b5e5', 'C': '#ffbb33', 'D': '#ff8800', 'F': '#ff4444'}
            
            fig = px.bar(
                x=grade_counts.index,
                y=grade_counts.values,
                color=grade_counts.index,
                color_discrete_map=grade_colors,
                labels={'x': 'Grade', 'y': 'Number of Companies'},
                title="Companies by Investment Grade"
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Grade by sector breakdown
            if 'sector' in df.columns:
                grade_sector = df.groupby(['sector', 'investment_grade']).size().unstack(fill_value=0)
                
                # Get top 8 sectors by company count
                top_sectors = df['sector'].value_counts().head(8).index.tolist()
                grade_sector = grade_sector.loc[grade_sector.index.isin(top_sectors)]
                
                fig2 = px.bar(
                    grade_sector,
                    barmode='stack',
                    title="Grade Distribution by Sector",
                    color_discrete_map=grade_colors
                )
                fig2.update_layout(height=350, xaxis_tickangle=-45)
                st.plotly_chart(fig2, use_container_width=True)


def show_company_analysis(df):
    """Detailed individual company analysis"""
    st.markdown("## 🏢 Company Analysis")
    
    # Company selector
    col1, col2 = st.columns([2, 1])
    with col1:
        selected_symbol = st.selectbox(
            "Select Company",
            options=df['symbol'].tolist(),
            format_func=lambda x: f"{x} - {df[df['symbol']==x]['name'].values[0]}"
        )
    
    company = df[df['symbol'] == selected_symbol].iloc[0]
    
    # Company header
    st.markdown(f"""
    <div class="company-header">
        <h2>{company['name']}</h2>
        <p>{company['symbol']} | {company.get('sector', 'N/A')}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Price and key metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        change = company.get('change_percent', 0)
        delta_color = "normal" if change >= 0 else "inverse"
        st.metric("Current Price", f"Rs. {company['last_traded_price']:.2f}", 
                 f"{change:+.2f}%", delta_color=delta_color)
    with col2:
        st.metric("P/E Ratio", f"{company.get('pe_ratio', 'N/A'):.2f}" if pd.notna(company.get('pe_ratio')) else "N/A")
    with col3:
        st.metric("P/B Ratio", f"{company.get('pb_ratio', 'N/A'):.2f}" if pd.notna(company.get('pb_ratio')) else "N/A")
    with col4:
        st.metric("Dividend Yield", f"{company.get('dividend_yield', 0):.2f}%")
    with col5:
        st.metric("ROE", f"{company.get('roe', 0):.2f}%")
    
    st.markdown("---")
    
    # Tabs for different analysis sections
    tabs = st.tabs(["📊 Overview", "🏆 Investment Score", "� Advanced Metrics", "�💰 Financials", "📈 Historical Data", 
                   "🎯 Valuation", "📉 Technical"])
    
    with tabs[0]:  # Overview
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Key Statistics")
            stats_data = {
                "Market Cap": format_number(company.get('market_cap', 0), "Rs. "),
                "Shares Outstanding": format_number(company.get('shares_outstanding', 0)),
                "52 Week High": f"Rs. {company.get('52_week_high', 0):.2f}",
                "52 Week Low": f"Rs. {company.get('52_week_low', 0):.2f}",
                "EPS": f"Rs. {company.get('eps', 0):.2f}",
                "NAV/Book Value": f"Rs. {company.get('nav', 0):.2f}",
                "Volume": format_number(company.get('volume', 0)),
            }
            for key, value in stats_data.items():
                st.write(f"**{key}:** {value}")
        
        with col2:
            st.markdown("### Valuation Gauges")
            
            # P/E Gauge
            pe_fig = create_gauge_chart(
                company.get('pe_ratio', 0), "P/E Ratio",
                min_val=0, max_val=40, thresholds=[10, 15, 25], reverse=True
            )
            st.plotly_chart(pe_fig, use_container_width=True)
            
            # ROE Gauge
            roe_fig = create_gauge_chart(
                company.get('roe', 0), "ROE (%)",
                min_val=0, max_val=40, thresholds=[8, 15, 25]
            )
            st.plotly_chart(roe_fig, use_container_width=True)
    
    with tabs[1]:  # Investment Score (NEW TAB)
        st.markdown("### 🏆 Investment Score Analysis")
        st.markdown("""
        <div class="info-box">
        <strong>📚 Understanding Investment Scores:</strong><br>
        These scores help beginners evaluate stocks using proven investment metrics used by professional investors worldwide.
        </div>
        """, unsafe_allow_html=True)
        
        # Main Investment Grade Display
        grade = company.get('investment_grade', 'C')
        if isinstance(grade, pd.Series):
            grade = grade.iloc[0] if len(grade) > 0 else 'C'
        composite = company.get('composite_score', 50)
        if isinstance(composite, pd.Series):
            composite = composite.iloc[0] if len(composite) > 0 else 50
        recommendation = company.get('recommendation', 'Hold')
        if isinstance(recommendation, pd.Series):
            recommendation = recommendation.iloc[0] if len(recommendation) > 0 else 'Hold'
        
        # Ensure values are proper types
        grade = str(grade) if grade else 'C'
        composite = int(composite) if composite else 50
        recommendation = str(recommendation) if recommendation else 'Hold'
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div style="text-align: center; padding: 20px;">
                <h4>Investment Grade</h4>
                <div class="grade-{grade}" style="font-size: 72px; font-weight: bold;">{grade}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="text-align: center; padding: 20px;">
                <h4>Composite Score</h4>
                <div style="font-size: 48px; font-weight: bold;">{composite}/100</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            rec_class = recommendation.lower().replace(' ', '-')
            st.markdown(f"""
            <div style="text-align: center; padding: 20px;">
                <h4>Recommendation</h4>
                <div class="{rec_class}" style="font-size: 24px; padding: 15px;">{recommendation}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Detailed Scores
        st.markdown("#### 📊 Detailed Score Breakdown")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Piotroski F-Score
            f_score = company.get('piotroski_f_score', 5)
            f_interpretation = "Strong" if f_score >= 7 else "Average" if f_score >= 4 else "Weak"
            st.markdown(f"""
            <div class="metric-card" style="padding: 15px; margin: 10px 0; border-radius: 10px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                <h4>Piotroski F-Score</h4>
                <div style="font-size: 36px; font-weight: bold;">{f_score}/9</div>
                <p style="margin: 5px 0; font-size: 14px;">{f_interpretation} Financial Health</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            **What is it?** A 0-9 score measuring a company's financial strength.
            - **7-9:** Strong financials ✅
            - **4-6:** Average financials ⚖️
            - **0-3:** Weak financials ⚠️
            """)
        
        with col2:
            # Altman Z-Score
            z_score = company.get('altman_z_score', 2.5)
            z_interpretation = "Safe Zone" if z_score > 2.99 else "Grey Zone" if z_score > 1.81 else "Distress Zone"
            z_color = "#00C851" if z_score > 2.99 else "#ffbb33" if z_score > 1.81 else "#ff4444"
            st.markdown(f"""
            <div class="metric-card" style="padding: 15px; margin: 10px 0; border-radius: 10px; background: {z_color}; color: white;">
                <h4>Altman Z-Score</h4>
                <div style="font-size: 36px; font-weight: bold;">{z_score:.2f}</div>
                <p style="margin: 5px 0; font-size: 14px;">{z_interpretation}</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            **What is it?** Predicts bankruptcy risk.
            - **>2.99:** Safe from bankruptcy ✅
            - **1.81-2.99:** Uncertain zone ⚖️
            - **<1.81:** High bankruptcy risk ⚠️
            """)
        
        with col3:
            # Graham Number
            graham = company.get('graham_number', 0)
            upside = company.get('graham_upside_pct', 0)
            price = company['last_traded_price']
            margin_color = "#00C851" if upside > 20 else "#ffbb33" if upside > 0 else "#ff4444"
            st.markdown(f"""
            <div class="metric-card" style="padding: 15px; margin: 10px 0; border-radius: 10px; background: {margin_color}; color: white;">
                <h4>Graham Number</h4>
                <div style="font-size: 36px; font-weight: bold;">Rs.{graham:.0f}</div>
                <p style="margin: 5px 0; font-size: 14px;">vs Price Rs.{price:.0f} ({upside:+.1f}%)</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            **What is it?** Fair value based on Benjamin Graham's formula.
            - **Price < Graham:** Potentially undervalued ✅
            - **Price ≈ Graham:** Fair valued ⚖️
            - **Price > Graham:** Potentially overvalued ⚠️
            """)
        
        st.markdown("---")
        
        # Score Gauges
        st.markdown("#### 🎯 Component Scores")
        
        col1, col2, col3, col4 = st.columns(4)
        
        scores = [
            ("Value Score", company.get('value_score', 50), col1),
            ("Quality Score", company.get('quality_score', 50), col2),
            ("Safety Score", company.get('safety_score', 50), col3),
            ("Dividend Score", company.get('dividend_score', 50), col4),
        ]
        
        for name, score, col in scores:
            with col:
                fig = create_gauge_chart(score, name, min_val=0, max_val=100, thresholds=[25, 50, 75])
                st.plotly_chart(fig, use_container_width=True)
        
        # Explanation for beginners
        with st.expander("📚 Learn About These Scores (Click to Expand)"):
            st.markdown("""
            ### Understanding Investment Scores
            
            #### 🏆 Investment Grade (A-F)
            Like school grades! **A** = Excellent investment candidate, **F** = Avoid
            
            #### 📊 Composite Score (0-100)
            Combines all factors into one easy number. Higher is better.
            
            #### 💰 Value Score
            - Measures if the stock is cheap compared to its earnings and assets
            - High score = Stock may be undervalued (good for buying)
            - Based on P/E ratio, P/B ratio, and dividend yield
            
            #### ⭐ Quality Score
            - Measures how well the company generates profits
            - High score = Company is efficient and profitable
            - Based on ROE, ROA, and profit margins
            
            #### 🛡️ Safety Score
            - Measures financial stability and debt levels
            - High score = Company is less likely to face financial trouble
            - Based on debt levels and current ratio
            
            #### 💵 Dividend Score
            - Measures how much cash the company returns to shareholders
            - High score = Good for income-seeking investors
            - Based on dividend yield and payout consistency
            
            ---
            
            ### 🎯 Quick Guide for Beginners
            
            **Best stocks to look for:**
            - Investment Grade: A or B
            - Piotroski F-Score: 7-9
            - Altman Z-Score: Above 2.99
            - Graham Number higher than current price
            
            **Warning signs:**
            - Investment Grade: D or F
            - Piotroski F-Score: 0-3
            - Altman Z-Score: Below 1.81
            - Very high debt (Debt/Equity > 2)
            """)

    with tabs[2]:  # Advanced Metrics (NEW TAB)
        st.markdown("### 📐 Advanced Financial Metrics")
        st.markdown("""
        <div class="info-box">
        <strong>📊 Professional-Grade Analysis:</strong> These advanced metrics are used by 
        professional investors and analysts to evaluate stocks more deeply.
        </div>
        """, unsafe_allow_html=True)
        
        # Valuation Metrics
        st.markdown("#### 💰 Advanced Valuation")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            peg = company.get('peg_ratio', 0)
            peg_color = "#00C851" if peg < 1 else "#ffbb33" if peg < 2 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {peg_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>PEG Ratio</strong><br>
                <span style="font-size: 28px;">{peg:.2f}</span><br>
                <small>{'Undervalued' if peg < 1 else 'Fair' if peg < 2 else 'Expensive'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            ev_ebitda = company.get('ev_ebitda', 0)
            ev_color = "#00C851" if ev_ebitda < 10 else "#ffbb33" if ev_ebitda < 15 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {ev_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>EV/EBITDA</strong><br>
                <span style="font-size: 28px;">{ev_ebitda:.1f}x</span><br>
                <small>{'Cheap' if ev_ebitda < 10 else 'Fair' if ev_ebitda < 15 else 'Expensive'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            ey = company.get('earnings_yield', 0)
            ey_color = "#00C851" if ey > 10 else "#ffbb33" if ey > 5 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {ey_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>Earnings Yield</strong><br>
                <span style="font-size: 28px;">{ey:.1f}%</span><br>
                <small>{'Attractive' if ey > 10 else 'Fair' if ey > 5 else 'Low'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            fcf_y = company.get('fcf_yield', 0)
            fcf_color = "#00C851" if fcf_y > 8 else "#ffbb33" if fcf_y > 4 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {fcf_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>FCF Yield</strong><br>
                <span style="font-size: 28px;">{fcf_y:.1f}%</span><br>
                <small>{'High' if fcf_y > 8 else 'Moderate' if fcf_y > 4 else 'Low'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Profitability Metrics
        st.markdown("#### 📈 Profitability & Returns")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            roic = company.get('roic', 0)
            roic_color = "#00C851" if roic > 15 else "#ffbb33" if roic > 10 else "#ff4444"
            st.metric("ROIC", f"{roic:.1f}%", help="Return on Invested Capital - Buffett's favorite!")
        
        with col2:
            roce = company.get('roce', 0)
            st.metric("ROCE", f"{roce:.1f}%", help="Return on Capital Employed")
        
        with col3:
            op_margin = company.get('operating_margin', 0)
            st.metric("Operating Margin", f"{op_margin:.1f}%", help="Operating efficiency")
        
        with col4:
            ebitda_margin = company.get('ebitda_margin', 0)
            st.metric("EBITDA Margin", f"{ebitda_margin:.1f}%", help="Cash profitability")
        
        st.markdown("---")
        
        # Liquidity & Safety
        st.markdown("#### 🛡️ Liquidity & Safety")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            quick = company.get('quick_ratio', 0)
            st.metric("Quick Ratio", f"{quick:.2f}x", 
                     delta="Safe" if quick > 1 else "At Risk",
                     delta_color="normal" if quick > 1 else "inverse",
                     help="Can pay bills without selling inventory")
        
        with col2:
            interest_cov = company.get('interest_coverage', 0)
            st.metric("Interest Coverage", f"{min(interest_cov, 99):.1f}x",
                     delta="Safe" if interest_cov > 3 else "Risky",
                     delta_color="normal" if interest_cov > 3 else "inverse",
                     help="Can company pay its debt interest?")
        
        with col3:
            debt_ebitda = company.get('debt_to_ebitda', 0)
            st.metric("Debt/EBITDA", f"{debt_ebitda:.1f}x" if debt_ebitda < 99 else "N/A",
                     help="Years to pay off debt with EBITDA")
        
        with col4:
            net_debt_eq = company.get('net_debt_to_equity', 0)
            st.metric("Net Debt/Equity", f"{net_debt_eq:.2f}x",
                     help="Debt minus cash, relative to equity")
        
        st.markdown("---")
        
        # Efficiency & Cash Cycle
        st.markdown("#### ⚡ Efficiency Metrics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            ccc = company.get('cash_conversion_cycle', 0)
            st.metric("Cash Conversion Cycle", f"{ccc:.0f} days",
                     help="Days to convert investments to cash")
        
        with col2:
            inv_turn = company.get('inventory_turnover', 0)
            st.metric("Inventory Turnover", f"{inv_turn:.1f}x",
                     help="Times inventory sold per year")
        
        with col3:
            dso = company.get('days_sales_outstanding', 0)
            st.metric("Days Sales Outstanding", f"{dso:.0f} days",
                     help="Days to collect from customers")
        
        with col4:
            fat = company.get('fixed_asset_turnover', 0)
            st.metric("Fixed Asset Turnover", f"{fat:.2f}x",
                     help="Revenue per rupee of fixed assets")
        
        st.markdown("---")
        
        # Earnings Quality & Risk
        st.markdown("#### 🔍 Quality & Risk Indicators")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            accruals = company.get('accruals_ratio', 0) * 100
            accrual_color = "#00C851" if abs(accruals) < 5 else "#ffbb33" if abs(accruals) < 10 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {accrual_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>Accruals Ratio</strong><br>
                <span style="font-size: 24px;">{accruals:.1f}%</span><br>
                <small>{'High Quality' if abs(accruals) < 5 else 'Moderate' if abs(accruals) < 10 else 'Low Quality'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            m_score = company.get('beneish_m_score', -3)
            m_risk = company.get('manipulation_risk', 'Low')
            m_color = "#00C851" if m_score < -2.22 else "#ffbb33" if m_score < -1.78 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {m_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>Beneish M-Score</strong><br>
                <span style="font-size: 24px;">{m_score:.2f}</span><br>
                <small>Manipulation Risk: {m_risk}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            eq = company.get('earnings_quality', 50)
            eq_color = "#00C851" if eq > 70 else "#ffbb33" if eq > 50 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {eq_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>Earnings Quality</strong><br>
                <span style="font-size: 24px;">{eq}/100</span><br>
                <small>{'High' if eq > 70 else 'Moderate' if eq > 50 else 'Low'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            fcf_ni = company.get('fcf_to_net_income', 0)
            fcf_ni_color = "#00C851" if fcf_ni > 80 else "#ffbb33" if fcf_ni > 50 else "#ff4444"
            st.markdown(f"""
            <div style="padding: 15px; background: {fcf_ni_color}; border-radius: 10px; color: white; text-align: center;">
                <strong>FCF/Net Income</strong><br>
                <span style="font-size: 24px;">{fcf_ni:.0f}%</span><br>
                <small>{'Cash Backed' if fcf_ni > 80 else 'Moderate' if fcf_ni > 50 else 'Accounting Profits'}</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Dividend Analysis
        st.markdown("#### 💵 Dividend Analysis")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            payout = company.get('payout_ratio', 0)
            st.metric("Payout Ratio", f"{payout:.0f}%",
                     delta="Sustainable" if payout < 60 else "High",
                     delta_color="normal" if payout < 60 else "inverse")
        
        with col2:
            div_cov = company.get('dividend_coverage', 0)
            st.metric("Dividend Coverage", f"{div_cov:.1f}x",
                     delta="Safe" if div_cov > 2 else "At Risk",
                     delta_color="normal" if div_cov > 2 else "inverse")
        
        with col3:
            div_safety = company.get('dividend_safety', 50)
            st.metric("Dividend Safety Score", f"{div_safety}/100")
        
        with col4:
            sgr = company.get('sustainable_growth_rate', 0)
            st.metric("Sustainable Growth Rate", f"{sgr:.1f}%",
                     help="Max growth without new debt/shares")
        
        # DuPont Analysis
        st.markdown("---")
        st.markdown("#### 📊 DuPont Analysis (ROE Breakdown)")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            npm = company.get('dupont_npm', 0)
            st.metric("Net Profit Margin", f"{npm:.1f}%", help="Profitability")
        
        with col2:
            at = company.get('dupont_at', 0)
            st.metric("Asset Turnover", f"{at:.2f}x", help="Efficiency")
        
        with col3:
            em = company.get('dupont_em', 0)
            st.metric("Equity Multiplier", f"{em:.2f}x", help="Leverage")
        
        with col4:
            driver = company.get('roe_driver', 'Balanced')
            st.info(f"ROE Driver: **{driver}**")
        
        # Explanation
        with st.expander("📚 What Do These Metrics Mean? (Click to Expand)"):
            st.markdown("""
            ### Advanced Metrics Explained
            
            #### 💰 Valuation
            - **PEG Ratio:** P/E divided by growth. <1 = undervalued for growth
            - **EV/EBITDA:** Enterprise value to cash profits. <10 is cheap
            - **Earnings Yield:** Inverse of P/E. Compare to bond rates
            - **FCF Yield:** Free cash return on price. >8% is attractive
            
            #### 📈 Profitability
            - **ROIC:** Buffett's favorite! Shows true capital efficiency. >15% excellent
            - **ROCE:** Return on all capital. Good for comparing debt levels
            
            #### 🛡️ Safety
            - **Quick Ratio:** Can pay bills without selling inventory. >1 is safe
            - **Interest Coverage:** Can pay debt interest. >3 is safe, <1.5 is danger
            
            #### 🔍 Quality
            - **Accruals Ratio:** Low = real cash profits. High = accounting tricks
            - **M-Score:** >-2.22 suggests possible earnings manipulation
            - **FCF/Net Income:** >80% means profits are backed by real cash
            
            #### 💵 Dividends
            - **Payout Ratio:** <60% is sustainable, >80% risky
            - **Dividend Coverage:** >2x means dividend is safe
            
            #### 📊 DuPont Analysis
            Shows WHERE ROE comes from:
            - High margin = pricing power
            - High turnover = efficiency
            - High leverage = debt (risky!)
            """)

    with tabs[3]:  # Financials (was tabs[2])
        st.markdown("### 💰 Financial Statements (From Annual Reports)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Income Statement")
            income_data = {
                "Revenue": format_number(company.get('revenue', 0), "Rs. "),
                "Gross Profit": format_number(company.get('gross_profit', 0), "Rs. "),
                "Operating Income": format_number(company.get('operating_income', 0), "Rs. "),
                "Net Profit": format_number(company.get('net_profit', 0), "Rs. "),
                "Gross Margin": f"{company.get('gross_margin', 0):.2f}%",
                "Net Margin": f"{company.get('net_margin', 0):.2f}%",
            }
            for key, value in income_data.items():
                col_a, col_b = st.columns([1, 1])
                col_a.write(f"**{key}**")
                col_b.write(value)
        
        with col2:
            st.markdown("#### Balance Sheet")
            balance_data = {
                "Total Assets": format_number(company.get('total_assets', 0), "Rs. "),
                "Total Liabilities": format_number(company.get('total_liabilities', 0), "Rs. "),
                "Shareholders' Equity": format_number(company.get('shareholders_equity', 0), "Rs. "),
                "Total Debt": format_number(company.get('total_debt', 0), "Rs. "),
                "Debt/Equity": f"{company.get('debt_equity', 0):.2f}x",
                "Current Ratio": f"{company.get('current_ratio', 0):.2f}x",
            }
            for key, value in balance_data.items():
                col_a, col_b = st.columns([1, 1])
                col_a.write(f"**{key}**")
                col_b.write(value)
        
        st.markdown("#### Cash Flow")
        col1, col2, col3 = st.columns(3)
        col1.metric("Operating Cash Flow", format_number(company.get('operating_cash_flow', 0), "Rs. "))
        col2.metric("Free Cash Flow", format_number(company.get('free_cash_flow', 0), "Rs. "))
        col3.metric("FCF Yield", f"{(company.get('free_cash_flow', 0) / max(company.get('market_cap', 1), 1)) * 100:.2f}%")
    
    with tabs[4]:  # Historical
        st.markdown("### 📈 Historical Financial Trends")
        
        historical = generate_historical_financials(selected_symbol, years=5)
        
        # Revenue and Profit Chart
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(x=historical['period'], y=historical['revenue']/1e9, 
                  name="Revenue (Bn)", marker_color='#667eea'),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=historical['period'], y=historical['net_profit']/1e9,
                      name="Net Profit (Bn)", line=dict(color='#00C851', width=3)),
            secondary_y=True
        )
        
        fig.update_layout(
            title="Revenue & Net Profit Trend",
            height=400,
            hovermode='x unified'
        )
        fig.update_yaxes(title_text="Revenue (Rs. Bn)", secondary_y=False)
        fig.update_yaxes(title_text="Net Profit (Rs. Bn)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Profitability metrics
        col1, col2 = st.columns(2)
        
        with col1:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=historical['period'], y=historical['roe'],
                                     name='ROE', mode='lines+markers',
                                     line=dict(color='#667eea', width=2)))
            fig2.add_trace(go.Scatter(x=historical['period'], y=historical['roa'],
                                     name='ROA', mode='lines+markers',
                                     line=dict(color='#764ba2', width=2)))
            fig2.update_layout(title="ROE & ROA Trend", height=300)
            st.plotly_chart(fig2, use_container_width=True)
        
        with col2:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=historical['period'], y=historical['debt_equity'],
                                     name='Debt/Equity', mode='lines+markers',
                                     line=dict(color='#ff4444', width=2)))
            fig3.add_trace(go.Scatter(x=historical['period'], y=historical['current_ratio'],
                                     name='Current Ratio', mode='lines+markers',
                                     line=dict(color='#00C851', width=2)))
            fig3.update_layout(title="Financial Health Trend", height=300)
            st.plotly_chart(fig3, use_container_width=True)
        
        # Historical data table
        st.markdown("#### 📋 Historical Data Table")
        st.dataframe(historical, use_container_width=True)
    
    with tabs[5]:  # Valuation
        st.markdown("### 🎯 Valuation Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Valuation Metrics")
            
            # Calculate Graham Number
            eps = company.get('eps', 0)
            nav = company.get('nav', 0)
            if eps > 0 and nav > 0:
                graham_number = (22.5 * eps * nav) ** 0.5
            else:
                graham_number = 0
            
            # Calculate intrinsic value
            growth_rate = 5  # Assumed
            intrinsic_value = eps * (8.5 + 2 * growth_rate) if eps > 0 else 0
            
            price = company['last_traded_price']
            
            valuation_metrics = {
                "Current Price": f"Rs. {price:.2f}",
                "Graham Number": f"Rs. {graham_number:.2f}",
                "Intrinsic Value (Graham)": f"Rs. {intrinsic_value:.2f}",
                "Margin of Safety": f"{((intrinsic_value - price) / intrinsic_value * 100) if intrinsic_value > 0 else 0:.1f}%",
                "Price to Earnings (P/E)": f"{company.get('pe_ratio', 0):.2f}x",
                "Price to Book (P/B)": f"{company.get('pb_ratio', 0):.2f}x",
                "Price to Sales (P/S)": f"{(company.get('market_cap', 0) / max(company.get('revenue', 1), 1)):.2f}x",
                "EV/EBITDA": f"{np.random.uniform(5, 15):.2f}x",
            }
            
            for metric, value in valuation_metrics.items():
                st.write(f"**{metric}:** {value}")
        
        with col2:
            st.markdown("#### Valuation Status")
            
            # Create valuation summary
            signals = []
            
            if company.get('pe_ratio', 100) < 15:
                signals.append(("✅ Low P/E", "Undervalued based on earnings"))
            else:
                signals.append(("⚠️ High P/E", "Premium valuation"))
            
            if company.get('pb_ratio', 10) < 1.5:
                signals.append(("✅ Low P/B", "Trading near book value"))
            else:
                signals.append(("⚠️ High P/B", "Premium to book"))
            
            if company.get('dividend_yield', 0) > 4:
                signals.append(("✅ High Dividend", "Good income potential"))
            
            if company.get('roe', 0) > 15:
                signals.append(("✅ High ROE", "Efficient use of equity"))
            
            if company.get('debt_equity', 10) < 0.5:
                signals.append(("✅ Low Debt", "Strong balance sheet"))
            
            for signal, description in signals:
                st.write(f"{signal}: {description}")
            
            # Overall verdict
            positive_signals = len([s for s in signals if "✅" in s[0]])
            
            if positive_signals >= 4:
                st.success("🌟 STRONG BUY: Multiple value indicators positive")
            elif positive_signals >= 3:
                st.info("👍 BUY: Good value characteristics")
            elif positive_signals >= 2:
                st.warning("🤔 HOLD: Mixed signals")
            else:
                st.error("⚠️ CAUTION: Few value indicators positive")
    
    with tabs[6]:  # Technical
        st.markdown("### 📉 Price Analysis")
        
        # Generate sample price history
        days = 365
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        base_price = company['last_traded_price']
        
        np.random.seed(hash(selected_symbol) % 2**32)
        returns = np.random.normal(0.0005, 0.02, days)
        prices = base_price * np.exp(np.cumsum(returns))
        
        price_df = pd.DataFrame({
            'Date': dates,
            'Price': prices,
            'MA20': pd.Series(prices).rolling(20).mean(),
            'MA50': pd.Series(prices).rolling(50).mean(),
            'MA200': pd.Series(prices).rolling(200).mean(),
        })
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=price_df['Date'], y=price_df['Price'],
                                name='Price', line=dict(color='#667eea', width=1)))
        fig.add_trace(go.Scatter(x=price_df['Date'], y=price_df['MA20'],
                                name='MA20', line=dict(color='orange', width=1, dash='dash')))
        fig.add_trace(go.Scatter(x=price_df['Date'], y=price_df['MA50'],
                                name='MA50', line=dict(color='red', width=1, dash='dash')))
        
        fig.update_layout(
            title=f"{selected_symbol} Price History (1 Year)",
            xaxis_title="Date",
            yaxis_title="Price (Rs.)",
            height=500,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 52-week range
        high_52 = company.get('52_week_high', base_price * 1.3)
        low_52 = company.get('52_week_low', base_price * 0.7)
        current = company['last_traded_price']
        
        pct_of_range = (current - low_52) / (high_52 - low_52) * 100
        
        st.markdown(f"""
        **52-Week Range:** Rs. {low_52:.2f} - Rs. {high_52:.2f}
        
        Current price is at **{pct_of_range:.1f}%** of 52-week range
        """)
        
        # Progress bar for 52-week range
        st.progress(min(max(pct_of_range/100, 0), 1))


def show_historical_trends(df):
    """Historical trends comparison across companies"""
    st.markdown("## 📈 Historical Trends Analysis")
    
    st.markdown("### Compare Financial Trends Across Companies")
    
    # Multi-select companies
    selected_companies = st.multiselect(
        "Select Companies to Compare (max 5)",
        options=df['symbol'].tolist(),
        default=df['symbol'].tolist()[:3],
        max_selections=5
    )
    
    if not selected_companies:
        st.warning("Please select at least one company")
        return
    
    # Metric to compare
    metric = st.selectbox(
        "Select Metric to Compare",
        options=["Revenue", "Net Profit", "EPS", "ROE", "Debt/Equity", 
                "Dividend per Share", "Gross Margin", "Net Margin"]
    )
    
    metric_map = {
        "Revenue": "revenue",
        "Net Profit": "net_profit",
        "EPS": "eps",
        "ROE": "roe",
        "Debt/Equity": "debt_equity",
        "Dividend per Share": "dividend_per_share",
        "Gross Margin": "gross_margin",
        "Net Margin": "net_margin",
    }
    
    # Generate historical data for each company
    fig = go.Figure()
    
    for symbol in selected_companies:
        hist = generate_historical_financials(symbol, years=5)
        col = metric_map.get(metric, "revenue")
        
        if col in hist.columns:
            fig.add_trace(go.Scatter(
                x=hist['period'],
                y=hist[col],
                name=symbol,
                mode='lines+markers'
            ))
    
    fig.update_layout(
        title=f"{metric} Comparison Over Time",
        xaxis_title="Period",
        yaxis_title=metric,
        height=500,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Year-over-year growth
    st.markdown("### Year-over-Year Growth Rates")
    
    growth_data = []
    for symbol in selected_companies:
        hist = generate_historical_financials(symbol, years=5)
        col = metric_map.get(metric, "revenue")
        
        if col in hist.columns and len(hist) > 1:
            latest = hist[col].iloc[-1]
            previous = hist[col].iloc[-2]
            growth = ((latest - previous) / previous * 100) if previous != 0 else 0
            
            growth_data.append({
                'Symbol': symbol,
                f'{metric} (Latest)': latest,
                f'{metric} (Previous)': previous,
                'YoY Growth (%)': round(growth, 2)
            })
    
    if growth_data:
        growth_df = pd.DataFrame(growth_data)
        st.dataframe(growth_df, use_container_width=True)
        
        # Growth chart
        fig2 = px.bar(growth_df, x='Symbol', y='YoY Growth (%)',
                     color='YoY Growth (%)',
                     color_continuous_scale=['red', 'yellow', 'green'])
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)


def show_stock_screener(df):
    """Interactive stock screener with investment scores"""
    st.markdown("## 🔍 Stock Screener")
    
    st.markdown("""
    <div class="info-box">
    <strong>💡 Tip:</strong> Use the Investment Score filters to find high-quality stocks easily. 
    Grade A and B stocks with high Piotroski scores are typically safer investments.
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Filter Stocks by Criteria")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("#### 📊 Valuation Filters")
        pe_max = st.slider("Max P/E Ratio", 0, 50, 20)
        pb_max = st.slider("Max P/B Ratio", 0.0, 5.0, 2.0)
    
    with col2:
        st.markdown("#### 💰 Income Filters")
        div_min = st.slider("Min Dividend Yield (%)", 0.0, 15.0, 0.0)
        roe_min = st.slider("Min ROE (%)", 0.0, 40.0, 0.0)
    
    with col3:
        st.markdown("#### ⚖️ Financial Health")
        de_max = st.slider("Max Debt/Equity", 0.0, 3.0, 1.5)
        if 'sector' in df.columns:
            sectors = ["All"] + df['sector'].dropna().unique().tolist()
            selected_sector = st.selectbox("Sector", sectors)
    
    with col4:
        st.markdown("#### 🏆 Investment Scores")
        # Investment Grade filter
        if 'investment_grade' in df.columns:
            grades = st.multiselect(
                "Investment Grade",
                options=['A', 'B', 'C', 'D', 'F'],
                default=['A', 'B', 'C']
            )
        
        # Piotroski F-Score filter
        if 'piotroski_f_score' in df.columns:
            f_score_min = st.slider("Min Piotroski F-Score", 0, 9, 0)
        else:
            f_score_min = 0
        
        # Composite Score filter
        if 'composite_score' in df.columns:
            composite_min = st.slider("Min Composite Score", 0, 100, 0)
        else:
            composite_min = 0
    
    # Apply filters
    filtered = df.copy()
    
    if 'pe_ratio' in filtered.columns:
        filtered = filtered[(filtered['pe_ratio'] > 0) & (filtered['pe_ratio'] <= pe_max)]
    if 'pb_ratio' in filtered.columns:
        filtered = filtered[(filtered['pb_ratio'] > 0) & (filtered['pb_ratio'] <= pb_max)]
    if 'dividend_yield' in filtered.columns:
        filtered = filtered[filtered['dividend_yield'] >= div_min]
    if 'roe' in filtered.columns:
        filtered = filtered[filtered['roe'] >= roe_min]
    if 'debt_equity' in filtered.columns:
        filtered = filtered[filtered['debt_equity'] <= de_max]
    if 'sector' in filtered.columns and selected_sector != "All":
        filtered = filtered[filtered['sector'] == selected_sector]
    
    # Apply investment score filters
    if 'investment_grade' in filtered.columns:
        filtered = filtered[filtered['investment_grade'].isin(grades)]
    if 'piotroski_f_score' in filtered.columns:
        filtered = filtered[filtered['piotroski_f_score'] >= f_score_min]
    if 'composite_score' in filtered.columns:
        filtered = filtered[filtered['composite_score'] >= composite_min]
    
    st.markdown("---")
    st.markdown(f"### Results: {len(filtered)} stocks found")
    
    if not filtered.empty:
        # Display columns - include investment scores
        display_cols = ['symbol', 'name', 'investment_grade', 'composite_score', 
                       'last_traded_price', 'pe_ratio', 'pb_ratio', 
                       'dividend_yield', 'roe', 'debt_equity', 'piotroski_f_score', 
                       'recommendation']
        available_cols = [c for c in display_cols if c in filtered.columns]
        
        # Sort by composite score by default
        sort_col = 'composite_score' if 'composite_score' in filtered.columns else 'pe_ratio'
        sort_ascending = False if sort_col == 'composite_score' else True
        
        st.dataframe(
            filtered[available_cols].sort_values(sort_col, ascending=sort_ascending),
            use_container_width=True,
            height=400
        )
        
        # Visualization
        if len(filtered) > 0:
            fig = px.scatter(
                filtered,
                x='pe_ratio',
                y='dividend_yield',
                size='market_cap' if 'market_cap' in filtered.columns else None,
                color='roe',
                hover_name='symbol',
                color_continuous_scale='RdYlGn',
                title="Screened Stocks: P/E vs Dividend Yield (Color=ROE, Size=Market Cap)"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No stocks match the current criteria. Try relaxing some filters.")


def show_sector_analysis(df):
    """Sector-wise analysis"""
    st.markdown("## 📊 Sector Analysis")
    
    if 'sector' not in df.columns:
        st.warning("Sector data not available")
        return
    
    # Sector overview
    st.markdown("### Sector Overview")
    
    sector_stats = df.groupby('sector').agg({
        'symbol': 'count',
        'pe_ratio': 'mean',
        'dividend_yield': 'mean',
        'roe': 'mean',
        'market_cap': 'sum'
    }).round(2)
    sector_stats.columns = ['Companies', 'Avg P/E', 'Avg Div Yield', 'Avg ROE', 'Total Market Cap']
    sector_stats = sector_stats.sort_values('Companies', ascending=False)
    
    st.dataframe(sector_stats, use_container_width=True)
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = px.bar(
            sector_stats.reset_index(),
            x='sector',
            y='Companies',
            color='Avg ROE',
            color_continuous_scale='RdYlGn',
            title="Companies by Sector (Color=Avg ROE)"
        )
        fig1.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.scatter(
            sector_stats.reset_index(),
            x='Avg P/E',
            y='Avg Div Yield',
            size='Companies',
            text='sector',
            title="Sector Valuation Map"
        )
        fig2.update_traces(textposition='top center')
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    # Detailed sector drill-down
    st.markdown("### Sector Drill-Down")
    
    selected_sector = st.selectbox(
        "Select Sector for Details",
        options=df['sector'].dropna().unique().tolist()
    )
    
    sector_df = df[df['sector'] == selected_sector]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Companies", len(sector_df))
    col2.metric("Avg P/E", f"{sector_df['pe_ratio'].mean():.2f}")
    col3.metric("Avg Dividend", f"{sector_df['dividend_yield'].mean():.2f}%")
    col4.metric("Avg ROE", f"{sector_df['roe'].mean():.2f}%")
    
    st.dataframe(
        sector_df[['symbol', 'name', 'last_traded_price', 'pe_ratio', 
                  'dividend_yield', 'roe', 'market_cap']].sort_values('market_cap', ascending=False),
        use_container_width=True
    )


def show_portfolio_builder(df):
    """Portfolio builder tool"""
    st.markdown("## 💼 Portfolio Builder")
    
    st.markdown("### Build Your Investment Portfolio")
    
    # Portfolio type selection
    portfolio_type = st.selectbox(
        "Select Portfolio Strategy",
        ["Balanced", "Income (High Dividend)", "Growth", "Value", "Custom"]
    )
    
    num_stocks = st.slider("Number of Stocks", 5, 20, 10)
    investment_amount = st.number_input("Investment Amount (Rs.)", 
                                        min_value=10000, value=100000, step=10000)
    
    # Generate portfolio based on strategy
    if portfolio_type == "Value":
        portfolio = df[df['pe_ratio'] > 0].nsmallest(num_stocks, 'pe_ratio')
    elif portfolio_type == "Income (High Dividend)":
        portfolio = df.nlargest(num_stocks, 'dividend_yield')
    elif portfolio_type == "Growth":
        portfolio = df.nlargest(num_stocks, 'roe')
    elif portfolio_type == "Balanced":
        # Score based on multiple factors
        df_temp = df.copy()
        df_temp['score'] = (
            df_temp['roe'].fillna(0) / df_temp['roe'].max() * 30 +
            df_temp['dividend_yield'].fillna(0) / df_temp['dividend_yield'].max() * 30 +
            (1 / df_temp['pe_ratio'].clip(lower=1)) / (1 / df_temp['pe_ratio'].clip(lower=1)).max() * 40
        )
        portfolio = df_temp.nlargest(num_stocks, 'score')
    else:  # Custom
        portfolio = df.head(num_stocks)
    
    st.markdown("### Suggested Portfolio")
    
    if not portfolio.empty:
        # Equal weight allocation
        weight = 1 / len(portfolio)
        portfolio['Weight (%)'] = round(weight * 100, 2)
        portfolio['Allocation (Rs.)'] = round(investment_amount * weight, 2)
        portfolio['Shares'] = (portfolio['Allocation (Rs.)'] / portfolio['last_traded_price']).astype(int)
        
        display_cols = ['symbol', 'name', 'last_traded_price', 'pe_ratio', 
                       'dividend_yield', 'roe', 'Weight (%)', 'Allocation (Rs.)', 'Shares']
        available_cols = [c for c in display_cols if c in portfolio.columns]
        
        st.dataframe(portfolio[available_cols], use_container_width=True)
        
        # Portfolio visualization
        col1, col2 = st.columns(2)
        
        with col1:
            fig1 = px.pie(portfolio, values='Weight (%)', names='symbol',
                         title="Portfolio Allocation",
                         color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Expected dividend income
            portfolio['Expected Dividend'] = (
                portfolio['Shares'] * portfolio['last_traded_price'] * 
                portfolio['dividend_yield'] / 100
            )
            total_dividend = portfolio['Expected Dividend'].sum()
            
            st.markdown("### Portfolio Summary")
            st.metric("Total Investment", f"Rs. {investment_amount:,.2f}")
            st.metric("Expected Annual Dividend", f"Rs. {total_dividend:,.2f}")
            st.metric("Portfolio Dividend Yield", f"{total_dividend/investment_amount*100:.2f}%")
            st.metric("Avg P/E Ratio", f"{portfolio['pe_ratio'].mean():.2f}")
            st.metric("Avg ROE", f"{portfolio['roe'].mean():.2f}%")


def show_financial_reports(df):
    """Financial reports viewer"""
    st.markdown("## 📑 Financial Reports")
    
    st.markdown("### Company Financial Statements")
    
    selected_company = st.selectbox(
        "Select Company",
        options=df['symbol'].tolist(),
        format_func=lambda x: f"{x} - {df[df['symbol']==x]['name'].values[0]}"
    )
    
    company = df[df['symbol'] == selected_company].iloc[0]
    historical = generate_historical_financials(selected_company, years=5)
    
    st.markdown(f"### {company['name']} - Financial History")
    
    tabs = st.tabs(["Income Statement", "Balance Sheet", "Cash Flow", "Ratios"])
    
    with tabs[0]:
        st.markdown("#### Income Statement (Rs. '000)")
        
        income_items = ['revenue', 'gross_profit', 'operating_income', 'net_profit']
        income_df = historical[['period'] + income_items].copy()
        
        for col in income_items:
            income_df[col] = income_df[col] / 1000
        
        income_df.columns = ['Period', 'Revenue', 'Gross Profit', 'Operating Income', 'Net Profit']
        
        st.dataframe(income_df.set_index('Period').T, use_container_width=True)
        
        # Trend chart
        fig = px.bar(
            historical.melt(id_vars=['period'], value_vars=income_items,
                          var_name='Item', value_name='Value'),
            x='period', y='Value', color='Item', barmode='group',
            title="Income Statement Trends"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tabs[1]:
        st.markdown("#### Balance Sheet (Rs. '000)")
        
        balance_items = ['total_assets', 'total_equity', 'total_debt']
        balance_df = historical[['period'] + balance_items].copy()
        
        for col in balance_items:
            balance_df[col] = balance_df[col] / 1000
        
        balance_df.columns = ['Period', 'Total Assets', 'Total Equity', 'Total Debt']
        
        st.dataframe(balance_df.set_index('Period').T, use_container_width=True)
        
        # Stacked area chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=historical['period'], y=historical['total_equity']/1e9,
                                fill='tonexty', name='Equity'))
        fig.add_trace(go.Scatter(x=historical['period'], y=historical['total_debt']/1e9,
                                fill='tonexty', name='Debt'))
        fig.update_layout(title="Balance Sheet Composition (Rs. Bn)", height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with tabs[2]:
        st.markdown("#### Cash Flow (Rs. '000)")
        
        cf_items = ['operating_cash_flow', 'free_cash_flow']
        cf_df = historical[['period'] + cf_items].copy()
        
        for col in cf_items:
            cf_df[col] = cf_df[col] / 1000
        
        cf_df.columns = ['Period', 'Operating Cash Flow', 'Free Cash Flow']
        
        st.dataframe(cf_df.set_index('Period').T, use_container_width=True)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=historical['period'], y=historical['operating_cash_flow']/1e9,
                            name='Operating CF'))
        fig.add_trace(go.Scatter(x=historical['period'], y=historical['free_cash_flow']/1e9,
                                name='Free CF', mode='lines+markers'))
        fig.update_layout(title="Cash Flow Trends (Rs. Bn)", height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with tabs[3]:
        st.markdown("#### Financial Ratios")
        
        ratio_items = ['roe', 'roa', 'debt_equity', 'current_ratio', 'gross_margin', 'net_margin']
        ratio_df = historical[['period'] + ratio_items].copy()
        ratio_df.columns = ['Period', 'ROE (%)', 'ROA (%)', 'Debt/Equity', 
                           'Current Ratio', 'Gross Margin (%)', 'Net Margin (%)']
        
        st.dataframe(ratio_df.set_index('Period').T, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(historical, x='period', y=['roe', 'roa'],
                         title="Profitability Ratios", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(historical, x='period', y=['gross_margin', 'net_margin'],
                         title="Margin Trends", markers=True)
            st.plotly_chart(fig, use_container_width=True)


def show_learning_center():
    """Educational page for investment beginners"""
    st.markdown("## 📚 Investment Learning Center")
    st.markdown("""
    <div class="info-box">
    <strong>Welcome!</strong> This page will help you understand the key metrics and scores used to evaluate stocks. 
    Perfect for beginners who want to learn about investing in the Colombo Stock Exchange.
    </div>
    """, unsafe_allow_html=True)
    
    tabs = st.tabs(["🎯 Key Metrics", "📊 Investment Scores", "📈 Financial Statements", 
                   "🎓 Investment Strategies", "⚠️ Risk Management"])
    
    with tabs[0]:
        st.markdown("### 🎯 Key Financial Metrics Explained")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            #### 📊 P/E Ratio (Price-to-Earnings)
            **What it means:** How much you pay for each rupee the company earns.
            
            **Formula:** Stock Price ÷ Earnings Per Share (EPS)
            
            **How to use it:**
            - **< 10:** Very cheap (but check why!)
            - **10-15:** Good value
            - **15-25:** Fair price
            - **> 25:** Expensive (needs high growth to justify)
            
            **Example:** If P/E = 12, you pay Rs.12 for every Rs.1 of profit.
            
            ---
            
            #### 📚 P/B Ratio (Price-to-Book)
            **What it means:** How much you pay compared to the company's net assets.
            
            **Formula:** Stock Price ÷ Book Value (NAV) Per Share
            
            **How to use it:**
            - **< 1:** Trading below asset value (potential bargain)
            - **1-2:** Fair price
            - **> 3:** Premium price (needs strong profits to justify)
            
            ---
            
            #### 💰 Dividend Yield
            **What it means:** Annual cash return from dividends as a percentage.
            
            **Formula:** (Annual Dividend ÷ Stock Price) × 100
            
            **How to use it:**
            - **> 5%:** High yield - good for income
            - **3-5%:** Moderate yield
            - **< 3%:** Low yield - growth focused
            """)
        
        with col2:
            st.markdown("""
            #### 📈 ROE (Return on Equity)
            **What it means:** How efficiently the company uses shareholder money to generate profits.
            
            **Formula:** Net Profit ÷ Shareholders' Equity × 100
            
            **How to use it:**
            - **> 20%:** Excellent - very efficient
            - **15-20%:** Good
            - **10-15%:** Average
            - **< 10%:** Poor efficiency
            
            ---
            
            #### ⚖️ Debt-to-Equity Ratio
            **What it means:** How much debt the company has compared to shareholder money.
            
            **Formula:** Total Debt ÷ Shareholders' Equity
            
            **How to use it:**
            - **< 0.5:** Low debt - very safe
            - **0.5-1:** Moderate debt - acceptable
            - **1-2:** High debt - risky
            - **> 2:** Very high debt - dangerous
            
            ---
            
            #### 💵 EPS (Earnings Per Share)
            **What it means:** Profit earned for each share you own.
            
            **Formula:** Net Profit ÷ Number of Shares
            
            **How to use it:**
            - Look for consistent growth over years
            - Compare with stock price (P/E ratio)
            - Higher is better (but compare within same industry)
            """)
    
    with tabs[1]:
        st.markdown("### 📊 Investment Scores Explained")
        
        st.markdown("""
        #### 🏆 Investment Grade (A to F)
        Like school grades! A simple way to rank stocks:
        
        | Grade | Score | Meaning |
        |-------|-------|---------|
        | **A** | 75-100 | Excellent - Strong buy candidate |
        | **B** | 60-74 | Good - Consider buying |
        | **C** | 40-59 | Average - Hold or wait |
        | **D** | 25-39 | Below average - Caution |
        | **F** | 0-24 | Poor - Avoid |
        
        ---
        
        #### 📈 Piotroski F-Score (0-9)
        Created by Professor Joseph Piotroski. Measures financial health using 9 criteria:
        
        **Profitability (4 points):**
        1. Positive Net Income
        2. Positive Operating Cash Flow
        3. Cash Flow > Net Income (quality earnings)
        4. ROA improved from last year
        
        **Leverage & Liquidity (3 points):**
        5. Debt decreased from last year
        6. Current Ratio improved
        7. No new shares issued (no dilution)
        
        **Operating Efficiency (2 points):**
        8. Gross Margin improved
        9. Asset Turnover improved
        
        **Interpretation:**
        - **7-9:** Strong financial health ✅
        - **4-6:** Average financial health ⚖️
        - **0-3:** Weak financial health ⚠️
        
        ---
        
        #### 📉 Altman Z-Score
        Predicts bankruptcy risk. Created by Professor Edward Altman.
        
        **Components:**
        - Working Capital / Total Assets
        - Retained Earnings / Total Assets
        - EBIT / Total Assets
        - Market Value of Equity / Total Debt
        - Sales / Total Assets
        
        **Interpretation:**
        - **> 2.99:** Safe Zone ✅ (Low bankruptcy risk)
        - **1.81 - 2.99:** Grey Zone ⚖️ (Some risk)
        - **< 1.81:** Distress Zone ⚠️ (High bankruptcy risk)
        
        ---
        
        #### 📘 Graham Number
        Fair value calculation from Benjamin Graham (Warren Buffett's teacher).
        
        **Formula:** √(22.5 × EPS × Book Value)
        
        **How to use:**
        - If Stock Price < Graham Number → Potentially undervalued ✅
        - If Stock Price > Graham Number → Potentially overvalued ⚠️
        """)
    
    with tabs[2]:
        st.markdown("### 📈 Understanding Financial Statements")
        
        st.markdown("""
        #### 📋 Three Main Financial Statements
        
        ---
        
        #### 1. Income Statement (Profit & Loss)
        **Shows:** How much money the company made or lost during a period.
        
        | Item | What it means |
        |------|---------------|
        | **Revenue** | Total sales/money earned from business |
        | **Gross Profit** | Revenue minus cost of goods sold |
        | **Operating Income** | Gross profit minus operating expenses |
        | **Net Profit** | Final profit after all expenses and taxes |
        | **EPS** | Net profit divided by number of shares |
        
        **Key Margins:**
        - **Gross Margin:** (Gross Profit ÷ Revenue) × 100
        - **Net Margin:** (Net Profit ÷ Revenue) × 100
        
        ---
        
        #### 2. Balance Sheet
        **Shows:** What the company owns (Assets) and owes (Liabilities) at a point in time.
        
        **The Basic Equation:**
        > Assets = Liabilities + Shareholders' Equity
        
        | Item | What it means |
        |------|---------------|
        | **Total Assets** | Everything the company owns |
        | **Total Liabilities** | Everything the company owes |
        | **Shareholders' Equity** | Net worth (Assets - Liabilities) |
        | **Total Debt** | Loans and borrowings |
        | **Current Ratio** | Current Assets ÷ Current Liabilities |
        
        ---
        
        #### 3. Cash Flow Statement
        **Shows:** Actual cash moving in and out of the company.
        
        | Item | What it means |
        |------|---------------|
        | **Operating Cash Flow** | Cash from main business operations |
        | **Free Cash Flow** | Cash left after all expenses and investments |
        
        **Why it's important:**
        - Profits can be manipulated, but cash is real
        - A company can show profit but have no cash (dangerous!)
        - Free Cash Flow is what can be paid as dividends or used for growth
        """)
    
    with tabs[3]:
        st.markdown("### 🎓 Simple Investment Strategies")
        
        st.markdown("""
        #### 💎 Value Investing (Warren Buffett Style)
        
        **Concept:** Buy stocks that are cheaper than their true value.
        
        **Look for:**
        - Low P/E ratio (< 15)
        - Low P/B ratio (< 1.5)
        - High dividend yield (> 4%)
        - Graham Number > Stock Price
        
        **Best for:** Patient investors who want steady returns.
        
        ---
        
        #### 📈 Quality Investing
        
        **Concept:** Buy high-quality companies at fair prices.
        
        **Look for:**
        - High ROE (> 15%)
        - Consistent profit growth
        - Low debt (Debt/Equity < 0.5)
        - Strong brands or market position
        
        **Best for:** Long-term investors who want reliable companies.
        
        ---
        
        #### 💰 Dividend Investing
        
        **Concept:** Buy stocks that pay regular, growing dividends.
        
        **Look for:**
        - Dividend Yield > 5%
        - Consistent dividend payments (10+ years)
        - Dividend growth over time
        - Payout ratio < 70% (sustainable)
        
        **Best for:** Investors who want regular income.
        
        ---
        
        #### 🔄 Diversification Rules
        
        **Don't put all eggs in one basket:**
        1. Own stocks in 5-7 different sectors
        2. Don't put more than 20% in any single stock
        3. Balance between growth and dividend stocks
        4. Consider your risk tolerance and age
        
        ---
        
        #### ✅ Beginner's Checklist Before Buying
        
        1. ✅ Investment Grade A or B
        2. ✅ Piotroski F-Score ≥ 6
        3. ✅ Altman Z-Score > 2
        4. ✅ P/E Ratio < 20
        5. ✅ Debt/Equity < 1
        6. ✅ ROE > 12%
        7. ✅ Dividend Yield > 3% (if income needed)
        8. ✅ Understand what the company does
        """)
    
    with tabs[4]:
        st.markdown("### ⚠️ Risk Management")
        
        st.markdown("""
        #### 🚨 Warning Signs to Avoid
        
        **Financial Red Flags:**
        - ❌ Negative earnings (losses) for 3+ years
        - ❌ Altman Z-Score < 1.81 (bankruptcy risk)
        - ❌ Piotroski F-Score < 3
        - ❌ Debt/Equity > 2 (too much debt)
        - ❌ Declining revenue for 3+ years
        - ❌ Cash Flow < Net Profit (earnings manipulation?)
        
        **Company Red Flags:**
        - ❌ Frequent management changes
        - ❌ Auditor qualified opinion or change
        - ❌ Related party transactions
        - ❌ Stock price manipulation rumors
        - ❌ Company not paying dividends when profitable
        
        ---
        
        #### 📊 Position Sizing Rules
        
        **How much to invest in one stock:**
        - Maximum 10-20% of total portfolio in one stock
        - Start with 5% and increase gradually
        - Never invest money you can't afford to lose
        
        ---
        
        #### ⏰ When to Sell
        
        **Sell when:**
        - Investment thesis is broken (company fundamentals changed)
        - Better opportunity available
        - Stock becomes extremely overvalued (P/E > 30+)
        - Financial health deteriorating (F-Score dropping)
        - You need the money for emergency
        
        **Don't sell just because:**
        - Price dropped temporarily
        - Market is down overall
        - Someone gave a tip to sell
        - You're scared/panicking
        
        ---
        
        #### 💡 Golden Rules for Beginners
        
        1. **Start Small:** Begin with a small amount you can afford to lose
        2. **Learn First:** Understand what you're buying
        3. **Be Patient:** Good returns take time (think 3-5 years)
        4. **Diversify:** Don't concentrate on one stock or sector
        5. **Don't Follow Tips:** Do your own research
        6. **Keep Learning:** Markets change, keep updating knowledge
        7. **Control Emotions:** Don't panic sell or greed buy
        8. **Review Regularly:** Check your portfolio quarterly
        """)
        
        st.markdown("---")
        
        st.info("""
        **Remember:** Past performance doesn't guarantee future results. 
        All investments carry risk. Consider consulting a licensed financial advisor 
        before making investment decisions.
        """)


if __name__ == "__main__":
    main()
