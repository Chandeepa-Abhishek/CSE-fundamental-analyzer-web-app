"""
Dashboard utility functions and data loaders
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import sys

# Add parent directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA_DIR, RAW_DATA_DIR, REPORTS_DIR


class DataLoader:
    """Data loading utilities for the dashboard"""
    
    @staticmethod
    def load_company_data() -> pd.DataFrame:
        """Load the latest company data from various sources"""
        
        # Try pickle files first (fastest)
        pkl_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.pkl"), reverse=True)
        if pkl_files:
            return pd.read_pickle(pkl_files[0])
        
        # Try CSV files
        csv_files = sorted(PROCESSED_DATA_DIR.glob("cse_companies_*.csv"), reverse=True)
        if csv_files:
            return pd.read_csv(csv_files[0])
        
        # Try raw data
        raw_csv = sorted(RAW_DATA_DIR.glob("*.csv"), reverse=True)
        if raw_csv:
            return pd.read_csv(raw_csv[0])
        
        return pd.DataFrame()
    
    @staticmethod
    def load_pdf_extracted_data() -> dict:
        """Load data extracted from PDF annual reports"""
        
        json_files = sorted(RAW_DATA_DIR.glob("pdf_extracted_data_*.json"), reverse=True)
        if json_files:
            with open(json_files[0], 'r') as f:
                return json.load(f)
        
        return {}
    
    @staticmethod
    def load_historical_financials(symbol: str) -> pd.DataFrame:
        """Load historical financial data for a specific company"""
        
        # Look for company-specific files
        company_files = sorted(
            RAW_DATA_DIR.glob(f"{symbol}_financials_*.json"), 
            reverse=True
        )
        
        if company_files:
            with open(company_files[0], 'r') as f:
                return pd.DataFrame(json.load(f))
        
        # Look in combined PDF data
        pdf_data = DataLoader.load_pdf_extracted_data()
        if symbol in pdf_data:
            return pd.DataFrame(pdf_data[symbol])
        
        return pd.DataFrame()
    
    @staticmethod
    def load_screening_results(strategy: str = None) -> pd.DataFrame:
        """Load saved screening results"""
        
        pattern = f"screening_{strategy}_*.csv" if strategy else "screening_*.csv"
        files = sorted(REPORTS_DIR.glob(pattern), reverse=True)
        
        if files:
            return pd.read_csv(files[0])
        
        return pd.DataFrame()
    
    @staticmethod
    def get_available_reports() -> list:
        """Get list of available report files"""
        
        reports = []
        
        for ext in ['xlsx', 'csv', 'pdf']:
            files = REPORTS_DIR.glob(f"*.{ext}")
            for f in files:
                reports.append({
                    'filename': f.name,
                    'path': str(f),
                    'type': ext.upper(),
                    'modified': datetime.fromtimestamp(f.stat().st_mtime)
                })
        
        return sorted(reports, key=lambda x: x['modified'], reverse=True)


class MetricsCalculator:
    """Calculate various financial metrics"""
    
    @staticmethod
    def calculate_graham_number(eps: float, book_value: float) -> float:
        """Calculate Graham Number (intrinsic value based on Benjamin Graham)"""
        if eps > 0 and book_value > 0:
            return (22.5 * eps * book_value) ** 0.5
        return 0
    
    @staticmethod
    def calculate_intrinsic_value_graham(eps: float, growth_rate: float = 5) -> float:
        """Calculate intrinsic value using Graham formula"""
        if eps > 0:
            return eps * (8.5 + 2 * growth_rate)
        return 0
    
    @staticmethod
    def calculate_dcf_value(free_cash_flow: float, 
                           growth_rate: float = 0.05,
                           discount_rate: float = 0.10,
                           terminal_growth: float = 0.02,
                           years: int = 5,
                           shares_outstanding: float = 1) -> float:
        """Calculate DCF intrinsic value per share"""
        
        if free_cash_flow <= 0 or shares_outstanding <= 0:
            return 0
        
        # Project future cash flows
        pv_cash_flows = 0
        for year in range(1, years + 1):
            future_cf = free_cash_flow * (1 + growth_rate) ** year
            pv_cash_flows += future_cf / (1 + discount_rate) ** year
        
        # Terminal value
        terminal_cf = free_cash_flow * (1 + growth_rate) ** years * (1 + terminal_growth)
        terminal_value = terminal_cf / (discount_rate - terminal_growth)
        pv_terminal = terminal_value / (1 + discount_rate) ** years
        
        enterprise_value = pv_cash_flows + pv_terminal
        
        return enterprise_value / shares_outstanding
    
    @staticmethod
    def calculate_peg_ratio(pe_ratio: float, growth_rate: float) -> float:
        """Calculate PEG Ratio"""
        if pe_ratio > 0 and growth_rate > 0:
            return pe_ratio / growth_rate
        return 0
    
    @staticmethod
    def calculate_margin_of_safety(current_price: float, 
                                   intrinsic_value: float) -> float:
        """Calculate margin of safety percentage"""
        if intrinsic_value > 0:
            return ((intrinsic_value - current_price) / intrinsic_value) * 100
        return 0
    
    @staticmethod
    def calculate_composite_score(company_data: dict,
                                  weights: dict = None) -> float:
        """Calculate composite investment score"""
        
        if weights is None:
            weights = {
                'value': 0.25,
                'profitability': 0.25,
                'financial_health': 0.20,
                'dividend': 0.15,
                'growth': 0.15
            }
        
        scores = {}
        
        # Value score (lower P/E and P/B = higher score)
        pe = company_data.get('pe_ratio', 20)
        pb = company_data.get('pb_ratio', 2)
        scores['value'] = min(100, max(0, 100 - (pe * 3) - (pb * 10)))
        
        # Profitability score
        roe = company_data.get('roe', 0)
        roa = company_data.get('roa', 0)
        net_margin = company_data.get('net_margin', 0)
        scores['profitability'] = min(100, (roe * 3 + roa * 5 + net_margin * 3))
        
        # Financial health score
        de = company_data.get('debt_equity', 1)
        current = company_data.get('current_ratio', 1)
        scores['financial_health'] = min(100, max(0, 100 - (de * 30) + (current * 20)))
        
        # Dividend score
        div_yield = company_data.get('dividend_yield', 0)
        scores['dividend'] = min(100, div_yield * 12)
        
        # Growth score (placeholder - would need historical data)
        scores['growth'] = 50  # Default to neutral
        
        # Calculate weighted average
        composite = sum(scores[k] * weights[k] for k in weights.keys())
        
        return round(composite, 2)


class ChartHelpers:
    """Helper functions for creating charts"""
    
    @staticmethod
    def get_color_for_change(value: float) -> str:
        """Get color based on positive/negative value"""
        if value >= 0:
            return '#00C851'  # Green
        return '#ff4444'  # Red
    
    @staticmethod
    def get_valuation_color(pe_ratio: float) -> str:
        """Get color based on P/E ratio valuation"""
        if pe_ratio <= 10:
            return '#00C851'  # Very undervalued - Green
        elif pe_ratio <= 15:
            return '#4CAF50'  # Undervalued - Light green
        elif pe_ratio <= 20:
            return '#FFC107'  # Fair - Yellow
        elif pe_ratio <= 25:
            return '#FF9800'  # Slightly overvalued - Orange
        else:
            return '#ff4444'  # Overvalued - Red
    
    @staticmethod
    def format_large_number(num: float, prefix: str = "", suffix: str = "") -> str:
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
    
    @staticmethod
    def format_percentage(value: float, decimals: int = 2) -> str:
        """Format value as percentage"""
        if pd.isna(value):
            return "N/A"
        return f"{value:.{decimals}f}%"
    
    @staticmethod
    def format_ratio(value: float, suffix: str = "x") -> str:
        """Format value as ratio"""
        if pd.isna(value):
            return "N/A"
        return f"{value:.2f}{suffix}"


def export_to_excel(df: pd.DataFrame, filename: str = None) -> Path:
    """Export DataFrame to Excel file"""
    
    if filename is None:
        filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    filepath = REPORTS_DIR / filename
    
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Data', index=False)
    
    return filepath


def get_market_status() -> dict:
    """Get current market status (placeholder)"""
    
    # In real implementation, this would fetch live market status
    now = datetime.now()
    
    # CSE trading hours: 9:30 AM - 2:30 PM on weekdays
    is_trading_hours = (
        now.weekday() < 5 and  # Monday to Friday
        9 <= now.hour < 15  # Between 9 AM and 3 PM
    )
    
    return {
        'is_open': is_trading_hours,
        'status': 'OPEN' if is_trading_hours else 'CLOSED',
        'last_update': now.strftime('%Y-%m-%d %H:%M:%S'),
        'trading_hours': '9:30 AM - 2:30 PM',
        'timezone': 'Asia/Colombo'
    }


# Validation thresholds for different metrics
METRIC_THRESHOLDS = {
    'pe_ratio': {
        'excellent': (0, 10),
        'good': (10, 15),
        'fair': (15, 20),
        'poor': (20, 30),
        'very_poor': (30, float('inf'))
    },
    'pb_ratio': {
        'excellent': (0, 1),
        'good': (1, 1.5),
        'fair': (1.5, 2.5),
        'poor': (2.5, 4),
        'very_poor': (4, float('inf'))
    },
    'roe': {
        'very_poor': (float('-inf'), 5),
        'poor': (5, 10),
        'fair': (10, 15),
        'good': (15, 20),
        'excellent': (20, float('inf'))
    },
    'dividend_yield': {
        'very_poor': (0, 1),
        'poor': (1, 2),
        'fair': (2, 4),
        'good': (4, 6),
        'excellent': (6, float('inf'))
    },
    'debt_equity': {
        'excellent': (0, 0.3),
        'good': (0.3, 0.5),
        'fair': (0.5, 1),
        'poor': (1, 1.5),
        'very_poor': (1.5, float('inf'))
    },
    'current_ratio': {
        'very_poor': (0, 0.5),
        'poor': (0.5, 1),
        'fair': (1, 1.5),
        'good': (1.5, 2),
        'excellent': (2, float('inf'))
    }
}


def get_metric_rating(metric_name: str, value: float) -> tuple:
    """Get rating and color for a metric value"""
    
    if metric_name not in METRIC_THRESHOLDS:
        return ('unknown', '#808080')
    
    thresholds = METRIC_THRESHOLDS[metric_name]
    
    for rating, (low, high) in thresholds.items():
        if low <= value < high:
            color_map = {
                'excellent': '#00C851',
                'good': '#4CAF50',
                'fair': '#FFC107',
                'poor': '#FF9800',
                'very_poor': '#ff4444',
                'unknown': '#808080'
            }
            return (rating.replace('_', ' ').title(), color_map.get(rating, '#808080'))
    
    return ('Unknown', '#808080')
