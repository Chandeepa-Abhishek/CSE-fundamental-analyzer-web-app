"""
Report Generator Module
Creates Excel and PDF reports with analysis results
"""
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List
import sys
sys.path.append('..')
from config.settings import REPORTS_DIR

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generates comprehensive reports from analysis results
    """
    
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def generate_excel_report(self, 
                               all_data: pd.DataFrame,
                               rankings: pd.DataFrame,
                               strategy_results: Dict[str, pd.DataFrame],
                               filename: str = None) -> str:
        """
        Generate comprehensive Excel report with multiple sheets
        """
        if filename is None:
            filename = f"cse_analysis_report_{self.timestamp}.xlsx"
        
        filepath = REPORTS_DIR / filename
        
        try:
            with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # Define formats
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#1F4E79',
                    'font_color': 'white',
                    'border': 1
                })
                
                good_format = workbook.add_format({
                    'bg_color': '#C6EFCE',
                    'font_color': '#006100'
                })
                
                bad_format = workbook.add_format({
                    'bg_color': '#FFC7CE',
                    'font_color': '#9C0006'
                })
                
                number_format = workbook.add_format({'num_format': '#,##0.00'})
                percent_format = workbook.add_format({'num_format': '0.00%'})
                
                # Sheet 1: Executive Summary
                summary_data = self._create_summary_sheet(all_data, rankings)
                summary_data.to_excel(writer, sheet_name='Summary', index=False)
                
                # Sheet 2: Top Ranked Stocks
                if not rankings.empty:
                    top_stocks = rankings.head(50)
                    top_stocks.to_excel(writer, sheet_name='Top 50 Stocks', index=False)
                
                # Sheet 3: All Companies Data
                if not all_data.empty:
                    all_data.to_excel(writer, sheet_name='All Companies', index=False)
                
                # Strategy sheets
                for strategy_name, strategy_df in strategy_results.items():
                    if not strategy_df.empty:
                        sheet_name = f'{strategy_name[:25]} Strategy'
                        strategy_df.head(30).to_excel(
                            writer, 
                            sheet_name=sheet_name, 
                            index=False
                        )
                
                # Sheet: Sector Analysis
                if 'sector' in all_data.columns:
                    sector_analysis = self._create_sector_analysis(all_data)
                    sector_analysis.to_excel(
                        writer, 
                        sheet_name='Sector Analysis', 
                        index=False
                    )
            
            logger.info(f"Excel report saved to {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            return ""
    
    def _create_summary_sheet(self, 
                               all_data: pd.DataFrame,
                               rankings: pd.DataFrame) -> pd.DataFrame:
        """Create executive summary data"""
        summary = []
        
        # Market overview
        summary.append({
            'Metric': 'Report Date',
            'Value': datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        summary.append({
            'Metric': 'Total Companies Analyzed',
            'Value': len(all_data)
        })
        
        # Add various statistics
        if 'pe_ratio' in all_data.columns:
            pe_valid = all_data['pe_ratio'].dropna()
            pe_valid = pe_valid[pe_valid > 0]
            if len(pe_valid) > 0:
                summary.append({
                    'Metric': 'Average P/E Ratio',
                    'Value': round(pe_valid.mean(), 2)
                })
                summary.append({
                    'Metric': 'Median P/E Ratio',
                    'Value': round(pe_valid.median(), 2)
                })
        
        if 'dividend_yield' in all_data.columns:
            div_valid = all_data['dividend_yield'].dropna()
            div_valid = div_valid[div_valid > 0]
            if len(div_valid) > 0:
                summary.append({
                    'Metric': 'Average Dividend Yield (%)',
                    'Value': round(div_valid.mean(), 2)
                })
        
        if 'roe' in all_data.columns:
            roe_valid = all_data['roe'].dropna()
            if len(roe_valid) > 0:
                summary.append({
                    'Metric': 'Average ROE (%)',
                    'Value': round(roe_valid.mean(), 2)
                })
        
        # Top picks summary
        if not rankings.empty:
            summary.append({'Metric': '', 'Value': ''})
            summary.append({'Metric': '--- TOP 5 PICKS ---', 'Value': ''})
            
            for i, row in rankings.head(5).iterrows():
                summary.append({
                    'Metric': f"#{row.get('rank', i+1)}: {row.get('symbol', '')}",
                    'Value': f"Score: {row.get('composite_score', 0):.1f}"
                })
        
        return pd.DataFrame(summary)
    
    def _create_sector_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create sector-wise analysis"""
        if 'sector' not in df.columns:
            return pd.DataFrame()
        
        metrics = ['pe_ratio', 'pb_ratio', 'dividend_yield', 'roe']
        available_metrics = [m for m in metrics if m in df.columns]
        
        sector_stats = []
        
        for sector in df['sector'].dropna().unique():
            sector_df = df[df['sector'] == sector]
            
            stat = {
                'Sector': sector,
                'Count': len(sector_df),
            }
            
            for metric in available_metrics:
                valid = sector_df[metric].dropna()
                if len(valid) > 0:
                    stat[f'{metric}_avg'] = round(valid.mean(), 2)
                    stat[f'{metric}_median'] = round(valid.median(), 2)
            
            sector_stats.append(stat)
        
        return pd.DataFrame(sector_stats).sort_values('Count', ascending=False)
    
    def generate_csv_report(self, 
                            rankings: pd.DataFrame,
                            filename: str = None) -> str:
        """Generate simple CSV report"""
        if filename is None:
            filename = f"cse_rankings_{self.timestamp}.csv"
        
        filepath = REPORTS_DIR / filename
        rankings.to_csv(filepath, index=False)
        
        logger.info(f"CSV report saved to {filepath}")
        return str(filepath)
    
    def generate_text_summary(self, 
                               rankings: pd.DataFrame,
                               num_top: int = 10) -> str:
        """Generate text summary for console output"""
        lines = []
        lines.append("=" * 60)
        lines.append("CSE STOCK ANALYSIS REPORT")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append("=" * 60)
        lines.append("")
        
        lines.append(f"TOP {num_top} STOCKS BY COMPOSITE SCORE:")
        lines.append("-" * 60)
        
        if not rankings.empty:
            for i, row in rankings.head(num_top).iterrows():
                symbol = row.get('symbol', 'N/A')
                name = row.get('name', '')[:30]
                score = row.get('composite_score', 0)
                price = row.get('last_traded_price', 0)
                
                lines.append(
                    f"{row.get('rank', i+1):3}. {symbol:15} | "
                    f"Score: {score:5.1f} | Price: {price:10.2f}"
                )
        
        lines.append("")
        lines.append("=" * 60)
        
        return "\n".join(lines)


class ConsoleReporter:
    """
    Displays analysis results in console with formatting
    """
    
    @staticmethod
    def print_header(title: str):
        """Print formatted header"""
        print("\n" + "=" * 60)
        print(f"  {title}")
        print("=" * 60)
    
    @staticmethod
    def print_subheader(title: str):
        """Print formatted subheader"""
        print(f"\n--- {title} ---")
    
    @staticmethod
    def print_table(df: pd.DataFrame, columns: List[str] = None, 
                    max_rows: int = 20):
        """Print DataFrame as formatted table"""
        if df.empty:
            print("No data available")
            return
        
        if columns:
            display_df = df[columns].head(max_rows)
        else:
            display_df = df.head(max_rows)
        
        print(display_df.to_string(index=False))
    
    @staticmethod
    def print_strategy_results(results: Dict[str, pd.DataFrame]):
        """Print results from all strategies"""
        for strategy_name, df in results.items():
            ConsoleReporter.print_subheader(f"{strategy_name.upper()} Strategy")
            print(f"Found {len(df)} stocks")
            
            if not df.empty and len(df) > 0:
                display_cols = ['symbol', 'name', 'last_traded_price', 
                               'pe_ratio', 'dividend_yield']
                available_cols = [c for c in display_cols if c in df.columns]
                
                if available_cols:
                    ConsoleReporter.print_table(
                        df[available_cols].head(10), 
                        max_rows=10
                    )
