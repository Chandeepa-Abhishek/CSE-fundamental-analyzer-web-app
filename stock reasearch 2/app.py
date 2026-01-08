"""
CSE Stock Research Tool - Main Entry Point
Colombo Stock Exchange Analysis & Stock Screening

This tool scrapes financial data from CSE (www.cse.lk) and performs
comprehensive analysis to find the best investment opportunities.
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import (
    LOG_LEVEL, LOG_FORMAT, LOG_FILE, 
    PROCESSED_DATA_DIR, REPORTS_DIR
)
from scrapers.cse_scraper import CSEDataCollector
from scrapers.api_client import CSEAPIClient
from scrapers.pdf_extractor import CSEPDFExtractor
from analysis.valuations import ValuationAnalyzer
from analysis.screeners import StockScreener
from analysis.rankings import CompanyRanker, PortfolioSuggester
from reports.report_generator import ReportGenerator, ConsoleReporter

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def setup_argparser() -> argparse.ArgumentParser:
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description="CSE Stock Research & Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     # Run full analysis
  python main.py --strategy value    # Run value investing screen
  python main.py --update-data       # Update data from CSE
  python main.py --export excel      # Export results to Excel
  python main.py --top 20            # Show top 20 stocks
        """
    )
    
    parser.add_argument(
        '--strategy', '-s',
        choices=['value', 'dividend', 'growth', 'garp', 'quality', 
                 'momentum', 'bargain', 'blue_chip', '52_week_low', 'all'],
        default='all',
        help='Investment strategy to apply'
    )
    
    parser.add_argument(
        '--update-data', '-u',
        action='store_true',
        help='Fetch fresh data from CSE website'
    )
    
    parser.add_argument(
        '--export', '-e',
        choices=['excel', 'csv', 'both'],
        help='Export results to file'
    )
    
    parser.add_argument(
        '--top', '-t',
        type=int,
        default=20,
        help='Number of top stocks to display'
    )
    
    parser.add_argument(
        '--sector',
        type=str,
        help='Filter by sector (e.g., "Banks", "Manufacturing")'
    )
    
    parser.add_argument(
        '--use-sample',
        action='store_true',
        help='Use sample data for testing (no web scraping)'
    )
    
    parser.add_argument(
        '--extract-pdfs',
        action='store_true',
        help='Extract financial data from PDF annual reports (slower but more detailed)'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress detailed output'
    )
    
    return parser


def print_banner():
    """Print application banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║           CSE STOCK RESEARCH & ANALYSIS TOOL                 ║
    ║         Colombo Stock Exchange - Sri Lanka                   ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Strategies: Value | Growth | Dividend | GARP | Quality      ║
    ║  Data Source: www.cse.lk                                     ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def progress_callback(current: int, total: int, symbol: str):
    """Progress callback for data fetching"""
    pct = (current / total) * 100
    print(f"\rFetching: {symbol:15} ({current}/{total}) [{pct:5.1f}%]", end='', flush=True)


def generate_sample_data():
    """Generate sample data for testing without scraping"""
    import pandas as pd
    import numpy as np
    
    # Sample Sri Lankan companies (representative data)
    sample_companies = [
        {"symbol": "JKH.N0000", "name": "John Keells Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "COMB.N0000", "name": "Commercial Bank of Ceylon PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "SAMP.N0000", "name": "Sampath Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "HNB.N0000", "name": "Hatton National Bank PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "DIAL.N0000", "name": "Dialog Axiata PLC", "sector": "Telecommunications"},
        {"symbol": "CARG.N0000", "name": "Cargills (Ceylon) PLC", "sector": "Stores Supplies"},
        {"symbol": "NEST.N0000", "name": "Nestle Lanka PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "CTC.N0000", "name": "Ceylon Tobacco Company PLC", "sector": "Beverage Food & Tobacco"},
        {"symbol": "HEXP.N0000", "name": "Hemas Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "TILE.N0000", "name": "Lanka Tiles PLC", "sector": "Manufacturing"},
        {"symbol": "LOLC.N0000", "name": "LOLC Holdings PLC", "sector": "Diversified Holdings"},
        {"symbol": "SLTL.N0000", "name": "Sri Lanka Telecom PLC", "sector": "Telecommunications"},
        {"symbol": "ALLI.N0000", "name": "Alliance Finance Company PLC", "sector": "Banks Finance & Insurance"},
        {"symbol": "RICH.N0000", "name": "Richard Pieris & Company PLC", "sector": "Diversified Holdings"},
        {"symbol": "GREG.N0000", "name": "Distilleries Company of Sri Lanka", "sector": "Beverage Food & Tobacco"},
        {"symbol": "EXPO.N0000", "name": "Expolanka Holdings PLC", "sector": "Services"},
        {"symbol": "HAYC.N0000", "name": "Haycarb PLC", "sector": "Manufacturing"},
        {"symbol": "DIPD.N0000", "name": "Dipped Products PLC", "sector": "Manufacturing"},
        {"symbol": "ASIR.N0000", "name": "Asiri Hospital Holdings PLC", "sector": "Healthcare"},
        {"symbol": "CARS.N0000", "name": "Ceylon & Foreign Trades PLC", "sector": "Trading"},
    ]
    
    np.random.seed(42)  # For reproducibility
    
    data = []
    for company in sample_companies:
        price = np.random.uniform(20, 500)
        eps = np.random.uniform(2, 30)
        nav = np.random.uniform(30, 200)
        
        data.append({
            **company,
            "last_traded_price": round(price, 2),
            "change_percent": round(np.random.uniform(-5, 5), 2),
            "volume": int(np.random.uniform(10000, 500000)),
            "market_cap": int(np.random.uniform(1e9, 100e9)),
            "eps": round(eps, 2),
            "pe_ratio": round(price / eps if eps > 0 else 0, 2),
            "pb_ratio": round(price / nav if nav > 0 else 0, 2),
            "nav": round(nav, 2),
            "dividend_yield": round(np.random.uniform(0, 10), 2),
            "dividend_per_share": round(np.random.uniform(0, 20), 2),
            "roe": round(np.random.uniform(5, 25), 2),
            "debt_equity": round(np.random.uniform(0, 1.5), 2),
            "52_week_high": round(price * np.random.uniform(1.1, 1.5), 2),
            "52_week_low": round(price * np.random.uniform(0.6, 0.9), 2),
        })
    
    return pd.DataFrame(data)


def main():
    """Main entry point"""
    print_banner()
    
    parser = setup_argparser()
    args = parser.parse_args()
    
    # Initialize components
    collector = CSEDataCollector()
    analyzer = ValuationAnalyzer()
    reporter = ReportGenerator()
    
    try:
        # Step 1: Get Data
        ConsoleReporter.print_header("DATA COLLECTION")
        
        if args.use_sample:
            print("Using sample data for testing...")
            df = generate_sample_data()
            print(f"Generated {len(df)} sample companies")
        elif args.update_data:
            print("Fetching fresh data from CSE website...")
            print("This may take several minutes...\n")
            df = collector.collect_all_data(progress_callback=progress_callback)
            print(f"\nCollected data for {len(df)} companies")
        else:
            # Try to load existing data
            print("Loading existing data...")
            df = collector.get_latest_data()
            
            if df.empty:
                print("No existing data found. Fetching from CSE...")
                df = collector.collect_all_data(progress_callback=progress_callback)
            else:
                print(f"Loaded {len(df)} companies from cache")
        
        if df.empty:
            print("\nERROR: No data available. Please try with --update-data flag")
            return
        
        # Optional: Extract detailed data from PDF annual reports
        if args.extract_pdfs and not args.use_sample:
            ConsoleReporter.print_header("PDF ANNUAL REPORT EXTRACTION")
            print("Downloading and parsing PDF annual reports...")
            print("This provides detailed financial statement data.\n")
            
            pdf_extractor = CSEPDFExtractor()
            symbols = df['symbol'].tolist() if 'symbol' in df.columns else []
            
            if symbols:
                pdf_data = pdf_extractor.extract_all_companies(
                    symbols[:20],  # Limit to first 20 for speed
                    progress_callback=progress_callback
                )
                
                if not pdf_data.empty:
                    # Merge PDF data with existing data
                    df = df.merge(pdf_data, on='symbol', how='left', suffixes=('', '_pdf'))
                    print(f"\n✅ Extracted PDF data for {len(pdf_data)} companies")
                    print("Added: Revenue, Net Profit, Total Assets, ROE, Debt/Equity, etc.")
        
        # Filter by sector if specified
        if args.sector:
            df = df[df['sector'].str.contains(args.sector, case=False, na=False)]
            print(f"Filtered to {len(df)} companies in {args.sector} sector")
        
        # Step 2: Run Analysis
        ConsoleReporter.print_header("VALUATION ANALYSIS")
        
        # Analyze all companies
        analysis_df = analyzer.analyze_all_companies(df)
        
        # Merge analysis with original data
        if 'symbol' in df.columns and 'symbol' in analysis_df.columns:
            full_df = df.merge(
                analysis_df[['symbol', 'intrinsic_value_graham', 'margin_of_safety', 
                            'valuation_status', 'value_signals_count']], 
                on='symbol', 
                how='left'
            )
        else:
            full_df = df
        
        # Step 3: Run Screeners
        ConsoleReporter.print_header("STOCK SCREENING")
        
        screener = StockScreener(full_df)
        strategy_results = {}
        
        if args.strategy == 'all':
            print("Running all investment strategies...")
            strategy_results = screener.run_all_strategies()
            
            for name, result_df in strategy_results.items():
                print(f"  {name:15}: {len(result_df):3} stocks found")
        else:
            strategies = screener.get_all_strategies()
            if args.strategy in strategies:
                print(f"Running {args.strategy} strategy...")
                strategy_results[args.strategy] = strategies[args.strategy]()
                print(f"Found {len(strategy_results[args.strategy])} stocks")
        
        # Step 4: Rank Companies
        ConsoleReporter.print_header("COMPANY RANKINGS")
        
        ranker = CompanyRanker(full_df)
        rankings = ranker.calculate_composite_score()
        
        # Display top stocks
        print(f"\nTOP {args.top} STOCKS BY COMPOSITE SCORE:")
        print("-" * 70)
        
        if not rankings.empty:
            display_cols = ['rank', 'symbol', 'name', 'composite_score', 
                           'value_score', 'growth_score', 'dividend_score']
            available_cols = [c for c in display_cols if c in rankings.columns]
            
            top_df = rankings[available_cols].head(args.top)
            print(top_df.to_string(index=False))
        
        # Step 5: Portfolio Suggestions
        ConsoleReporter.print_header("PORTFOLIO SUGGESTIONS")
        
        suggester = PortfolioSuggester(ranker)
        
        print("\nBalanced Portfolio (10 stocks):")
        balanced = suggester.suggest_balanced_portfolio(10)
        if not balanced.empty:
            print(balanced[['symbol', 'composite_score']].to_string(index=False))
        
        print("\nDividend Income Portfolio:")
        income = suggester.suggest_income_portfolio(5)
        if not income.empty and 'symbol' in income.columns:
            print(income[['symbol']].head().to_string(index=False))
        
        # Step 6: Generate Reports
        if args.export:
            ConsoleReporter.print_header("GENERATING REPORTS")
            
            if args.export in ['excel', 'both']:
                excel_path = reporter.generate_excel_report(
                    full_df, rankings, strategy_results
                )
                print(f"Excel report: {excel_path}")
            
            if args.export in ['csv', 'both']:
                csv_path = reporter.generate_csv_report(rankings)
                print(f"CSV report: {csv_path}")
        
        # Print summary
        ConsoleReporter.print_header("ANALYSIS COMPLETE")
        print(f"""
Summary:
  - Companies analyzed: {len(full_df)}
  - Top pick: {rankings.iloc[0]['symbol'] if not rankings.empty else 'N/A'}
  - Value stocks found: {len(strategy_results.get('value', []))}
  - Dividend stocks found: {len(strategy_results.get('dividend', []))}
  
Run with --export excel to save full report.
        """)
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("An error occurred during analysis")
        print(f"\nError: {e}")
        print("Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
