# CSE Sri Lanka Stock Research & Analysis Tool

A comprehensive Python tool to scrape financial data from the Colombo Stock Exchange (CSE) and analyze companies using standard investment valuation strategies.

## 🌟 NEW: Web Dashboard

**Interactive web interface for visualizing all company data with historical trends!**

```bash
# Run the dashboard
python run_dashboard.py

# Or directly with streamlit
streamlit run web/app.py
```

### Dashboard Features:
- 📊 **Dashboard Overview** - Market summary, top gainers/losers, sector distribution
- 🏢 **Company Analysis** - Deep-dive into individual companies with all financial metrics
- 📈 **Historical Trends** - Compare multiple companies over time, track report-by-report changes
- 🔍 **Stock Screener** - Filter stocks by P/E, P/B, Dividend, ROE, Debt/Equity
- 📊 **Sector Analysis** - Sector comparison and drill-down
- 💼 **Portfolio Builder** - Build value/income/growth portfolios
- 📑 **Financial Reports** - Income Statement, Balance Sheet, Cash Flow from PDF annual reports

![Dashboard Preview](docs/dashboard.png)

## Features

### Data Collection
- Fetch all listed companies from CSE
- Scrape financial metrics (EPS, PE, Book Value, Dividend Yield, etc.)
- **📄 PDF Annual Report Extraction** - Download and parse detailed financial statements
- Historical price data collection
- Corporate announcements tracking

### PDF Financial Data Extraction
The tool can download and parse PDF annual reports to extract:
- **Income Statement**: Revenue, Gross Profit, Operating Profit, Net Profit
- **Balance Sheet**: Total Assets, Liabilities, Equity, Debt levels
- **Cash Flow Statement**: Operating, Investing, Financing cash flows
- **Calculated Ratios**: ROE, ROA, Debt/Equity, Profit Margins

### Valuation Strategies Implemented

1. **Value Investing (Graham/Buffett Style)**
   - Low P/E ratio screening
   - Price-to-Book value analysis
   - Debt-to-Equity evaluation
   - Earnings consistency check

2. **Dividend Investing**
   - High dividend yield stocks
   - Dividend payout ratio analysis
   - Dividend growth tracking

3. **Growth Investing**
   - EPS growth rate analysis
   - Revenue growth trends
   - PEG ratio calculation

4. **GARP (Growth at Reasonable Price)**
   - Combines growth metrics with value metrics
   - PEG ratio < 1 screening

5. **Quality Investing**
   - ROE (Return on Equity) analysis
   - Profit margin evaluation
   - Asset turnover metrics

## Project Structure

```
stock-research-2/
├── config/
│   └── settings.py          # Configuration settings
├── scrapers/
│   ├── __init__.py
│   ├── cse_scraper.py       # Main CSE data scraper
│   ├── api_client.py        # CSE API client
│   └── pdf_extractor.py     # PDF annual report parser
├── analysis/
│   ├── __init__.py
│   ├── valuations.py        # Valuation calculations
│   ├── screeners.py         # Stock screening strategies
│   └── rankings.py          # Company ranking system
├── web/                     # 🆕 Web Dashboard
│   ├── __init__.py
│   ├── app.py               # Streamlit dashboard application
│   └── utils.py             # Dashboard utilities
├── data/
│   ├── raw/                 # Raw scraped data
│   │   └── pdfs/            # Downloaded PDF reports
│   └── processed/           # Cleaned data
├── reports/
│   └── (generated reports)
├── main.py                  # CLI entry point
├── run_dashboard.py         # 🆕 Dashboard launcher
├── requirements.txt
└── README.md
```

## Installation

```bash
# Navigate to project directory
cd "d:\Projects\CODING\stock reasearch 2"

# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows)
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 🌐 Web Dashboard (Recommended)

```bash
# Launch the interactive dashboard
python run_dashboard.py

# The dashboard opens at http://localhost:8501
```

### 💻 Command Line Interface

```bash
# Run full analysis
python main.py

# Run specific screener
python main.py --strategy value
python main.py --strategy dividend
python main.py --strategy growth

# Extract detailed data from PDF annual reports (recommended for best analysis)
python main.py --update-data --extract-pdfs --export excel

# Export results to Excel
python main.py --export excel

# Update data only
python main.py --update-data

# Use sample data for testing
python main.py --use-sample
```

## Key Metrics Analyzed

| Metric | Description | Good Value |
|--------|-------------|------------|
| P/E Ratio | Price to Earnings | < 15 (Value) |
| P/B Ratio | Price to Book | < 1.5 |
| Dividend Yield | Annual dividend / Price | > 4% |
| ROE | Return on Equity | > 15% |
| Debt/Equity | Financial leverage | < 0.5 |
| EPS Growth | Earnings growth rate | > 10% |
| PEG Ratio | P/E to Growth | < 1 |

## Data Sources

- **CSE Website**: https://www.cse.lk
- **Company Profiles**: Financial statements, ratios
- **Market Data**: Real-time prices, volumes
- **Announcements**: Corporate disclosures
- **PDF Annual Reports**: Detailed audited financial statements (Income Statement, Balance Sheet, Cash Flow)

## Data Collection Methods

| Method | Data Available | Speed | Detail Level |
|--------|---------------|-------|--------------|
| API/Web Scraping | P/E, EPS, Price, Volume | Fast | Basic |
| PDF Extraction | Full financial statements | Slow | Comprehensive |

### PDF Data Fields Extracted:
- Revenue, Cost of Sales, Gross Profit
- Operating Expenses, Operating Income
- Finance Costs, Profit Before Tax, Net Profit
- Total Assets, Current Assets, Fixed Assets
- Total Liabilities, Current Liabilities, Debt
- Shareholders' Equity, Retained Earnings
- Operating Cash Flow, Free Cash Flow
- ROE, ROA, Debt/Equity, Profit Margins

## Disclaimer

This tool is for educational and research purposes only. Always do your own due diligence before making investment decisions. Past performance does not guarantee future results.

## Screenshots

### Dashboard Home
- Market overview with top gainers/losers
- P/E distribution chart
- Sector breakdown
- Top stocks by various metrics

### Company Analysis
- Price & key metrics display
- Financial statements from PDFs
- Historical trend charts (report-by-report)
- Valuation analysis with buy/sell signals
- 52-week price range

### Stock Screener
- Filter by multiple criteria simultaneously
- Visual scatter plot of results
- Export filtered stocks

### Historical Trends
- Compare up to 5 companies
- Track any metric over multiple years
- Year-over-year growth analysis

## License

MIT License - Free to use and modify
