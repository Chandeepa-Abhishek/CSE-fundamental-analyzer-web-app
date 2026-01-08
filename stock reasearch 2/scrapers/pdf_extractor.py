"""
PDF Financial Report Extractor
Downloads and extracts financial data from CSE annual reports and quarterly reports
"""
import requests
import os
import re
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import json

import pdfplumber
import pandas as pd

import sys
sys.path.append('..')
from config.settings import (
    CSE_BASE_URL, DEFAULT_HEADERS, RAW_DATA_DIR, 
    REQUEST_TIMEOUT, REQUEST_DELAY
)

logger = logging.getLogger(__name__)


class CSEPDFExtractor:
    """
    Downloads and extracts financial data from CSE company PDF reports
    
    CSE publishes:
    - Annual Reports (comprehensive financial statements)
    - Quarterly Reports (interim financials)
    - Financial Statements (audited accounts)
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.pdf_dir = RAW_DATA_DIR / "pdfs"
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        
        # CSE document URLs pattern
        self.cse_cdn_url = "https://cdn.cse.lk"
    
    def get_company_documents(self, symbol: str) -> List[Dict]:
        """
        Fetch list of available documents (PDFs) for a company
        """
        documents = []
        
        # Try the announcements/filings API
        urls_to_try = [
            f"{CSE_BASE_URL}/api/companyAnnouncements?symbol={symbol}",
            f"{CSE_BASE_URL}/api/companyFilings?symbol={symbol}",
            f"{CSE_BASE_URL}/api/annualReports?symbol={symbol}",
        ]
        
        for url in urls_to_try:
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        documents.extend(data)
                    elif isinstance(data, dict) and 'data' in data:
                        documents.extend(data['data'])
            except Exception as e:
                logger.debug(f"Could not fetch from {url}: {e}")
        
        # Filter for financial documents
        financial_docs = []
        keywords = ['annual report', 'financial', 'quarterly', 'interim', 
                   'accounts', 'statement', 'balance sheet']
        
        for doc in documents:
            title = doc.get('title', '') + doc.get('description', '')
            if any(kw in title.lower() for kw in keywords):
                financial_docs.append(doc)
        
        return financial_docs
    
    def download_pdf(self, pdf_url: str, symbol: str, 
                     doc_type: str = "report") -> Optional[str]:
        """
        Download a PDF file from CSE
        """
        try:
            # Clean up URL
            if not pdf_url.startswith('http'):
                if pdf_url.startswith('/'):
                    pdf_url = f"{self.cse_cdn_url}{pdf_url}"
                else:
                    pdf_url = f"{self.cse_cdn_url}/{pdf_url}"
            
            response = self.session.get(pdf_url, timeout=60)
            response.raise_for_status()
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"{symbol}_{doc_type}_{timestamp}.pdf"
            filepath = self.pdf_dir / symbol / filename
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Failed to download PDF from {pdf_url}: {e}")
            return None
    
    def extract_tables_from_pdf(self, pdf_path: str) -> List[pd.DataFrame]:
        """
        Extract all tables from a PDF using pdfplumber
        """
        tables = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_tables = page.extract_tables()
                    
                    for table in page_tables:
                        if table and len(table) > 1:
                            # Convert to DataFrame
                            df = pd.DataFrame(table[1:], columns=table[0])
                            df['_page'] = page_num + 1
                            tables.append(df)
            
            logger.info(f"Extracted {len(tables)} tables from {pdf_path}")
            
        except Exception as e:
            logger.error(f"Error extracting tables from {pdf_path}: {e}")
        
        return tables
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract all text from a PDF
        """
        text = ""
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
        
        return text
    
    def parse_income_statement(self, tables: List[pd.DataFrame]) -> Dict:
        """
        Parse income statement data from extracted tables
        """
        income_data = {
            'revenue': None,
            'cost_of_sales': None,
            'gross_profit': None,
            'operating_expenses': None,
            'operating_income': None,
            'finance_costs': None,
            'profit_before_tax': None,
            'tax_expense': None,
            'net_profit': None,
            'eps': None,
        }
        
        # Keywords to identify income statement rows
        row_mappings = {
            'revenue': ['revenue', 'turnover', 'sales', 'income from operations'],
            'cost_of_sales': ['cost of sales', 'cost of goods', 'cost of revenue'],
            'gross_profit': ['gross profit', 'gross margin'],
            'operating_expenses': ['operating expenses', 'admin expenses', 'distribution costs'],
            'operating_income': ['operating profit', 'operating income', 'profit from operations'],
            'finance_costs': ['finance cost', 'interest expense', 'finance expense'],
            'profit_before_tax': ['profit before tax', 'pbt', 'income before tax'],
            'tax_expense': ['tax expense', 'income tax', 'taxation'],
            'net_profit': ['profit for the year', 'net profit', 'profit after tax', 'net income'],
            'eps': ['earnings per share', 'eps', 'basic eps'],
        }
        
        for table in tables:
            for _, row in table.iterrows():
                row_text = ' '.join(str(v).lower() for v in row.values if pd.notna(v))
                
                for field, keywords in row_mappings.items():
                    if any(kw in row_text for kw in keywords):
                        # Try to extract numeric value
                        for val in row.values:
                            num = self._extract_number(val)
                            if num is not None:
                                income_data[field] = num
                                break
        
        return income_data
    
    def parse_balance_sheet(self, tables: List[pd.DataFrame]) -> Dict:
        """
        Parse balance sheet data from extracted tables
        """
        balance_data = {
            'total_assets': None,
            'current_assets': None,
            'non_current_assets': None,
            'cash_and_equivalents': None,
            'inventory': None,
            'receivables': None,
            'total_liabilities': None,
            'current_liabilities': None,
            'non_current_liabilities': None,
            'total_debt': None,
            'shareholders_equity': None,
            'retained_earnings': None,
            'share_capital': None,
        }
        
        row_mappings = {
            'total_assets': ['total assets'],
            'current_assets': ['current assets', 'total current assets'],
            'non_current_assets': ['non-current assets', 'non current assets', 'fixed assets'],
            'cash_and_equivalents': ['cash and cash equivalents', 'cash and bank', 'cash at bank'],
            'inventory': ['inventory', 'inventories', 'stock'],
            'receivables': ['trade receivables', 'accounts receivable', 'debtors'],
            'total_liabilities': ['total liabilities'],
            'current_liabilities': ['current liabilities', 'total current liabilities'],
            'non_current_liabilities': ['non-current liabilities', 'long term liabilities'],
            'total_debt': ['total borrowings', 'bank borrowings', 'loans and borrowings'],
            'shareholders_equity': ['shareholders equity', 'total equity', 'shareholders funds'],
            'retained_earnings': ['retained earnings', 'accumulated profits'],
            'share_capital': ['share capital', 'stated capital', 'issued capital'],
        }
        
        for table in tables:
            for _, row in table.iterrows():
                row_text = ' '.join(str(v).lower() for v in row.values if pd.notna(v))
                
                for field, keywords in row_mappings.items():
                    if any(kw in row_text for kw in keywords):
                        for val in row.values:
                            num = self._extract_number(val)
                            if num is not None:
                                balance_data[field] = num
                                break
        
        return balance_data
    
    def parse_cash_flow(self, tables: List[pd.DataFrame]) -> Dict:
        """
        Parse cash flow statement data
        """
        cashflow_data = {
            'operating_cash_flow': None,
            'investing_cash_flow': None,
            'financing_cash_flow': None,
            'net_cash_flow': None,
            'free_cash_flow': None,
            'capex': None,
            'dividends_paid': None,
        }
        
        row_mappings = {
            'operating_cash_flow': ['cash from operating', 'operating activities', 'cash generated from operations'],
            'investing_cash_flow': ['cash from investing', 'investing activities'],
            'financing_cash_flow': ['cash from financing', 'financing activities'],
            'net_cash_flow': ['net increase in cash', 'net change in cash'],
            'capex': ['purchase of property', 'capital expenditure', 'acquisition of assets'],
            'dividends_paid': ['dividends paid', 'dividend paid'],
        }
        
        for table in tables:
            for _, row in table.iterrows():
                row_text = ' '.join(str(v).lower() for v in row.values if pd.notna(v))
                
                for field, keywords in row_mappings.items():
                    if any(kw in row_text for kw in keywords):
                        for val in row.values:
                            num = self._extract_number(val)
                            if num is not None:
                                cashflow_data[field] = num
                                break
        
        # Calculate free cash flow if possible
        if cashflow_data['operating_cash_flow'] and cashflow_data['capex']:
            cashflow_data['free_cash_flow'] = (
                cashflow_data['operating_cash_flow'] - abs(cashflow_data['capex'])
            )
        
        return cashflow_data
    
    def _extract_number(self, value) -> Optional[float]:
        """
        Extract numeric value from cell, handling various formats
        """
        if pd.isna(value):
            return None
        
        text = str(value).strip()
        
        # Remove currency symbols and common text
        text = re.sub(r'[Rs.LKR\s,()]', '', text)
        
        # Handle brackets as negative (accounting format)
        is_negative = text.startswith('(') or text.endswith(')')
        text = text.replace('(', '').replace(')', '')
        
        # Handle millions/thousands notation
        multiplier = 1
        if "'000" in str(value) or "000s" in str(value).lower():
            multiplier = 1000
        if "mn" in str(value).lower() or "million" in str(value).lower():
            multiplier = 1_000_000
        
        try:
            # Extract number
            match = re.search(r'-?[\d.]+', text)
            if match:
                num = float(match.group()) * multiplier
                return -num if is_negative else num
        except ValueError:
            pass
        
        return None
    
    def extract_financial_data(self, symbol: str, 
                               pdf_path: str = None) -> Dict:
        """
        Extract comprehensive financial data from company PDF
        
        Returns combined data from income statement, balance sheet, and cash flow
        """
        # Download PDF if not provided
        if pdf_path is None:
            docs = self.get_company_documents(symbol)
            if docs:
                # Get most recent annual report
                for doc in docs:
                    url = doc.get('url', doc.get('link', ''))
                    if url and url.endswith('.pdf'):
                        pdf_path = self.download_pdf(url, symbol, 'annual_report')
                        break
        
        if not pdf_path or not os.path.exists(pdf_path):
            logger.warning(f"No PDF available for {symbol}")
            return {}
        
        # Extract tables
        tables = self.extract_tables_from_pdf(pdf_path)
        
        if not tables:
            logger.warning(f"No tables found in PDF for {symbol}")
            return {}
        
        # Parse financial statements
        income_data = self.parse_income_statement(tables)
        balance_data = self.parse_balance_sheet(tables)
        cashflow_data = self.parse_cash_flow(tables)
        
        # Combine all data
        financial_data = {
            'symbol': symbol,
            'pdf_source': pdf_path,
            'extracted_date': datetime.now().isoformat(),
            **income_data,
            **balance_data,
            **cashflow_data,
        }
        
        # Calculate additional ratios
        financial_data.update(self._calculate_ratios(financial_data))
        
        return financial_data
    
    def _calculate_ratios(self, data: Dict) -> Dict:
        """
        Calculate financial ratios from extracted data
        """
        ratios = {}
        
        # Profitability ratios
        if data.get('revenue') and data.get('net_profit'):
            ratios['net_profit_margin'] = round(
                (data['net_profit'] / data['revenue']) * 100, 2
            )
        
        if data.get('revenue') and data.get('gross_profit'):
            ratios['gross_profit_margin'] = round(
                (data['gross_profit'] / data['revenue']) * 100, 2
            )
        
        # Return ratios
        if data.get('net_profit') and data.get('shareholders_equity'):
            ratios['roe'] = round(
                (data['net_profit'] / data['shareholders_equity']) * 100, 2
            )
        
        if data.get('net_profit') and data.get('total_assets'):
            ratios['roa'] = round(
                (data['net_profit'] / data['total_assets']) * 100, 2
            )
        
        # Leverage ratios
        if data.get('total_debt') and data.get('shareholders_equity'):
            ratios['debt_to_equity'] = round(
                data['total_debt'] / data['shareholders_equity'], 2
            )
        
        # Liquidity ratios
        if data.get('current_assets') and data.get('current_liabilities'):
            ratios['current_ratio'] = round(
                data['current_assets'] / data['current_liabilities'], 2
            )
        
        return ratios
    
    def extract_all_companies(self, symbols: List[str], 
                             progress_callback=None) -> pd.DataFrame:
        """
        Extract financial data from PDFs for multiple companies
        """
        all_data = []
        total = len(symbols)
        
        for i, symbol in enumerate(symbols):
            if progress_callback:
                progress_callback(i + 1, total, symbol)
            
            logger.info(f"Processing {symbol} ({i+1}/{total})")
            
            try:
                data = self.extract_financial_data(symbol)
                if data:
                    all_data.append(data)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
            
            time.sleep(REQUEST_DELAY)  # Be respectful
        
        df = pd.DataFrame(all_data)
        
        # Save extracted data
        if not df.empty:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(RAW_DATA_DIR / f"pdf_extracted_data_{timestamp}.csv", index=False)
            df.to_json(RAW_DATA_DIR / f"pdf_extracted_data_{timestamp}.json", 
                      orient='records', indent=2)
        
        return df


class FinancialStatementParser:
    """
    Advanced parser for Sri Lankan financial statement formats
    """
    
    # Sri Lankan Accounting Standards (SLFRS/LKAS) common terms
    SRI_LANKAN_TERMS = {
        # Revenue terms
        'revenue': ['revenue', 'turnover', 'gross income', 'income from operations'],
        
        # Expense terms  
        'cost_of_sales': ['cost of sales', 'cost of goods sold', 'cost of revenue'],
        'admin_expenses': ['administrative expenses', 'admin expenses'],
        'selling_expenses': ['selling and distribution', 'distribution costs'],
        'finance_costs': ['finance cost', 'finance expenses', 'interest expense'],
        
        # Profit terms
        'gross_profit': ['gross profit', 'gross margin'],
        'operating_profit': ['operating profit', 'results from operating activities'],
        'profit_before_tax': ['profit before tax', 'profit before income tax'],
        'profit_after_tax': ['profit for the year', 'profit for the period', 'net profit'],
        
        # Balance sheet - Assets
        'ppe': ['property, plant and equipment', 'fixed assets'],
        'intangibles': ['intangible assets', 'goodwill'],
        'investments': ['investments', 'financial assets'],
        'inventory': ['inventories', 'stocks'],
        'receivables': ['trade and other receivables', 'trade receivables', 'debtors'],
        'cash': ['cash and cash equivalents', 'cash and bank balances'],
        
        # Balance sheet - Liabilities
        'borrowings': ['interest bearing borrowings', 'bank borrowings', 'loans'],
        'payables': ['trade and other payables', 'trade payables', 'creditors'],
        'provisions': ['provisions', 'employee benefits'],
        
        # Equity
        'share_capital': ['stated capital', 'share capital', 'issued capital'],
        'reserves': ['reserves', 'revaluation reserve'],
        'retained_earnings': ['retained earnings', 'accumulated profits'],
    }
    
    @classmethod
    def identify_statement_type(cls, text: str) -> str:
        """
        Identify the type of financial statement from text content
        """
        text_lower = text.lower()
        
        if any(term in text_lower for term in ['income statement', 'profit or loss', 
                                                'statement of comprehensive income']):
            return 'income_statement'
        elif any(term in text_lower for term in ['balance sheet', 'financial position',
                                                  'statement of financial position']):
            return 'balance_sheet'
        elif any(term in text_lower for term in ['cash flow', 'cash flows']):
            return 'cash_flow'
        elif any(term in text_lower for term in ['changes in equity', 'equity statement']):
            return 'equity_statement'
        
        return 'unknown'
    
    @classmethod
    def extract_years_from_header(cls, header_row: List) -> List[str]:
        """
        Extract financial years from table header
        """
        years = []
        for cell in header_row:
            cell_str = str(cell)
            # Match patterns like 2024, 2023/24, 31.03.2024
            year_match = re.search(r'20\d{2}', cell_str)
            if year_match:
                years.append(year_match.group())
        return years
