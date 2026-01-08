"""
Advanced Financial Metrics Calculator
=====================================
Calculates comprehensive financial ratios and special scores
from annual report data for investment analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple


class AdvancedMetricsCalculator:
    """Calculate advanced financial metrics for stock analysis"""
    
    def __init__(self):
        self.risk_free_rate = 0.10  # 10% for Sri Lanka
    
    def calculate_all_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all advanced metrics for a dataframe"""
        df = df.copy()
        
        # Valuation Metrics
        df = self._calculate_valuation_metrics(df)
        
        # Profitability Metrics
        df = self._calculate_profitability_metrics(df)
        
        # Liquidity Metrics
        df = self._calculate_liquidity_metrics(df)
        
        # Leverage Metrics
        df = self._calculate_leverage_metrics(df)
        
        # Efficiency Metrics
        df = self._calculate_efficiency_metrics(df)
        
        # Cash Flow Metrics
        df = self._calculate_cashflow_metrics(df)
        
        # Quality Metrics
        df = self._calculate_quality_metrics(df)
        
        # Special Scores
        df = self._calculate_special_scores(df)
        
        return df
    
    def _calculate_valuation_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate valuation ratios"""
        
        # PEG Ratio (P/E divided by growth rate)
        # Assuming 10% growth if not available
        df['earnings_growth_rate'] = df.get('eps_growth', 10)
        df['peg_ratio'] = np.where(
            df['earnings_growth_rate'] > 0,
            df['pe_ratio'] / df['earnings_growth_rate'],
            np.nan
        )
        
        # Enterprise Value = Market Cap + Total Debt - Cash
        cash = df.get('cash', df.get('operating_cash_flow', 0) * 0.3)
        df['enterprise_value'] = df['market_cap'] + df.get('total_debt', 0) - cash
        
        # EV/EBITDA
        df['ebitda'] = df.get('operating_income', 0) + df.get('depreciation', 
                       df.get('operating_income', 0) * 0.15)  # Estimate depreciation
        df['ev_ebitda'] = np.where(
            df['ebitda'] > 0,
            df['enterprise_value'] / df['ebitda'],
            np.nan
        )
        
        # Price to Sales
        shares = df.get('shares_outstanding', df['market_cap'] / df['last_traded_price'])
        revenue_per_share = df.get('revenue', 0) / shares
        df['ps_ratio'] = np.where(
            revenue_per_share > 0,
            df['last_traded_price'] / revenue_per_share,
            np.nan
        )
        
        # Price to Free Cash Flow
        fcf_per_share = df.get('free_cash_flow', 0) / shares
        df['p_fcf'] = np.where(
            fcf_per_share > 0,
            df['last_traded_price'] / fcf_per_share,
            np.nan
        )
        
        # Earnings Yield (inverse of P/E, as percentage)
        df['earnings_yield'] = np.where(
            df['pe_ratio'] > 0,
            (1 / df['pe_ratio']) * 100,
            0
        )
        
        # FCF Yield
        df['fcf_yield'] = np.where(
            df['market_cap'] > 0,
            (df.get('free_cash_flow', 0) / df['market_cap']) * 100,
            0
        )
        
        return df
    
    def _calculate_profitability_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate profitability ratios"""
        
        # Operating Margin
        df['operating_margin'] = np.where(
            df.get('revenue', 0) > 0,
            (df.get('operating_income', 0) / df['revenue']) * 100,
            0
        )
        
        # EBITDA Margin
        df['ebitda_margin'] = np.where(
            df.get('revenue', 0) > 0,
            (df.get('ebitda', 0) / df['revenue']) * 100,
            0
        )
        
        # Return on Invested Capital (ROIC)
        # ROIC = NOPAT / Invested Capital
        # NOPAT = Operating Income * (1 - Tax Rate)
        tax_rate = 0.24  # Sri Lanka corporate tax
        nopat = df.get('operating_income', 0) * (1 - tax_rate)
        invested_capital = df.get('shareholders_equity', 0) + df.get('total_debt', 0)
        df['roic'] = np.where(
            invested_capital > 0,
            (nopat / invested_capital) * 100,
            0
        )
        
        # Return on Capital Employed (ROCE)
        # ROCE = EBIT / Capital Employed
        capital_employed = df.get('total_assets', 0) - df.get('total_liabilities', 0) * 0.3
        df['roce'] = np.where(
            capital_employed > 0,
            (df.get('operating_income', 0) / capital_employed) * 100,
            0
        )
        
        return df
    
    def _calculate_liquidity_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate liquidity ratios"""
        
        # Estimate current assets and liabilities if not available
        current_assets = df.get('current_assets', df.get('total_assets', 0) * 0.4)
        current_liabilities = df.get('current_liabilities', 
                                     df.get('total_liabilities', 0) * 0.35)
        inventory = df.get('inventory', current_assets * 0.25)
        cash = df.get('cash', current_assets * 0.15)
        
        # Quick Ratio (Acid Test)
        df['quick_ratio'] = np.where(
            current_liabilities > 0,
            (current_assets - inventory) / current_liabilities,
            0
        )
        
        # Cash Ratio
        df['cash_ratio'] = np.where(
            current_liabilities > 0,
            cash / current_liabilities,
            0
        )
        
        # Working Capital
        df['working_capital'] = current_assets - current_liabilities
        
        # Working Capital Ratio
        df['working_capital_ratio'] = np.where(
            df.get('revenue', 0) > 0,
            df['working_capital'] / df['revenue'],
            0
        )
        
        return df
    
    def _calculate_leverage_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate leverage and solvency ratios"""
        
        # Interest Coverage Ratio
        interest_expense = df.get('interest_expense', df.get('total_debt', 0) * 0.08)
        df['interest_coverage'] = np.where(
            interest_expense > 0,
            df.get('operating_income', 0) / interest_expense,
            999  # No debt
        )
        
        # Debt to Assets
        df['debt_to_assets'] = np.where(
            df.get('total_assets', 0) > 0,
            df.get('total_debt', 0) / df['total_assets'],
            0
        )
        
        # Debt to EBITDA
        df['debt_to_ebitda'] = np.where(
            df.get('ebitda', 0) > 0,
            df.get('total_debt', 0) / df['ebitda'],
            np.nan
        )
        
        # Equity Multiplier (Financial Leverage)
        df['equity_multiplier'] = np.where(
            df.get('shareholders_equity', 0) > 0,
            df.get('total_assets', 0) / df['shareholders_equity'],
            0
        )
        
        # Net Debt
        cash = df.get('cash', df.get('operating_cash_flow', 0) * 0.3)
        df['net_debt'] = df.get('total_debt', 0) - cash
        
        # Net Debt to Equity
        df['net_debt_to_equity'] = np.where(
            df.get('shareholders_equity', 0) > 0,
            df['net_debt'] / df['shareholders_equity'],
            0
        )
        
        return df
    
    def _calculate_efficiency_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate efficiency/activity ratios"""
        
        # Fixed Asset Turnover
        fixed_assets = df.get('fixed_assets', df.get('total_assets', 0) * 0.5)
        df['fixed_asset_turnover'] = np.where(
            fixed_assets > 0,
            df.get('revenue', 0) / fixed_assets,
            0
        )
        
        # Inventory Turnover (estimate)
        inventory = df.get('inventory', df.get('total_assets', 0) * 0.1)
        cogs = df.get('cogs', df.get('revenue', 0) * 0.65)  # Estimate COGS
        df['inventory_turnover'] = np.where(
            inventory > 0,
            cogs / inventory,
            0
        )
        
        # Days Inventory Outstanding (DIO)
        df['days_inventory'] = np.where(
            df['inventory_turnover'] > 0,
            365 / df['inventory_turnover'],
            0
        )
        
        # Receivables Turnover (estimate)
        receivables = df.get('receivables', df.get('revenue', 0) / 12)  # ~1 month
        df['receivables_turnover'] = np.where(
            receivables > 0,
            df.get('revenue', 0) / receivables,
            0
        )
        
        # Days Sales Outstanding (DSO)
        df['days_sales_outstanding'] = np.where(
            df['receivables_turnover'] > 0,
            365 / df['receivables_turnover'],
            0
        )
        
        # Payables Turnover (estimate)
        payables = df.get('payables', cogs / 10)  # ~1 month
        df['payables_turnover'] = np.where(
            payables > 0,
            cogs / payables,
            0
        )
        
        # Days Payables Outstanding (DPO)
        df['days_payables'] = np.where(
            df['payables_turnover'] > 0,
            365 / df['payables_turnover'],
            0
        )
        
        # Cash Conversion Cycle = DIO + DSO - DPO
        df['cash_conversion_cycle'] = (
            df['days_inventory'] + 
            df['days_sales_outstanding'] - 
            df['days_payables']
        )
        
        return df
    
    def _calculate_cashflow_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate cash flow ratios"""
        
        # Operating Cash Flow Margin
        df['ocf_margin'] = np.where(
            df.get('revenue', 0) > 0,
            (df.get('operating_cash_flow', 0) / df['revenue']) * 100,
            0
        )
        
        # Cash Flow to Debt
        df['cf_to_debt'] = np.where(
            df.get('total_debt', 0) > 0,
            df.get('operating_cash_flow', 0) / df['total_debt'],
            999  # No debt
        )
        
        # Cash Flow per Share
        shares = df.get('shares_outstanding', df['market_cap'] / df['last_traded_price'])
        df['cfps'] = df.get('operating_cash_flow', 0) / shares
        
        # Free Cash Flow per Share
        df['fcfps'] = df.get('free_cash_flow', 0) / shares
        
        # FCF to Net Income (Quality indicator)
        df['fcf_to_net_income'] = np.where(
            df.get('net_profit', 0) > 0,
            (df.get('free_cash_flow', 0) / df['net_profit']) * 100,
            0
        )
        
        # Cash Return on Assets
        df['cash_roa'] = np.where(
            df.get('total_assets', 0) > 0,
            (df.get('operating_cash_flow', 0) / df['total_assets']) * 100,
            0
        )
        
        return df
    
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate earnings quality metrics"""
        
        # Accruals Ratio = (Net Income - Operating Cash Flow) / Total Assets
        # Lower is better (more cash-based earnings)
        df['accruals_ratio'] = np.where(
            df.get('total_assets', 0) > 0,
            (df.get('net_profit', 0) - df.get('operating_cash_flow', 0)) / df['total_assets'],
            0
        )
        
        # Sloan Ratio (same as accruals, different interpretation)
        # High accruals suggest lower quality earnings
        df['sloan_ratio'] = df['accruals_ratio']
        
        # Earnings Quality Score (0-100)
        # Based on FCF/NI ratio, accruals, and consistency
        df['earnings_quality'] = 50  # Base score
        df.loc[df['fcf_to_net_income'] > 80, 'earnings_quality'] += 20
        df.loc[df['fcf_to_net_income'] > 100, 'earnings_quality'] += 10
        df.loc[df['accruals_ratio'].abs() < 0.05, 'earnings_quality'] += 15
        df.loc[df['accruals_ratio'].abs() < 0.02, 'earnings_quality'] += 5
        df['earnings_quality'] = df['earnings_quality'].clip(0, 100)
        
        return df
    
    def _calculate_special_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate special investment scores"""
        
        # 1. Beneish M-Score (Earnings Manipulation Detection)
        df = self._calculate_beneish_m_score(df)
        
        # 2. DuPont Analysis
        df = self._calculate_dupont(df)
        
        # 3. Sustainable Growth Rate
        df = self._calculate_growth_rates(df)
        
        # 4. Dividend Safety
        df = self._calculate_dividend_metrics(df)
        
        return df
    
    def _calculate_beneish_m_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Beneish M-Score for detecting earnings manipulation.
        M-Score > -2.22 suggests possible manipulation.
        """
        # Simplified version using available data
        # Full M-Score requires multi-year data
        
        # Components (using estimates where needed)
        dsri = 1.0  # Days Sales in Receivables Index
        gmi = 1.0   # Gross Margin Index  
        aqi = 1.0   # Asset Quality Index
        sgi = 1.1   # Sales Growth Index (assume 10% growth)
        depi = 1.0  # Depreciation Index
        sgai = 1.0  # SG&A Index
        lvgi = df['equity_multiplier'] / 2  # Leverage Index
        tata = df['accruals_ratio']  # Total Accruals to Total Assets
        
        # M-Score Formula
        df['beneish_m_score'] = (
            -4.84 +
            0.920 * dsri +
            0.528 * gmi +
            0.404 * aqi +
            0.892 * sgi +
            0.115 * depi -
            0.172 * sgai +
            4.679 * tata -
            0.327 * lvgi
        )
        
        # Interpretation
        df['manipulation_risk'] = 'Low'
        df.loc[df['beneish_m_score'] > -2.22, 'manipulation_risk'] = 'Possible'
        df.loc[df['beneish_m_score'] > -1.78, 'manipulation_risk'] = 'Likely'
        
        return df
    
    def _calculate_dupont(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        DuPont Analysis - Break down ROE into components.
        ROE = Net Margin × Asset Turnover × Equity Multiplier
        """
        # Net Profit Margin
        df['dupont_npm'] = df.get('net_margin', 0)
        
        # Asset Turnover
        df['dupont_at'] = df.get('asset_turnover', 
            df.get('revenue', 0) / df.get('total_assets', 1))
        
        # Equity Multiplier (Financial Leverage)
        df['dupont_em'] = df.get('equity_multiplier', 
            df.get('total_assets', 0) / df.get('shareholders_equity', 1))
        
        # Verify: ROE = NPM × AT × EM
        df['dupont_roe_calc'] = (
            (df['dupont_npm'] / 100) * 
            df['dupont_at'] * 
            df['dupont_em'] * 100
        )
        
        # ROE Source Analysis
        df['roe_driver'] = 'Balanced'
        df.loc[df['dupont_npm'] > df['roe'] * 0.5, 'roe_driver'] = 'Margin Driven'
        df.loc[df['dupont_at'] > 1.5, 'roe_driver'] = 'Efficiency Driven'
        df.loc[df['dupont_em'] > 3, 'roe_driver'] = 'Leverage Driven'
        
        return df
    
    def _calculate_growth_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate growth-related metrics"""
        
        # Sustainable Growth Rate = ROE × (1 - Payout Ratio)
        payout_ratio = df.get('payout_ratio', 0.4)  # Assume 40% if not available
        df['sustainable_growth_rate'] = df['roe'] * (1 - payout_ratio)
        
        # Internal Growth Rate = ROA × (1 - Payout Ratio) / (1 - ROA × (1 - Payout Ratio))
        retention = 1 - payout_ratio
        roa_decimal = df['roa'] / 100
        df['internal_growth_rate'] = np.where(
            (1 - roa_decimal * retention) > 0,
            (roa_decimal * retention) / (1 - roa_decimal * retention) * 100,
            0
        )
        
        return df
    
    def _calculate_dividend_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate dividend analysis metrics"""
        
        # Dividend Payout Ratio
        df['payout_ratio'] = np.where(
            df.get('eps', 0) > 0,
            (df.get('dividend_per_share', 0) / df['eps']) * 100,
            0
        )
        
        # Dividend Coverage Ratio
        df['dividend_coverage'] = np.where(
            df.get('dividend_per_share', 0) > 0,
            df.get('eps', 0) / df['dividend_per_share'],
            0
        )
        
        # FCF Dividend Coverage
        shares = df.get('shares_outstanding', df['market_cap'] / df['last_traded_price'])
        total_dividends = df.get('dividend_per_share', 0) * shares
        df['fcf_dividend_coverage'] = np.where(
            total_dividends > 0,
            df.get('free_cash_flow', 0) / total_dividends,
            0
        )
        
        # Dividend Safety Score (0-100)
        df['dividend_safety'] = 50
        df.loc[df['payout_ratio'] < 60, 'dividend_safety'] += 15
        df.loc[df['payout_ratio'] < 40, 'dividend_safety'] += 10
        df.loc[df['dividend_coverage'] > 2, 'dividend_safety'] += 15
        df.loc[df['fcf_dividend_coverage'] > 1.5, 'dividend_safety'] += 10
        df['dividend_safety'] = df['dividend_safety'].clip(0, 100)
        
        return df
    
    def get_metric_explanations(self) -> Dict[str, Dict]:
        """Return explanations for all metrics"""
        return {
            # Valuation
            "peg_ratio": {
                "name": "PEG Ratio",
                "formula": "P/E Ratio ÷ Earnings Growth Rate",
                "good": "< 1.0",
                "bad": "> 2.0",
                "description": "Values P/E relative to growth. <1 means growth is underpriced."
            },
            "ev_ebitda": {
                "name": "EV/EBITDA",
                "formula": "Enterprise Value ÷ EBITDA",
                "good": "< 10",
                "bad": "> 15",
                "description": "Compares company value to cash profits. Better than P/E for comparing companies with different debt levels."
            },
            "earnings_yield": {
                "name": "Earnings Yield",
                "formula": "(EPS ÷ Price) × 100",
                "good": "> 10%",
                "bad": "< 5%",
                "description": "Inverse of P/E. Compare to bond yields - if higher, stocks may be better value."
            },
            "fcf_yield": {
                "name": "FCF Yield",
                "formula": "(Free Cash Flow ÷ Market Cap) × 100",
                "good": "> 8%",
                "bad": "< 3%",
                "description": "Cash return on investment. Higher = better value."
            },
            
            # Profitability
            "roic": {
                "name": "ROIC (Return on Invested Capital)",
                "formula": "NOPAT ÷ Invested Capital",
                "good": "> 15%",
                "bad": "< 8%",
                "description": "True measure of how well company uses capital. Warren Buffett's favorite metric."
            },
            "roce": {
                "name": "ROCE (Return on Capital Employed)",
                "formula": "EBIT ÷ Capital Employed",
                "good": "> 15%",
                "bad": "< 10%",
                "description": "How efficiently company uses all capital including debt."
            },
            
            # Liquidity
            "quick_ratio": {
                "name": "Quick Ratio (Acid Test)",
                "formula": "(Current Assets - Inventory) ÷ Current Liabilities",
                "good": "> 1.0",
                "bad": "< 0.5",
                "description": "Can company pay short-term bills without selling inventory?"
            },
            "interest_coverage": {
                "name": "Interest Coverage Ratio",
                "formula": "EBIT ÷ Interest Expense",
                "good": "> 5",
                "bad": "< 2",
                "description": "Can company afford its debt payments? <1.5 is danger zone."
            },
            
            # Efficiency
            "cash_conversion_cycle": {
                "name": "Cash Conversion Cycle",
                "formula": "Days Inventory + Days Receivables - Days Payables",
                "good": "< 30 days",
                "bad": "> 90 days",
                "description": "How fast company converts investments into cash. Lower is better."
            },
            
            # Quality
            "accruals_ratio": {
                "name": "Accruals Ratio",
                "formula": "(Net Income - Operating Cash Flow) ÷ Total Assets",
                "good": "< 5%",
                "bad": "> 10%",
                "description": "High accruals suggest accounting profits, not real cash. May indicate manipulation."
            },
            "beneish_m_score": {
                "name": "Beneish M-Score",
                "formula": "Complex 8-variable model",
                "good": "< -2.22",
                "bad": "> -2.22",
                "description": "Detects earnings manipulation. Score > -2.22 suggests possible fraud."
            },
            
            # Dividend
            "payout_ratio": {
                "name": "Dividend Payout Ratio",
                "formula": "(Dividends ÷ Net Income) × 100",
                "good": "30-60%",
                "bad": "> 80%",
                "description": "What % of profits paid as dividends. Too high = unsustainable."
            },
            "dividend_coverage": {
                "name": "Dividend Coverage",
                "formula": "EPS ÷ Dividend Per Share",
                "good": "> 2.0",
                "bad": "< 1.5",
                "description": "How many times can earnings cover dividends? Higher = safer dividend."
            },
            
            # Growth
            "sustainable_growth_rate": {
                "name": "Sustainable Growth Rate",
                "formula": "ROE × (1 - Payout Ratio)",
                "good": "> 10%",
                "bad": "< 5%",
                "description": "Max growth rate without taking new debt or issuing shares."
            },
        }


def get_metrics_for_beginners() -> str:
    """Return a beginner-friendly explanation of key metrics"""
    return """
    ## 🎓 Understanding Advanced Metrics
    
    ### 💰 Valuation Metrics - "Is it Cheap?"
    
    **PEG Ratio** - Better than P/E!
    - P/E ratio divided by growth rate
    - PEG < 1 = Stock is undervalued relative to growth
    - PEG > 2 = Stock is expensive
    - Example: P/E of 20 with 20% growth = PEG of 1 (fair value)
    
    **EV/EBITDA** - Professional's favorite
    - Compares total company value to cash profits
    - Works better than P/E when comparing companies with different debt
    - EV/EBITDA < 10 is generally cheap
    
    **Earnings Yield** - Compare to bonds
    - If earnings yield > 10% and bonds pay 8%, stocks are better value
    - Higher earnings yield = cheaper stock
    
    ---
    
    ### 📊 Profitability - "Is it a Good Business?"
    
    **ROIC (Return on Invested Capital)** - Warren Buffett's favorite!
    - Shows how well company uses ALL money (yours + borrowed)
    - ROIC > 15% = Excellent business
    - ROIC > Cost of Capital = Creates value
    - Look for consistent ROIC over 5+ years
    
    ---
    
    ### 🛡️ Safety - "Will it Survive?"
    
    **Interest Coverage** - Can it pay debts?
    - How many times profits cover interest payments
    - > 5x = Very safe
    - < 2x = Danger zone!
    - < 1x = May default on debt
    
    **Quick Ratio** - Emergency liquidity
    - Can company pay bills WITHOUT selling inventory?
    - > 1 = Safe
    - < 0.5 = May face cash crisis
    
    ---
    
    ### 🔍 Quality - "Are Profits Real?"
    
    **Accruals Ratio** - Earnings manipulation detector
    - Low accruals = Cash-based profits (real!)
    - High accruals = Accounting profits (could be fake)
    - < 5% is good, > 10% is suspicious
    
    **Beneish M-Score** - Fraud detector
    - M-Score > -2.22 = Possible earnings manipulation
    - Used by forensic accountants
    - Enron had high M-Score before collapse!
    
    ---
    
    ### 💵 Dividend Safety - "Will Dividends Continue?"
    
    **Payout Ratio** - Sustainability check
    - < 60% = Dividend is sustainable
    - > 80% = Dividend might be cut
    - > 100% = Company paying more than it earns (unsustainable!)
    
    **Dividend Coverage** - Safety margin
    - How many times earnings cover dividends
    - > 2x = Very safe dividend
    - < 1.5x = Dividend at risk
    
    ---
    
    ### 🎯 Quick Reference for Stock Selection
    
    **Value Investor Checklist:**
    ✅ PEG < 1
    ✅ EV/EBITDA < 10
    ✅ Earnings Yield > 10%
    
    **Quality Investor Checklist:**
    ✅ ROIC > 15%
    ✅ Accruals Ratio < 5%
    ✅ FCF/Net Income > 80%
    
    **Dividend Investor Checklist:**
    ✅ Payout Ratio < 60%
    ✅ Dividend Coverage > 2x
    ✅ Interest Coverage > 5x
    
    **Safety Checklist:**
    ✅ Interest Coverage > 5x
    ✅ Quick Ratio > 1
    ✅ Debt/EBITDA < 3
    ✅ M-Score < -2.22
    """
