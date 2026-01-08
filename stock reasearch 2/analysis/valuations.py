"""
Valuation Analysis Module
Implements standard valuation metrics and calculations
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
import sys
sys.path.append('..')
from config.settings import VALUATION_THRESHOLDS

logger = logging.getLogger(__name__)


class ValuationAnalyzer:
    """
    Analyzes stock valuations using fundamental metrics
    Implements classic valuation methodologies
    """
    
    def __init__(self, thresholds: Dict = None):
        self.thresholds = thresholds or VALUATION_THRESHOLDS
    
    def calculate_intrinsic_value_graham(self, eps: float, 
                                          growth_rate: float = 0,
                                          aaa_yield: float = 4.4) -> float:
        """
        Benjamin Graham's Intrinsic Value Formula
        V = EPS × (8.5 + 2g) × 4.4/Y
        
        Where:
        - EPS: Earnings Per Share
        - g: Expected growth rate (%)
        - Y: Current AAA corporate bond yield (%)
        
        Returns intrinsic value per share
        """
        if eps is None or eps <= 0:
            return 0
        
        # Graham's formula
        intrinsic_value = eps * (8.5 + 2 * growth_rate) * (4.4 / aaa_yield)
        return round(intrinsic_value, 2)
    
    def calculate_intrinsic_value_dcf(self, 
                                       free_cash_flow: float,
                                       growth_rate: float = 0.05,
                                       discount_rate: float = 0.10,
                                       terminal_growth: float = 0.02,
                                       years: int = 5,
                                       shares_outstanding: float = 1) -> float:
        """
        Discounted Cash Flow (DCF) Valuation
        
        Projects future cash flows and discounts them to present value
        
        Returns intrinsic value per share
        """
        if free_cash_flow is None or free_cash_flow <= 0:
            return 0
        
        if shares_outstanding is None or shares_outstanding <= 0:
            shares_outstanding = 1
        
        # Project future cash flows
        present_value = 0
        current_fcf = free_cash_flow
        
        for year in range(1, years + 1):
            projected_fcf = current_fcf * (1 + growth_rate) ** year
            pv_fcf = projected_fcf / (1 + discount_rate) ** year
            present_value += pv_fcf
        
        # Terminal value (perpetuity)
        terminal_fcf = current_fcf * (1 + growth_rate) ** years * (1 + terminal_growth)
        terminal_value = terminal_fcf / (discount_rate - terminal_growth)
        pv_terminal = terminal_value / (1 + discount_rate) ** years
        
        total_value = present_value + pv_terminal
        intrinsic_value_per_share = total_value / shares_outstanding
        
        return round(intrinsic_value_per_share, 2)
    
    def calculate_peg_ratio(self, pe_ratio: float, 
                            growth_rate: float) -> Optional[float]:
        """
        PEG Ratio = P/E / Growth Rate
        
        Useful for comparing stocks with different growth rates
        PEG < 1 is generally considered undervalued
        """
        if pe_ratio is None or growth_rate is None or growth_rate <= 0:
            return None
        
        return round(pe_ratio / growth_rate, 2)
    
    def calculate_margin_of_safety(self, 
                                    current_price: float,
                                    intrinsic_value: float) -> float:
        """
        Margin of Safety = (Intrinsic Value - Current Price) / Intrinsic Value
        
        Expressed as percentage
        Higher is better (more safety margin)
        """
        if intrinsic_value is None or intrinsic_value <= 0:
            return 0
        
        if current_price is None:
            return 0
        
        mos = ((intrinsic_value - current_price) / intrinsic_value) * 100
        return round(mos, 2)
    
    def calculate_earnings_yield(self, eps: float, price: float) -> Optional[float]:
        """
        Earnings Yield = EPS / Price (inverse of P/E)
        
        Useful for comparing stocks to bond yields
        Higher is better
        """
        if eps is None or price is None or price <= 0:
            return None
        
        return round((eps / price) * 100, 2)
    
    def calculate_dividend_payout_ratio(self, 
                                         dividend_per_share: float,
                                         eps: float) -> Optional[float]:
        """
        Dividend Payout Ratio = DPS / EPS
        
        Shows what portion of earnings is paid as dividends
        Lower ratio = more earnings retained for growth
        """
        if dividend_per_share is None or eps is None or eps <= 0:
            return None
        
        return round((dividend_per_share / eps) * 100, 2)
    
    def calculate_price_to_sales(self, 
                                  market_cap: float,
                                  revenue: float) -> Optional[float]:
        """
        Price-to-Sales Ratio = Market Cap / Revenue
        
        Useful for companies with no earnings
        Lower is generally better
        """
        if market_cap is None or revenue is None or revenue <= 0:
            return None
        
        return round(market_cap / revenue, 2)
    
    def calculate_ev_to_ebitda(self, 
                                market_cap: float,
                                total_debt: float,
                                cash: float,
                                ebitda: float) -> Optional[float]:
        """
        EV/EBITDA = Enterprise Value / EBITDA
        
        Enterprise Value = Market Cap + Debt - Cash
        Lower is generally better
        """
        if ebitda is None or ebitda <= 0:
            return None
        
        debt = total_debt if total_debt else 0
        cash_val = cash if cash else 0
        cap = market_cap if market_cap else 0
        
        ev = cap + debt - cash_val
        
        return round(ev / ebitda, 2)
    
    def analyze_company(self, company_data: Dict) -> Dict:
        """
        Perform comprehensive valuation analysis on a single company
        
        Returns dict with all calculated metrics
        """
        analysis = {
            "symbol": company_data.get("symbol"),
            "name": company_data.get("name"),
            
            # Raw metrics
            "price": company_data.get("last_traded_price"),
            "eps": company_data.get("eps"),
            "pe_ratio": company_data.get("pe_ratio"),
            "pb_ratio": company_data.get("pb_ratio"),
            "nav": company_data.get("nav"),
            "dividend_yield": company_data.get("dividend_yield"),
            "roe": company_data.get("roe"),
            "market_cap": company_data.get("market_cap"),
            
            # Calculated metrics
            "intrinsic_value_graham": None,
            "margin_of_safety": None,
            "peg_ratio": None,
            "earnings_yield": None,
            "payout_ratio": None,
            
            # Valuation signals
            "is_undervalued_pe": False,
            "is_undervalued_pb": False,
            "is_undervalued_graham": False,
            "has_high_dividend": False,
            "has_good_roe": False,
            "has_low_debt": False,
            
            # Overall assessment
            "value_signals_count": 0,
            "valuation_status": "Neutral",
        }
        
        price = company_data.get("last_traded_price")
        eps = company_data.get("eps")
        pe = company_data.get("pe_ratio")
        pb = company_data.get("pb_ratio")
        nav = company_data.get("nav")
        div_yield = company_data.get("dividend_yield")
        roe = company_data.get("roe")
        debt_equity = company_data.get("debt_equity")
        growth_rate = company_data.get("eps_growth", 5)  # Default 5%
        
        # Graham Intrinsic Value
        if eps and eps > 0:
            analysis["intrinsic_value_graham"] = self.calculate_intrinsic_value_graham(
                eps, growth_rate
            )
            if price and analysis["intrinsic_value_graham"]:
                analysis["margin_of_safety"] = self.calculate_margin_of_safety(
                    price, analysis["intrinsic_value_graham"]
                )
        
        # PEG Ratio
        if pe and growth_rate and growth_rate > 0:
            analysis["peg_ratio"] = self.calculate_peg_ratio(pe, growth_rate)
        
        # Earnings Yield
        if eps and price:
            analysis["earnings_yield"] = self.calculate_earnings_yield(eps, price)
        
        # Dividend Payout Ratio
        dps = company_data.get("dividend_per_share")
        if dps and eps:
            analysis["payout_ratio"] = self.calculate_dividend_payout_ratio(dps, eps)
        
        # Valuation signals
        signals = 0
        
        # P/E check
        if pe and pe > 0 and pe < self.thresholds["pe_ratio_max"]:
            analysis["is_undervalued_pe"] = True
            signals += 1
        
        # P/B check
        if pb and pb > 0 and pb < self.thresholds["pb_ratio_max"]:
            analysis["is_undervalued_pb"] = True
            signals += 1
        
        # Graham check
        if analysis["margin_of_safety"] and analysis["margin_of_safety"] > 30:
            analysis["is_undervalued_graham"] = True
            signals += 1
        
        # Dividend check
        if div_yield and div_yield > self.thresholds["dividend_yield_min"]:
            analysis["has_high_dividend"] = True
            signals += 1
        
        # ROE check
        if roe and roe > self.thresholds["roe_min"]:
            analysis["has_good_roe"] = True
            signals += 1
        
        # Debt check
        if debt_equity is not None and debt_equity < self.thresholds["debt_equity_max"]:
            analysis["has_low_debt"] = True
            signals += 1
        
        analysis["value_signals_count"] = signals
        
        # Overall status
        if signals >= 4:
            analysis["valuation_status"] = "Strongly Undervalued"
        elif signals >= 3:
            analysis["valuation_status"] = "Undervalued"
        elif signals >= 2:
            analysis["valuation_status"] = "Fairly Valued"
        elif signals >= 1:
            analysis["valuation_status"] = "Neutral"
        else:
            analysis["valuation_status"] = "Potentially Overvalued"
        
        return analysis
    
    def analyze_all_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze all companies in the DataFrame
        """
        analyses = []
        
        for _, row in df.iterrows():
            company_data = row.to_dict()
            analysis = self.analyze_company(company_data)
            analyses.append(analysis)
        
        return pd.DataFrame(analyses)
    
    def compare_to_sector(self, company_data: Dict, 
                          sector_df: pd.DataFrame) -> Dict:
        """
        Compare a company's metrics to its sector averages
        """
        comparison = {}
        
        metrics = ["pe_ratio", "pb_ratio", "dividend_yield", "roe"]
        
        for metric in metrics:
            company_value = company_data.get(metric)
            sector_avg = sector_df[metric].mean()
            sector_median = sector_df[metric].median()
            
            if company_value and not pd.isna(sector_avg):
                comparison[f"{metric}_vs_sector_avg"] = round(
                    ((company_value - sector_avg) / sector_avg) * 100, 2
                )
                comparison[f"{metric}_sector_avg"] = round(sector_avg, 2)
                comparison[f"{metric}_sector_median"] = round(sector_median, 2)
        
        return comparison


class FinancialRatioCalculator:
    """
    Calculate financial ratios from raw financial statement data
    """
    
    @staticmethod
    def calculate_profitability_ratios(data: Dict) -> Dict:
        """
        Calculate profitability ratios
        """
        ratios = {}
        
        # Gross Profit Margin
        if data.get("revenue") and data.get("gross_profit"):
            ratios["gross_margin"] = round(
                (data["gross_profit"] / data["revenue"]) * 100, 2
            )
        
        # Operating Profit Margin
        if data.get("revenue") and data.get("operating_income"):
            ratios["operating_margin"] = round(
                (data["operating_income"] / data["revenue"]) * 100, 2
            )
        
        # Net Profit Margin
        if data.get("revenue") and data.get("net_income"):
            ratios["net_margin"] = round(
                (data["net_income"] / data["revenue"]) * 100, 2
            )
        
        # Return on Assets (ROA)
        if data.get("net_income") and data.get("total_assets"):
            ratios["roa"] = round(
                (data["net_income"] / data["total_assets"]) * 100, 2
            )
        
        # Return on Equity (ROE)
        if data.get("net_income") and data.get("shareholders_equity"):
            ratios["roe"] = round(
                (data["net_income"] / data["shareholders_equity"]) * 100, 2
            )
        
        # Return on Invested Capital (ROIC)
        if data.get("operating_income") and data.get("invested_capital"):
            ratios["roic"] = round(
                (data["operating_income"] / data["invested_capital"]) * 100, 2
            )
        
        return ratios
    
    @staticmethod
    def calculate_liquidity_ratios(data: Dict) -> Dict:
        """
        Calculate liquidity ratios
        """
        ratios = {}
        
        # Current Ratio
        if data.get("current_assets") and data.get("current_liabilities"):
            ratios["current_ratio"] = round(
                data["current_assets"] / data["current_liabilities"], 2
            )
        
        # Quick Ratio (Acid Test)
        if (data.get("current_assets") and data.get("inventory") 
            and data.get("current_liabilities")):
            ratios["quick_ratio"] = round(
                (data["current_assets"] - data["inventory"]) / data["current_liabilities"], 2
            )
        
        # Cash Ratio
        if data.get("cash") and data.get("current_liabilities"):
            ratios["cash_ratio"] = round(
                data["cash"] / data["current_liabilities"], 2
            )
        
        return ratios
    
    @staticmethod
    def calculate_leverage_ratios(data: Dict) -> Dict:
        """
        Calculate leverage/solvency ratios
        """
        ratios = {}
        
        # Debt to Equity
        if data.get("total_debt") and data.get("shareholders_equity"):
            ratios["debt_to_equity"] = round(
                data["total_debt"] / data["shareholders_equity"], 2
            )
        
        # Debt Ratio
        if data.get("total_debt") and data.get("total_assets"):
            ratios["debt_ratio"] = round(
                data["total_debt"] / data["total_assets"], 2
            )
        
        # Interest Coverage
        if data.get("operating_income") and data.get("interest_expense"):
            if data["interest_expense"] > 0:
                ratios["interest_coverage"] = round(
                    data["operating_income"] / data["interest_expense"], 2
                )
        
        return ratios
    
    @staticmethod
    def calculate_efficiency_ratios(data: Dict) -> Dict:
        """
        Calculate efficiency ratios
        """
        ratios = {}
        
        # Asset Turnover
        if data.get("revenue") and data.get("total_assets"):
            ratios["asset_turnover"] = round(
                data["revenue"] / data["total_assets"], 2
            )
        
        # Inventory Turnover
        if data.get("cost_of_goods_sold") and data.get("inventory"):
            ratios["inventory_turnover"] = round(
                data["cost_of_goods_sold"] / data["inventory"], 2
            )
        
        # Receivables Turnover
        if data.get("revenue") and data.get("accounts_receivable"):
            ratios["receivables_turnover"] = round(
                data["revenue"] / data["accounts_receivable"], 2
            )
        
        return ratios
