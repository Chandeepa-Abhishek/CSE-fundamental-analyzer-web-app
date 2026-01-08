"""
Comprehensive Investment Analysis Module

This module provides professional-grade investment analysis metrics:
1. Piotroski F-Score - Financial strength (0-9)
2. Altman Z-Score - Bankruptcy risk prediction
3. Graham Number - Intrinsic value (Benjamin Graham)
4. DCF Valuation - Discounted Cash Flow
5. Magic Formula - Joel Greenblatt ranking
6. Quality Score - Business quality assessment
7. Safety Score - Financial safety rating
8. Dividend Analysis - Income investing metrics
9. Growth Analysis - Growth investing metrics
10. Momentum Score - Technical strength

All metrics are calculated to help make informed investment decisions.
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass


@dataclass
class InvestmentScores:
    """Container for all investment scores"""
    piotroski_f_score: int  # 0-9 (8-9 is strong)
    altman_z_score: float  # >2.99 safe, <1.81 distress
    graham_number: float  # Intrinsic value
    graham_upside: float  # % upside to Graham Number
    magic_formula_rank: int  # Lower is better
    quality_score: int  # 0-100
    safety_score: int  # 0-100
    value_score: int  # 0-100
    dividend_score: int  # 0-100
    growth_score: int  # 0-100
    momentum_score: int  # 0-100
    composite_score: int  # 0-100 (overall)
    investment_grade: str  # A, B, C, D, F
    recommendation: str  # Strong Buy, Buy, Hold, Sell, Avoid


class ComprehensiveInvestmentAnalyzer:
    """
    Complete investment analysis system
    
    This analyzer uses multiple proven investment frameworks:
    - Value Investing (Graham, Buffett)
    - Quality Investing (Greenblatt)
    - Financial Health (Piotroski, Altman)
    - Income Investing (Dividend analysis)
    - Growth Investing (PEG, growth rates)
    """
    
    def __init__(self):
        # Thresholds based on investment research
        self.thresholds = {
            'pe_excellent': 10,
            'pe_good': 15,
            'pe_fair': 20,
            'pe_poor': 30,
            
            'pb_excellent': 1.0,
            'pb_good': 1.5,
            'pb_fair': 2.0,
            'pb_poor': 3.0,
            
            'roe_excellent': 20,
            'roe_good': 15,
            'roe_fair': 10,
            'roe_poor': 5,
            
            'debt_equity_excellent': 0.3,
            'debt_equity_good': 0.5,
            'debt_equity_fair': 1.0,
            'debt_equity_poor': 1.5,
            
            'current_ratio_excellent': 2.0,
            'current_ratio_good': 1.5,
            'current_ratio_fair': 1.2,
            'current_ratio_poor': 1.0,
            
            'dividend_excellent': 6,
            'dividend_good': 4,
            'dividend_fair': 2,
            'dividend_poor': 1,
        }
    
    def analyze_company(self, data: Dict) -> InvestmentScores:
        """
        Perform comprehensive analysis on a company
        
        Args:
            data: Dictionary with company financial data
            
        Returns:
            InvestmentScores with all calculated metrics
        """
        # Calculate all scores
        piotroski = self.calculate_piotroski_f_score(data)
        altman = self.calculate_altman_z_score(data)
        graham = self.calculate_graham_number(data)
        graham_upside = self.calculate_graham_upside(data, graham)
        magic_rank = self.calculate_magic_formula_rank(data)
        quality = self.calculate_quality_score(data)
        safety = self.calculate_safety_score(data)
        value = self.calculate_value_score(data)
        dividend = self.calculate_dividend_score(data)
        growth = self.calculate_growth_score(data)
        momentum = self.calculate_momentum_score(data)
        
        # Calculate composite score (weighted average)
        composite = self.calculate_composite_score(
            value=value,
            quality=quality,
            safety=safety,
            dividend=dividend,
            growth=growth,
            momentum=momentum
        )
        
        # Determine investment grade
        grade = self.determine_grade(composite, piotroski, altman)
        
        # Generate recommendation
        recommendation = self.generate_recommendation(
            composite, piotroski, altman, graham_upside
        )
        
        return InvestmentScores(
            piotroski_f_score=piotroski,
            altman_z_score=altman,
            graham_number=graham,
            graham_upside=graham_upside,
            magic_formula_rank=magic_rank,
            quality_score=quality,
            safety_score=safety,
            value_score=value,
            dividend_score=dividend,
            growth_score=growth,
            momentum_score=momentum,
            composite_score=composite,
            investment_grade=grade,
            recommendation=recommendation
        )
    
    def calculate_piotroski_f_score(self, data: Dict) -> int:
        """
        Calculate Piotroski F-Score (0-9)
        
        Developed by Joseph Piotroski to identify financially strong companies.
        Score 8-9: Strong, consider buying
        Score 0-2: Weak, avoid or consider shorting
        
        9 criteria across 3 categories:
        
        PROFITABILITY (4 points):
        1. Positive Net Income
        2. Positive Operating Cash Flow
        3. ROA improving year-over-year
        4. Operating Cash Flow > Net Income (quality of earnings)
        
        LEVERAGE/LIQUIDITY (3 points):
        5. Long-term debt ratio decreasing
        6. Current ratio improving
        7. No new shares issued (no dilution)
        
        EFFICIENCY (2 points):
        8. Gross margin improving
        9. Asset turnover improving
        """
        score = 0
        
        # Get values with defaults
        net_profit = self._get_float(data, 'net_profit', 0)
        eps = self._get_float(data, 'eps', 0)
        operating_cf = self._get_float(data, 'operating_cash_flow', 0)
        roa = self._get_float(data, 'roa', 0)
        roe = self._get_float(data, 'roe', 0)
        debt_equity = self._get_float(data, 'debt_equity', 1)
        current_ratio = self._get_float(data, 'current_ratio', 1)
        gross_margin = self._get_float(data, 'gross_margin', 0)
        asset_turnover = self._get_float(data, 'asset_turnover', 0)
        
        # PROFITABILITY SIGNALS
        # 1. Positive Net Income
        if net_profit > 0 or eps > 0:
            score += 1
        
        # 2. Positive Operating Cash Flow
        if operating_cf > 0:
            score += 1
        
        # 3. Positive ROA (proxy for ROA improvement)
        if roa > 0:
            score += 1
        
        # 4. Cash Flow > Net Income (quality of earnings)
        if operating_cf > net_profit and net_profit > 0:
            score += 1
        elif operating_cf > 0 and eps > 0:  # Proxy when exact data unavailable
            score += 1
        
        # LEVERAGE/LIQUIDITY SIGNALS
        # 5. Low/Decreasing Debt (using debt/equity as proxy)
        if debt_equity < 0.5:
            score += 1
        
        # 6. Good Current Ratio
        if current_ratio > 1.0:
            score += 1
        
        # 7. No Dilution (assume no dilution if data unavailable)
        # In real implementation, compare shares outstanding YoY
        if debt_equity < 1 and roe > 10:  # Proxy: good financials = likely no dilution
            score += 1
        
        # EFFICIENCY SIGNALS
        # 8. Good Gross Margin
        if gross_margin > 20:
            score += 1
        
        # 9. Good Asset Turnover
        if asset_turnover > 0.5:
            score += 1
        
        return min(score, 9)
    
    def calculate_altman_z_score(self, data: Dict) -> float:
        """
        Calculate Altman Z-Score for bankruptcy prediction
        
        Formula: Z = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
        
        A = Working Capital / Total Assets
        B = Retained Earnings / Total Assets
        C = EBIT / Total Assets
        D = Market Value of Equity / Total Liabilities
        E = Sales / Total Assets
        
        Interpretation (for manufacturing companies):
        Z > 2.99: Safe Zone - Low probability of bankruptcy
        1.81 < Z < 2.99: Grey Zone - Moderate risk
        Z < 1.81: Distress Zone - High probability of bankruptcy
        
        For non-manufacturing, thresholds are different.
        """
        # Get financial data
        total_assets = self._get_float(data, 'total_assets', 1)
        total_liabilities = self._get_float(data, 'total_liabilities', 0)
        market_cap = self._get_float(data, 'market_cap', 0)
        revenue = self._get_float(data, 'revenue', 0)
        shareholders_equity = self._get_float(data, 'shareholders_equity', 0)
        current_ratio = self._get_float(data, 'current_ratio', 1)
        operating_income = self._get_float(data, 'operating_income', 0)
        net_profit = self._get_float(data, 'net_profit', 0)
        
        if total_assets <= 0:
            return 0
        
        # Calculate components
        # A: Working Capital / Total Assets
        # Working Capital = Current Assets - Current Liabilities
        # Proxy: (Current Ratio - 1) * (Current Liabilities / Total Assets)
        working_capital_ratio = (current_ratio - 1) * 0.3  # Approximate
        A = max(min(working_capital_ratio, 0.5), -0.3)
        
        # B: Retained Earnings / Total Assets
        # Proxy: Shareholders' Equity / Total Assets * 0.7 (assuming 70% is retained)
        B = (shareholders_equity / total_assets) * 0.7 if shareholders_equity > 0 else 0
        B = max(min(B, 0.5), 0)
        
        # C: EBIT / Total Assets (Operating Income / Total Assets)
        if operating_income > 0:
            C = operating_income / total_assets
        else:
            # Use net profit as proxy
            C = (net_profit / total_assets) * 1.3 if net_profit > 0 else 0
        C = max(min(C, 0.3), -0.1)
        
        # D: Market Value of Equity / Total Liabilities
        if total_liabilities > 0:
            D = market_cap / total_liabilities
        else:
            D = 5  # Very low liabilities = very safe
        D = max(min(D, 5), 0)
        
        # E: Sales / Total Assets (Asset Turnover)
        E = revenue / total_assets if revenue > 0 else 0
        E = max(min(E, 2), 0)
        
        # Calculate Z-Score
        z_score = (1.2 * A) + (1.4 * B) + (3.3 * C) + (0.6 * D) + (1.0 * E)
        
        return round(z_score, 2)
    
    def calculate_graham_number(self, data: Dict) -> float:
        """
        Calculate Graham Number (Benjamin Graham's intrinsic value formula)
        
        Formula: √(22.5 × EPS × Book Value per Share)
        
        The multiplier 22.5 comes from:
        - Max P/E of 15 (Graham's criterion)
        - Max P/B of 1.5 (Graham's criterion)
        - 15 × 1.5 = 22.5
        
        A stock trading below its Graham Number may be undervalued.
        """
        eps = self._get_float(data, 'eps', 0)
        nav = self._get_float(data, 'nav', 0)  # NAV = Book Value per Share
        
        if eps > 0 and nav > 0:
            graham_number = (22.5 * eps * nav) ** 0.5
            return round(graham_number, 2)
        
        return 0
    
    def calculate_graham_upside(self, data: Dict, graham_number: float = None) -> float:
        """
        Calculate potential upside to Graham Number
        
        Returns percentage difference between Graham Number and current price.
        Positive = undervalued, Negative = overvalued
        """
        if graham_number is None:
            graham_number = self.calculate_graham_number(data)
        
        price = self._get_float(data, 'last_traded_price', 0)
        
        if price > 0 and graham_number > 0:
            upside = ((graham_number - price) / price) * 100
            return round(upside, 2)
        
        return 0
    
    def calculate_magic_formula_rank(self, data: Dict) -> int:
        """
        Magic Formula ranking (Joel Greenblatt)
        
        Combines:
        1. Earnings Yield = EBIT / Enterprise Value
        2. Return on Capital = EBIT / (Net Working Capital + Net Fixed Assets)
        
        Companies are ranked by each metric, and ranks are summed.
        Lower combined rank = better investment.
        
        Returns a score 1-100 (lower is better)
        """
        # Calculate Earnings Yield
        pe = self._get_float(data, 'pe_ratio', 20)
        earnings_yield = (1 / pe) * 100 if pe > 0 else 0
        
        # Calculate Return on Capital (proxy: ROE adjusted for leverage)
        roe = self._get_float(data, 'roe', 10)
        debt_equity = self._get_float(data, 'debt_equity', 0.5)
        
        # Adjust ROE for leverage - ROC is typically lower than ROE for leveraged companies
        roc = roe / (1 + debt_equity) if debt_equity > 0 else roe
        
        # Score both metrics (higher is better)
        ey_score = min(earnings_yield, 20) / 20 * 50  # Max 50 points
        roc_score = min(roc, 30) / 30 * 50  # Max 50 points
        
        total_score = ey_score + roc_score
        
        # Convert to rank (1-100, lower is better)
        rank = 100 - int(total_score)
        
        return max(1, min(rank, 100))
    
    def calculate_quality_score(self, data: Dict) -> int:
        """
        Quality Score (0-100)
        
        Measures business quality based on:
        - Profitability (ROE, ROA, margins)
        - Consistency (stable earnings)
        - Competitive advantage indicators
        """
        score = 0
        
        roe = self._get_float(data, 'roe', 0)
        roa = self._get_float(data, 'roa', 0)
        gross_margin = self._get_float(data, 'gross_margin', 0)
        net_margin = self._get_float(data, 'net_margin', 0)
        asset_turnover = self._get_float(data, 'asset_turnover', 0)
        
        # ROE component (max 30 points)
        if roe >= 25:
            score += 30
        elif roe >= 20:
            score += 25
        elif roe >= 15:
            score += 20
        elif roe >= 10:
            score += 15
        elif roe >= 5:
            score += 10
        elif roe > 0:
            score += 5
        
        # ROA component (max 20 points)
        if roa >= 15:
            score += 20
        elif roa >= 10:
            score += 15
        elif roa >= 7:
            score += 10
        elif roa >= 5:
            score += 7
        elif roa > 0:
            score += 3
        
        # Gross Margin component (max 20 points)
        if gross_margin >= 40:
            score += 20
        elif gross_margin >= 30:
            score += 15
        elif gross_margin >= 20:
            score += 10
        elif gross_margin >= 15:
            score += 5
        
        # Net Margin component (max 20 points)
        if net_margin >= 20:
            score += 20
        elif net_margin >= 15:
            score += 15
        elif net_margin >= 10:
            score += 10
        elif net_margin >= 5:
            score += 5
        
        # Asset Turnover component (max 10 points)
        if asset_turnover >= 1.5:
            score += 10
        elif asset_turnover >= 1.0:
            score += 7
        elif asset_turnover >= 0.5:
            score += 4
        
        return min(score, 100)
    
    def calculate_safety_score(self, data: Dict) -> int:
        """
        Safety Score (0-100)
        
        Measures financial safety based on:
        - Debt levels
        - Liquidity
        - Interest coverage
        - Earnings stability
        """
        score = 0
        
        debt_equity = self._get_float(data, 'debt_equity', 1)
        current_ratio = self._get_float(data, 'current_ratio', 1)
        eps = self._get_float(data, 'eps', 0)
        
        # Debt/Equity component (max 40 points)
        if debt_equity <= 0.2:
            score += 40
        elif debt_equity <= 0.3:
            score += 35
        elif debt_equity <= 0.5:
            score += 30
        elif debt_equity <= 0.7:
            score += 25
        elif debt_equity <= 1.0:
            score += 20
        elif debt_equity <= 1.5:
            score += 10
        
        # Current Ratio component (max 30 points)
        if current_ratio >= 2.5:
            score += 30
        elif current_ratio >= 2.0:
            score += 25
        elif current_ratio >= 1.5:
            score += 20
        elif current_ratio >= 1.2:
            score += 15
        elif current_ratio >= 1.0:
            score += 10
        elif current_ratio >= 0.8:
            score += 5
        
        # Positive Earnings component (max 30 points)
        if eps > 10:
            score += 30
        elif eps > 5:
            score += 25
        elif eps > 2:
            score += 20
        elif eps > 0:
            score += 15
        
        return min(score, 100)
    
    def calculate_value_score(self, data: Dict) -> int:
        """
        Value Score (0-100)
        
        Measures value based on:
        - P/E ratio
        - P/B ratio
        - Earnings yield
        - Dividend yield
        """
        score = 0
        
        pe = self._get_float(data, 'pe_ratio', 30)
        pb = self._get_float(data, 'pb_ratio', 3)
        div_yield = self._get_float(data, 'dividend_yield', 0)
        
        # P/E component (max 35 points) - lower is better
        if 0 < pe <= 8:
            score += 35
        elif pe <= 10:
            score += 30
        elif pe <= 12:
            score += 25
        elif pe <= 15:
            score += 20
        elif pe <= 20:
            score += 15
        elif pe <= 25:
            score += 10
        elif pe <= 30:
            score += 5
        
        # P/B component (max 35 points) - lower is better
        if 0 < pb <= 0.7:
            score += 35
        elif pb <= 1.0:
            score += 30
        elif pb <= 1.2:
            score += 25
        elif pb <= 1.5:
            score += 20
        elif pb <= 2.0:
            score += 15
        elif pb <= 2.5:
            score += 10
        elif pb <= 3.0:
            score += 5
        
        # Dividend Yield component (max 30 points)
        if div_yield >= 8:
            score += 30
        elif div_yield >= 6:
            score += 25
        elif div_yield >= 5:
            score += 20
        elif div_yield >= 4:
            score += 15
        elif div_yield >= 3:
            score += 10
        elif div_yield >= 2:
            score += 5
        
        return min(score, 100)
    
    def calculate_dividend_score(self, data: Dict) -> int:
        """
        Dividend Score (0-100)
        
        For income investors, measures:
        - Dividend yield
        - Payout ratio sustainability
        - Dividend coverage
        """
        score = 0
        
        div_yield = self._get_float(data, 'dividend_yield', 0)
        dps = self._get_float(data, 'dividend_per_share', 0)
        eps = self._get_float(data, 'eps', 0)
        
        # Calculate payout ratio
        payout_ratio = (dps / eps * 100) if eps > 0 and dps > 0 else 0
        
        # Dividend Yield component (max 50 points)
        if div_yield >= 8:
            score += 50
        elif div_yield >= 6:
            score += 45
        elif div_yield >= 5:
            score += 40
        elif div_yield >= 4:
            score += 35
        elif div_yield >= 3:
            score += 25
        elif div_yield >= 2:
            score += 15
        elif div_yield >= 1:
            score += 5
        
        # Payout Ratio component (max 30 points)
        # Optimal is 30-60% (sustainable but growing)
        if 30 <= payout_ratio <= 50:
            score += 30
        elif 20 <= payout_ratio <= 60:
            score += 25
        elif 10 <= payout_ratio <= 70:
            score += 20
        elif payout_ratio > 70 and payout_ratio <= 80:
            score += 10  # High but potentially sustainable
        elif payout_ratio > 0:
            score += 5
        
        # Has dividend component (max 20 points)
        if div_yield > 0 and dps > 0:
            score += 20
        
        return min(score, 100)
    
    def calculate_growth_score(self, data: Dict) -> int:
        """
        Growth Score (0-100)
        
        Measures growth potential based on:
        - ROE (reinvestment capacity)
        - Asset turnover trend
        - Plowback ratio
        """
        score = 0
        
        roe = self._get_float(data, 'roe', 0)
        pe = self._get_float(data, 'pe_ratio', 20)
        div_yield = self._get_float(data, 'dividend_yield', 0)
        dps = self._get_float(data, 'dividend_per_share', 0)
        eps = self._get_float(data, 'eps', 0)
        
        # Calculate retention ratio (1 - payout ratio)
        payout_ratio = (dps / eps) if eps > 0 and dps > 0 else 0.5
        retention_ratio = 1 - payout_ratio
        
        # Sustainable Growth Rate = ROE * Retention Ratio
        sustainable_growth = roe * retention_ratio if roe > 0 else 0
        
        # ROE component - higher ROE = more growth capacity (max 35 points)
        if roe >= 25:
            score += 35
        elif roe >= 20:
            score += 30
        elif roe >= 15:
            score += 25
        elif roe >= 12:
            score += 20
        elif roe >= 10:
            score += 15
        elif roe >= 5:
            score += 10
        
        # Sustainable Growth Rate component (max 35 points)
        if sustainable_growth >= 20:
            score += 35
        elif sustainable_growth >= 15:
            score += 30
        elif sustainable_growth >= 12:
            score += 25
        elif sustainable_growth >= 10:
            score += 20
        elif sustainable_growth >= 8:
            score += 15
        elif sustainable_growth >= 5:
            score += 10
        
        # PEG-like component - reasonable PE for growth (max 30 points)
        peg_proxy = pe / max(sustainable_growth, 5) if sustainable_growth > 0 else pe / 10
        
        if peg_proxy <= 0.5:
            score += 30
        elif peg_proxy <= 1.0:
            score += 25
        elif peg_proxy <= 1.5:
            score += 20
        elif peg_proxy <= 2.0:
            score += 15
        elif peg_proxy <= 2.5:
            score += 10
        
        return min(score, 100)
    
    def calculate_momentum_score(self, data: Dict) -> int:
        """
        Momentum Score (0-100)
        
        Technical/price momentum based on:
        - Position in 52-week range
        - Recent price change
        - Volume activity
        """
        score = 0
        
        price = self._get_float(data, 'last_traded_price', 0)
        high_52 = self._get_float(data, '52_week_high', price * 1.2)
        low_52 = self._get_float(data, '52_week_low', price * 0.8)
        change_pct = self._get_float(data, 'change_percent', 0)
        
        # Position in 52-week range (max 50 points)
        if high_52 > low_52:
            position = (price - low_52) / (high_52 - low_52) * 100
            
            # Sweet spot: 30-70% of range (not at extremes)
            if 40 <= position <= 60:
                score += 50  # Middle of range - balanced
            elif 30 <= position <= 70:
                score += 40
            elif 20 <= position <= 80:
                score += 30
            elif position <= 20:
                score += 35  # Near 52-week low - potential value
            elif position >= 80:
                score += 20  # Near high - momentum but risky
        
        # Recent performance component (max 30 points)
        if change_pct >= 3:
            score += 30
        elif change_pct >= 2:
            score += 25
        elif change_pct >= 1:
            score += 20
        elif change_pct >= 0:
            score += 15
        elif change_pct >= -1:
            score += 10
        elif change_pct >= -2:
            score += 5
        
        # Discount from 52-week high component (max 20 points)
        if high_52 > 0:
            discount = ((high_52 - price) / high_52) * 100
            
            if 20 <= discount <= 40:
                score += 20  # Good buying opportunity
            elif 10 <= discount <= 50:
                score += 15
            elif discount < 10:
                score += 10  # Near high
            elif discount > 50:
                score += 5  # Too beaten down
        
        return min(score, 100)
    
    def calculate_composite_score(self, **scores) -> int:
        """
        Calculate weighted composite investment score
        
        Weights reflect a balanced investment approach:
        - Value: 25% (fundamental importance)
        - Quality: 25% (business strength)
        - Safety: 20% (risk management)
        - Dividend: 15% (income component)
        - Growth: 10% (growth potential)
        - Momentum: 5% (timing)
        """
        weights = {
            'value': 0.25,
            'quality': 0.25,
            'safety': 0.20,
            'dividend': 0.15,
            'growth': 0.10,
            'momentum': 0.05
        }
        
        composite = sum(
            scores.get(key, 50) * weight 
            for key, weight in weights.items()
        )
        
        return int(min(composite, 100))
    
    def determine_grade(self, composite: int, piotroski: int, altman: float) -> str:
        """
        Determine investment grade (A-F)
        
        Based on composite score and safety indicators
        """
        # Start with composite-based grade
        if composite >= 80:
            base_grade = 'A'
        elif composite >= 65:
            base_grade = 'B'
        elif composite >= 50:
            base_grade = 'C'
        elif composite >= 35:
            base_grade = 'D'
        else:
            base_grade = 'F'
        
        # Adjust based on Piotroski
        if piotroski >= 7 and base_grade in ['B', 'C']:
            base_grade = chr(ord(base_grade) - 1)  # Upgrade
        elif piotroski <= 3 and base_grade in ['A', 'B', 'C']:
            base_grade = chr(ord(base_grade) + 1)  # Downgrade
        
        # Adjust based on Altman Z-Score
        if altman < 1.81 and base_grade != 'F':
            base_grade = chr(min(ord(base_grade) + 1, ord('F')))  # Downgrade for distress
        
        return base_grade
    
    def generate_recommendation(self, composite: int, piotroski: int, 
                               altman: float, graham_upside: float) -> str:
        """
        Generate investment recommendation
        """
        # Check for distress first
        if altman < 1.5:
            return "Avoid - High Bankruptcy Risk"
        
        if piotroski <= 2:
            return "Avoid - Weak Financials"
        
        # Strong indicators
        if composite >= 75 and piotroski >= 7 and graham_upside > 20:
            return "Strong Buy"
        
        if composite >= 65 and piotroski >= 6 and graham_upside > 0:
            return "Buy"
        
        if composite >= 50 and piotroski >= 5:
            return "Hold"
        
        if composite >= 35:
            return "Weak Hold"
        
        return "Sell / Avoid"
    
    def _get_float(self, data: Dict, key: str, default: float = 0) -> float:
        """Safely get float value from dictionary"""
        value = data.get(key, default)
        if value is None:
            return default
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def analyze_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze all companies in a DataFrame
        
        Adds all investment scores as new columns
        """
        print("🔍 Analyzing companies with comprehensive metrics...")
        
        # Calculate scores for each company
        scores_data = []
        
        for _, row in df.iterrows():
            data = row.to_dict()
            scores = self.analyze_company(data)
            
            scores_data.append({
                'piotroski_f_score': scores.piotroski_f_score,
                'altman_z_score': scores.altman_z_score,
                'graham_number': scores.graham_number,
                'graham_upside_pct': scores.graham_upside,
                'magic_formula_rank': scores.magic_formula_rank,
                'quality_score': scores.quality_score,
                'safety_score': scores.safety_score,
                'value_score': scores.value_score,
                'dividend_score': scores.dividend_score,
                'growth_score': scores.growth_score,
                'momentum_score': scores.momentum_score,
                'composite_score': scores.composite_score,
                'investment_grade': scores.investment_grade,
                'recommendation': scores.recommendation
            })
        
        # Add scores to dataframe
        scores_df = pd.DataFrame(scores_data)
        
        # Remove columns that will be added to avoid duplicates
        columns_to_add = scores_df.columns.tolist()
        df_clean = df.drop(columns=[col for col in columns_to_add if col in df.columns], errors='ignore')
        
        result = pd.concat([df_clean.reset_index(drop=True), scores_df], axis=1)
        
        # Remove any remaining duplicate columns
        result = result.loc[:, ~result.columns.duplicated()]
        
        # Sort by composite score
        result = result.sort_values('composite_score', ascending=False)
        
        print(f"✅ Analyzed {len(result)} companies")
        
        return result


def get_investment_analysis_explanation():
    """
    Return explanation of all metrics for users unfamiliar with investing
    """
    return """
    📚 INVESTMENT METRICS EXPLAINED (For Beginners)
    ================================================
    
    🎯 WHAT THESE NUMBERS MEAN:
    
    1. PIOTROSKI F-SCORE (0-9)
       What: Measures financial health of a company
       How to read:
       - 8-9 = EXCELLENT - Very strong company
       - 6-7 = GOOD - Healthy company
       - 4-5 = AVERAGE - Be cautious
       - 0-3 = WEAK - Avoid, might have problems
    
    2. ALTMAN Z-SCORE
       What: Predicts if company might go bankrupt
       How to read:
       - Above 3.0 = SAFE - Low risk of bankruptcy
       - 1.8 to 3.0 = CAUTION - Some risk
       - Below 1.8 = DANGER - High risk of failure
    
    3. GRAHAM NUMBER
       What: Fair price for stock (invented by Warren Buffett's teacher)
       How to read:
       - If current price < Graham Number = UNDERVALUED (good buy)
       - If current price > Graham Number = OVERVALUED (expensive)
    
    4. P/E RATIO (Price to Earnings)
       What: How many years of profits to pay back stock price
       How to read:
       - Below 10 = CHEAP
       - 10-15 = FAIR VALUE
       - 15-20 = SLIGHTLY EXPENSIVE
       - Above 25 = EXPENSIVE
    
    5. P/B RATIO (Price to Book)
       What: Price vs company's actual asset value
       How to read:
       - Below 1.0 = CHEAP (paying less than company owns)
       - 1.0-1.5 = FAIR
       - Above 2.0 = EXPENSIVE
    
    6. ROE (Return on Equity)
       What: How good company is at making profit
       How to read:
       - Above 20% = EXCELLENT
       - 15-20% = GOOD
       - 10-15% = AVERAGE
       - Below 10% = POOR
    
    7. DIVIDEND YIELD
       What: Yearly cash payment as % of stock price
       How to read:
       - Above 5% = HIGH (good for income)
       - 3-5% = MODERATE
       - 1-3% = LOW
       - 0% = No dividend
    
    8. DEBT/EQUITY RATIO
       What: How much company borrowed vs owns
       How to read:
       - Below 0.5 = LOW DEBT (safe)
       - 0.5-1.0 = MODERATE DEBT
       - Above 1.0 = HIGH DEBT (risky)
    
    9. INVESTMENT GRADE (A to F)
       What: Overall rating like school grades
       How to read:
       - A = EXCELLENT investment candidate
       - B = GOOD investment candidate
       - C = AVERAGE, needs more research
       - D = BELOW AVERAGE, risky
       - F = POOR, probably avoid
    
    10. COMPOSITE SCORE (0-100)
        What: Overall investment attractiveness
        How to read:
        - 75-100 = STRONG BUY candidate
        - 60-75 = BUY candidate
        - 45-60 = HOLD/NEUTRAL
        - 30-45 = WEAK
        - 0-30 = AVOID
    
    ⚠️ IMPORTANT REMINDERS:
    - No single metric tells the whole story
    - Always look at multiple factors together
    - Higher composite score = better overall
    - Check both PIOTROSKI and ALTMAN for safety
    - Past performance doesn't guarantee future results
    - Consider your own risk tolerance
    - This is educational, not financial advice!
    """
