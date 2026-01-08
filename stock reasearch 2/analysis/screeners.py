"""
Stock Screeners Module
Implements various investment screening strategies
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
import logging
import sys
sys.path.append('..')
from config.settings import VALUATION_THRESHOLDS

logger = logging.getLogger(__name__)


@dataclass
class ScreeningCriteria:
    """Defines a single screening criterion"""
    name: str
    column: str
    operator: str  # 'gt', 'lt', 'gte', 'lte', 'eq', 'between'
    value: float
    value2: float = None  # For 'between' operator
    weight: float = 1.0  # Importance weight for scoring


class StockScreener:
    """
    Stock screening engine with multiple investment strategies
    """
    
    def __init__(self, df: pd.DataFrame = None):
        self.df = df
        self.thresholds = VALUATION_THRESHOLDS
    
    def set_data(self, df: pd.DataFrame):
        """Set the data to screen"""
        self.df = df
    
    def _apply_criterion(self, df: pd.DataFrame, 
                         criterion: ScreeningCriteria) -> pd.DataFrame:
        """Apply a single screening criterion"""
        col = criterion.column
        
        if col not in df.columns:
            logger.warning(f"Column {col} not found in data")
            return df
        
        # Handle NaN values
        valid_mask = df[col].notna()
        
        if criterion.operator == 'gt':
            mask = valid_mask & (df[col] > criterion.value)
        elif criterion.operator == 'lt':
            mask = valid_mask & (df[col] < criterion.value)
        elif criterion.operator == 'gte':
            mask = valid_mask & (df[col] >= criterion.value)
        elif criterion.operator == 'lte':
            mask = valid_mask & (df[col] <= criterion.value)
        elif criterion.operator == 'eq':
            mask = valid_mask & (df[col] == criterion.value)
        elif criterion.operator == 'between':
            mask = valid_mask & (df[col] >= criterion.value) & (df[col] <= criterion.value2)
        else:
            mask = valid_mask
        
        return df[mask]
    
    def screen(self, criteria: List[ScreeningCriteria]) -> pd.DataFrame:
        """Apply multiple screening criteria"""
        if self.df is None or self.df.empty:
            return pd.DataFrame()
        
        result = self.df.copy()
        
        for criterion in criteria:
            result = self._apply_criterion(result, criterion)
            logger.info(f"After {criterion.name}: {len(result)} stocks remaining")
        
        return result
    
    # ==================== PREDEFINED STRATEGIES ====================
    
    def screen_value_investing(self) -> pd.DataFrame:
        """
        Benjamin Graham / Warren Buffett Style Value Investing
        
        Criteria:
        - Low P/E ratio (< 15)
        - Low P/B ratio (< 1.5)
        - Low Debt/Equity (< 0.5)
        - Positive earnings (EPS > 0)
        - Reasonable dividend yield
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("Low P/E", "pe_ratio", "lt", self.thresholds["pe_ratio_max"]),
            ScreeningCriteria("Low P/B", "pb_ratio", "lt", self.thresholds["pb_ratio_max"]),
        ]
        
        result = self.screen(criteria)
        
        # Sort by P/E (lower is better for value investing)
        if not result.empty and 'pe_ratio' in result.columns:
            result = result.sort_values('pe_ratio', ascending=True)
        
        return result
    
    def screen_dividend_investing(self) -> pd.DataFrame:
        """
        Dividend Income Strategy
        
        Criteria:
        - High dividend yield (> 4%)
        - Sustainable payout ratio (< 70%)
        - Positive earnings
        - Consistent dividend history
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("High Dividend Yield", "dividend_yield", "gt", 
                            self.thresholds["dividend_yield_min"]),
        ]
        
        result = self.screen(criteria)
        
        # Sort by dividend yield (higher is better)
        if not result.empty and 'dividend_yield' in result.columns:
            result = result.sort_values('dividend_yield', ascending=False)
        
        return result
    
    def screen_growth_investing(self) -> pd.DataFrame:
        """
        Growth Investing Strategy
        
        Criteria:
        - High EPS growth (> 10%)
        - High ROE (> 15%)
        - Reasonable P/E for growth
        - Strong revenue growth
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("High ROE", "roe", "gt", self.thresholds["roe_min"]),
        ]
        
        result = self.screen(criteria)
        
        # Sort by ROE (higher is better for growth)
        if not result.empty and 'roe' in result.columns:
            result = result.sort_values('roe', ascending=False)
        
        return result
    
    def screen_garp(self) -> pd.DataFrame:
        """
        Growth At Reasonable Price (GARP) Strategy
        
        Combines value and growth metrics
        Criteria:
        - PEG ratio < 1 (P/E relative to growth)
        - Positive earnings growth
        - Reasonable P/E
        - Good ROE
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("Good ROE", "roe", "gt", 10),
            ScreeningCriteria("P/E not too high", "pe_ratio", "lt", 25),
        ]
        
        result = self.screen(criteria)
        
        # Calculate and filter by PEG if growth data available
        if 'peg_ratio' in result.columns:
            result = result[result['peg_ratio'] < self.thresholds["peg_ratio_max"]]
        
        return result
    
    def screen_quality_investing(self) -> pd.DataFrame:
        """
        Quality Investing Strategy
        
        Focus on high-quality, well-managed companies
        Criteria:
        - High ROE (> 15%)
        - High profit margins
        - Low debt
        - Consistent earnings
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("High ROE", "roe", "gt", self.thresholds["roe_min"]),
        ]
        
        result = self.screen(criteria)
        
        # Additional quality filters if data available
        if 'debt_equity' in result.columns:
            result = result[result['debt_equity'] < self.thresholds["debt_equity_max"]]
        
        # Sort by ROE
        if not result.empty and 'roe' in result.columns:
            result = result.sort_values('roe', ascending=False)
        
        return result
    
    def screen_momentum_investing(self) -> pd.DataFrame:
        """
        Momentum Strategy
        
        Focus on stocks with strong recent performance
        Criteria:
        - Positive price change
        - High volume
        - Price above moving averages
        """
        criteria = [
            ScreeningCriteria("Positive Change", "change_percent", "gt", 0),
        ]
        
        result = self.screen(criteria)
        
        # Sort by price change
        if not result.empty and 'change_percent' in result.columns:
            result = result.sort_values('change_percent', ascending=False)
        
        return result
    
    def screen_bargain_stocks(self) -> pd.DataFrame:
        """
        Deep Value / Bargain Hunting Strategy
        
        Find extremely undervalued stocks
        Criteria:
        - Very low P/E (< 10)
        - P/B < 1 (trading below book value)
        - Price significantly below 52-week high
        """
        criteria = [
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
            ScreeningCriteria("Very Low P/E", "pe_ratio", "lt", 10),
            ScreeningCriteria("Below Book", "pb_ratio", "lt", 1.0),
        ]
        
        result = self.screen(criteria)
        
        # Sort by P/B (lowest first)
        if not result.empty and 'pb_ratio' in result.columns:
            result = result.sort_values('pb_ratio', ascending=True)
        
        return result
    
    def screen_blue_chips(self) -> pd.DataFrame:
        """
        Blue Chip / Large Cap Strategy
        
        Focus on large, established companies
        Criteria:
        - High market cap
        - Established dividend
        - Low volatility
        - Strong brand/market position
        """
        criteria = [
            ScreeningCriteria("Large Market Cap", "market_cap", "gt", 
                            self.thresholds["market_cap_min"]),
            ScreeningCriteria("Positive EPS", "eps", "gt", 0),
        ]
        
        result = self.screen(criteria)
        
        # Sort by market cap
        if not result.empty and 'market_cap' in result.columns:
            result = result.sort_values('market_cap', ascending=False)
        
        return result
    
    def screen_52_week_low(self) -> pd.DataFrame:
        """
        52-Week Low Strategy
        
        Find stocks near their 52-week lows
        (potential turnaround opportunities)
        """
        if self.df is None or self.df.empty:
            return pd.DataFrame()
        
        result = self.df.copy()
        
        # Filter stocks with 52-week data
        if '52_week_low' in result.columns and 'last_traded_price' in result.columns:
            # Calculate % above 52-week low
            result['pct_above_52w_low'] = (
                (result['last_traded_price'] - result['52_week_low']) 
                / result['52_week_low'] * 100
            )
            
            # Filter stocks within 10% of 52-week low
            result = result[result['pct_above_52w_low'] <= 10]
            result = result.sort_values('pct_above_52w_low', ascending=True)
        
        return result
    
    def screen_custom(self, criteria_dict: Dict) -> pd.DataFrame:
        """
        Custom screening with user-defined criteria
        
        criteria_dict format:
        {
            'pe_ratio': {'operator': 'lt', 'value': 15},
            'dividend_yield': {'operator': 'gt', 'value': 5},
            ...
        }
        """
        criteria = []
        
        for col, params in criteria_dict.items():
            criterion = ScreeningCriteria(
                name=f"Custom {col}",
                column=col,
                operator=params.get('operator', 'gt'),
                value=params.get('value', 0),
                value2=params.get('value2'),
            )
            criteria.append(criterion)
        
        return self.screen(criteria)
    
    def get_all_strategies(self) -> Dict[str, Callable]:
        """Return dictionary of all available strategies"""
        return {
            "value": self.screen_value_investing,
            "dividend": self.screen_dividend_investing,
            "growth": self.screen_growth_investing,
            "garp": self.screen_garp,
            "quality": self.screen_quality_investing,
            "momentum": self.screen_momentum_investing,
            "bargain": self.screen_bargain_stocks,
            "blue_chip": self.screen_blue_chips,
            "52_week_low": self.screen_52_week_low,
        }
    
    def run_all_strategies(self) -> Dict[str, pd.DataFrame]:
        """Run all strategies and return results"""
        results = {}
        strategies = self.get_all_strategies()
        
        for name, strategy_func in strategies.items():
            logger.info(f"Running {name} strategy...")
            results[name] = strategy_func()
            logger.info(f"{name}: Found {len(results[name])} stocks")
        
        return results
    
    def get_strategy_summary(self) -> pd.DataFrame:
        """
        Get summary of how stocks perform across different strategies
        Shows which stocks appear in multiple strategies
        """
        if self.df is None or self.df.empty:
            return pd.DataFrame()
        
        results = self.run_all_strategies()
        
        # Create summary
        summary_data = []
        
        for _, row in self.df.iterrows():
            symbol = row.get('symbol', '')
            
            strategies_matched = []
            for strategy_name, strategy_df in results.items():
                if symbol in strategy_df['symbol'].values:
                    strategies_matched.append(strategy_name)
            
            summary_data.append({
                'symbol': symbol,
                'name': row.get('name', ''),
                'strategies_count': len(strategies_matched),
                'strategies': ', '.join(strategies_matched),
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('strategies_count', ascending=False)
        
        return summary_df


class SectorScreener:
    """
    Screen stocks within specific sectors
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.screener = StockScreener(df)
    
    def screen_by_sector(self, sector: str, 
                         strategy: str = "value") -> pd.DataFrame:
        """Screen stocks within a specific sector"""
        if 'sector' not in self.df.columns:
            logger.warning("Sector column not found")
            return pd.DataFrame()
        
        # Filter by sector
        sector_df = self.df[
            self.df['sector'].str.contains(sector, case=False, na=False)
        ]
        
        # Apply strategy to sector
        self.screener.set_data(sector_df)
        strategies = self.screener.get_all_strategies()
        
        if strategy in strategies:
            return strategies[strategy]()
        
        return sector_df
    
    def compare_sectors(self) -> pd.DataFrame:
        """Compare average metrics across sectors"""
        if 'sector' not in self.df.columns:
            return pd.DataFrame()
        
        metrics = ['pe_ratio', 'pb_ratio', 'dividend_yield', 'roe']
        available_metrics = [m for m in metrics if m in self.df.columns]
        
        sector_stats = self.df.groupby('sector')[available_metrics].agg(['mean', 'median', 'count'])
        
        return sector_stats
