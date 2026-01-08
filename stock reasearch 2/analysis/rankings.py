"""
Company Ranking Module
Scores and ranks companies based on multiple factors
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
import sys
sys.path.append('..')
from config.settings import VALUATION_THRESHOLDS, SCORING_WEIGHTS

logger = logging.getLogger(__name__)


@dataclass
class RankingFactor:
    """Defines a ranking factor"""
    name: str
    column: str
    weight: float
    higher_is_better: bool = True
    transform: str = None  # 'normalize', 'percentile', 'log'


class CompanyRanker:
    """
    Ranks companies using multiple factor scoring
    """
    
    def __init__(self, df: pd.DataFrame = None):
        self.df = df
        self.weights = SCORING_WEIGHTS
    
    def set_data(self, df: pd.DataFrame):
        """Set the data to rank"""
        self.df = df
    
    def _normalize_column(self, series: pd.Series, 
                          higher_is_better: bool = True) -> pd.Series:
        """Normalize a column to 0-100 scale"""
        if series.empty or series.isna().all():
            return series
        
        min_val = series.min()
        max_val = series.max()
        
        if max_val == min_val:
            return pd.Series([50] * len(series), index=series.index)
        
        normalized = (series - min_val) / (max_val - min_val) * 100
        
        if not higher_is_better:
            normalized = 100 - normalized
        
        return normalized
    
    def _percentile_rank(self, series: pd.Series, 
                         higher_is_better: bool = True) -> pd.Series:
        """Convert values to percentile ranks"""
        if series.empty or series.isna().all():
            return series
        
        percentiles = series.rank(pct=True) * 100
        
        if not higher_is_better:
            percentiles = 100 - percentiles
        
        return percentiles
    
    def calculate_value_score(self) -> pd.Series:
        """
        Calculate value investing score
        
        Based on:
        - P/E ratio (lower is better)
        - P/B ratio (lower is better)
        - Margin of safety (higher is better)
        - Earnings yield (higher is better)
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # P/E ratio (lower is better)
        if 'pe_ratio' in self.df.columns:
            pe_valid = self.df['pe_ratio'].notna() & (self.df['pe_ratio'] > 0)
            pe_score = pd.Series(0.0, index=self.df.index)
            pe_score[pe_valid] = self._normalize_column(
                self.df.loc[pe_valid, 'pe_ratio'], 
                higher_is_better=False
            )
            score += pe_score * 0.35
            weights_sum += 0.35
        
        # P/B ratio (lower is better)
        if 'pb_ratio' in self.df.columns:
            pb_valid = self.df['pb_ratio'].notna() & (self.df['pb_ratio'] > 0)
            pb_score = pd.Series(0.0, index=self.df.index)
            pb_score[pb_valid] = self._normalize_column(
                self.df.loc[pb_valid, 'pb_ratio'], 
                higher_is_better=False
            )
            score += pb_score * 0.35
            weights_sum += 0.35
        
        # EPS (higher is better)
        if 'eps' in self.df.columns:
            eps_valid = self.df['eps'].notna() & (self.df['eps'] > 0)
            eps_score = pd.Series(0.0, index=self.df.index)
            eps_score[eps_valid] = self._normalize_column(
                self.df.loc[eps_valid, 'eps'], 
                higher_is_better=True
            )
            score += eps_score * 0.30
            weights_sum += 0.30
        
        # Normalize by actual weights used
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_growth_score(self) -> pd.Series:
        """
        Calculate growth score
        
        Based on:
        - ROE (higher is better)
        - EPS growth (higher is better)
        - Revenue growth (higher is better)
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # ROE (higher is better)
        if 'roe' in self.df.columns:
            roe_valid = self.df['roe'].notna() & (self.df['roe'] > 0)
            roe_score = pd.Series(0.0, index=self.df.index)
            roe_score[roe_valid] = self._normalize_column(
                self.df.loc[roe_valid, 'roe'], 
                higher_is_better=True
            )
            score += roe_score * 0.50
            weights_sum += 0.50
        
        # EPS growth (if available)
        if 'eps_growth' in self.df.columns:
            growth_valid = self.df['eps_growth'].notna()
            growth_score = pd.Series(0.0, index=self.df.index)
            growth_score[growth_valid] = self._normalize_column(
                self.df.loc[growth_valid, 'eps_growth'], 
                higher_is_better=True
            )
            score += growth_score * 0.50
            weights_sum += 0.50
        
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_dividend_score(self) -> pd.Series:
        """
        Calculate dividend score
        
        Based on:
        - Dividend yield (higher is better)
        - Payout ratio (moderate is best - 30-60%)
        - Dividend consistency
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # Dividend yield (higher is better, up to a point)
        if 'dividend_yield' in self.df.columns:
            div_valid = self.df['dividend_yield'].notna() & (self.df['dividend_yield'] > 0)
            div_score = pd.Series(0.0, index=self.df.index)
            div_score[div_valid] = self._normalize_column(
                self.df.loc[div_valid, 'dividend_yield'], 
                higher_is_better=True
            )
            score += div_score * 1.0
            weights_sum += 1.0
        
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_quality_score(self) -> pd.Series:
        """
        Calculate quality score
        
        Based on:
        - ROE (higher is better)
        - Debt/Equity (lower is better)
        - Profit margins
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # ROE
        if 'roe' in self.df.columns:
            roe_valid = self.df['roe'].notna() & (self.df['roe'] > 0)
            roe_score = pd.Series(0.0, index=self.df.index)
            roe_score[roe_valid] = self._normalize_column(
                self.df.loc[roe_valid, 'roe'], 
                higher_is_better=True
            )
            score += roe_score * 0.50
            weights_sum += 0.50
        
        # Debt/Equity (lower is better)
        if 'debt_equity' in self.df.columns:
            de_valid = self.df['debt_equity'].notna() & (self.df['debt_equity'] >= 0)
            de_score = pd.Series(0.0, index=self.df.index)
            de_score[de_valid] = self._normalize_column(
                self.df.loc[de_valid, 'debt_equity'], 
                higher_is_better=False
            )
            score += de_score * 0.50
            weights_sum += 0.50
        
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_momentum_score(self) -> pd.Series:
        """
        Calculate momentum score
        
        Based on:
        - Recent price change
        - Distance from 52-week high
        - Volume trends
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # Price change
        if 'change_percent' in self.df.columns:
            change_valid = self.df['change_percent'].notna()
            change_score = pd.Series(0.0, index=self.df.index)
            change_score[change_valid] = self._percentile_rank(
                self.df.loc[change_valid, 'change_percent'], 
                higher_is_better=True
            )
            score += change_score * 1.0
            weights_sum += 1.0
        
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_safety_score(self) -> pd.Series:
        """
        Calculate safety/stability score
        
        Based on:
        - Low volatility
        - Low debt
        - Large market cap
        - Consistent earnings
        """
        if self.df is None or self.df.empty:
            return pd.Series()
        
        score = pd.Series(0.0, index=self.df.index)
        weights_sum = 0
        
        # Market cap (larger is safer)
        if 'market_cap' in self.df.columns:
            cap_valid = self.df['market_cap'].notna() & (self.df['market_cap'] > 0)
            cap_score = pd.Series(0.0, index=self.df.index)
            cap_score[cap_valid] = self._normalize_column(
                self.df.loc[cap_valid, 'market_cap'], 
                higher_is_better=True
            )
            score += cap_score * 0.50
            weights_sum += 0.50
        
        # Debt/Equity (lower is safer)
        if 'debt_equity' in self.df.columns:
            de_valid = self.df['debt_equity'].notna() & (self.df['debt_equity'] >= 0)
            de_score = pd.Series(0.0, index=self.df.index)
            de_score[de_valid] = self._normalize_column(
                self.df.loc[de_valid, 'debt_equity'], 
                higher_is_better=False
            )
            score += de_score * 0.50
            weights_sum += 0.50
        
        if weights_sum > 0:
            score = score / weights_sum * 100
        
        return score.fillna(0)
    
    def calculate_composite_score(self, 
                                   custom_weights: Dict[str, float] = None) -> pd.DataFrame:
        """
        Calculate composite score combining all factor scores
        
        Returns DataFrame with individual and composite scores
        """
        if self.df is None or self.df.empty:
            return pd.DataFrame()
        
        weights = custom_weights or self.weights
        
        # Calculate individual scores
        scores = pd.DataFrame(index=self.df.index)
        scores['symbol'] = self.df.get('symbol', '')
        scores['name'] = self.df.get('name', '')
        scores['last_traded_price'] = self.df.get('last_traded_price', 0)
        
        scores['value_score'] = self.calculate_value_score()
        scores['growth_score'] = self.calculate_growth_score()
        scores['dividend_score'] = self.calculate_dividend_score()
        scores['quality_score'] = self.calculate_quality_score()
        scores['momentum_score'] = self.calculate_momentum_score()
        scores['safety_score'] = self.calculate_safety_score()
        
        # Calculate composite score
        scores['composite_score'] = (
            scores['value_score'] * weights.get('value_score', 0.25) +
            scores['growth_score'] * weights.get('growth_score', 0.20) +
            scores['dividend_score'] * weights.get('dividend_score', 0.15) +
            scores['quality_score'] * weights.get('quality_score', 0.20) +
            scores['momentum_score'] * weights.get('momentum_score', 0.10) +
            scores['safety_score'] * weights.get('safety_score', 0.10)
        )
        
        # Rank by composite score
        scores['rank'] = scores['composite_score'].rank(ascending=False).astype(int)
        
        # Sort by rank
        scores = scores.sort_values('rank')
        
        return scores
    
    def get_top_stocks(self, n: int = 20, 
                       strategy: str = "composite") -> pd.DataFrame:
        """
        Get top N stocks based on specified strategy
        
        Strategies: composite, value, growth, dividend, quality, momentum, safety
        """
        if self.df is None or self.df.empty:
            return pd.DataFrame()
        
        score_funcs = {
            'value': self.calculate_value_score,
            'growth': self.calculate_growth_score,
            'dividend': self.calculate_dividend_score,
            'quality': self.calculate_quality_score,
            'momentum': self.calculate_momentum_score,
            'safety': self.calculate_safety_score,
        }
        
        if strategy == "composite":
            scores = self.calculate_composite_score()
            return scores.head(n)
        elif strategy in score_funcs:
            score = score_funcs[strategy]()
            result = self.df.copy()
            result['score'] = score
            result['rank'] = score.rank(ascending=False).astype(int)
            result = result.sort_values('score', ascending=False)
            return result.head(n)
        
        return self.df.head(n)
    
    def rank_by_sector(self) -> Dict[str, pd.DataFrame]:
        """
        Rank companies within each sector
        """
        if self.df is None or self.df.empty or 'sector' not in self.df.columns:
            return {}
        
        sector_rankings = {}
        
        for sector in self.df['sector'].dropna().unique():
            sector_df = self.df[self.df['sector'] == sector].copy()
            
            if len(sector_df) > 0:
                sector_ranker = CompanyRanker(sector_df)
                sector_rankings[sector] = sector_ranker.calculate_composite_score()
        
        return sector_rankings
    
    def get_ranking_summary(self) -> pd.DataFrame:
        """
        Get summary showing companies with best rankings across categories
        """
        scores = self.calculate_composite_score()
        
        # Add category rankings
        score_columns = ['value_score', 'growth_score', 'dividend_score', 
                        'quality_score', 'momentum_score', 'safety_score']
        
        for col in score_columns:
            scores[f'{col}_rank'] = scores[col].rank(ascending=False).astype(int)
        
        # Identify best category for each stock
        def get_best_category(row):
            categories = {
                'Value': row['value_score'],
                'Growth': row['growth_score'],
                'Dividend': row['dividend_score'],
                'Quality': row['quality_score'],
                'Momentum': row['momentum_score'],
                'Safety': row['safety_score'],
            }
            return max(categories, key=categories.get)
        
        scores['best_category'] = scores.apply(get_best_category, axis=1)
        
        return scores


class PortfolioSuggester:
    """
    Suggests portfolio composition based on rankings and investment goals
    """
    
    def __init__(self, ranker: CompanyRanker):
        self.ranker = ranker
    
    def suggest_balanced_portfolio(self, 
                                    num_stocks: int = 10,
                                    max_per_sector: int = 3) -> pd.DataFrame:
        """
        Suggest a balanced portfolio with diversification
        """
        scores = self.ranker.calculate_composite_score()
        
        if 'sector' not in self.ranker.df.columns:
            return scores.head(num_stocks)
        
        # Add sector info
        scores = scores.merge(
            self.ranker.df[['symbol', 'sector']], 
            on='symbol', 
            how='left'
        )
        
        # Select stocks with sector diversification
        selected = []
        sector_counts = {}
        
        for _, row in scores.iterrows():
            sector = row.get('sector', 'Unknown')
            
            if sector_counts.get(sector, 0) < max_per_sector:
                selected.append(row)
                sector_counts[sector] = sector_counts.get(sector, 0) + 1
            
            if len(selected) >= num_stocks:
                break
        
        return pd.DataFrame(selected)
    
    def suggest_income_portfolio(self, num_stocks: int = 10) -> pd.DataFrame:
        """
        Suggest portfolio focused on dividend income
        """
        return self.ranker.get_top_stocks(num_stocks, strategy='dividend')
    
    def suggest_growth_portfolio(self, num_stocks: int = 10) -> pd.DataFrame:
        """
        Suggest portfolio focused on growth
        """
        return self.ranker.get_top_stocks(num_stocks, strategy='growth')
    
    def suggest_value_portfolio(self, num_stocks: int = 10) -> pd.DataFrame:
        """
        Suggest portfolio focused on value
        """
        return self.ranker.get_top_stocks(num_stocks, strategy='value')
