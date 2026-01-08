"""
Web Dashboard Package for CSE Stock Research

This package provides a comprehensive Streamlit-based web interface
for analyzing Sri Lankan stocks from the Colombo Stock Exchange.

Features:
- Dashboard overview with market summary
- Individual company analysis with historical trends
- Stock screener with multiple filters
- Sector analysis and comparison
- Portfolio builder tool
- Financial reports viewer

Usage:
    streamlit run web/app.py
"""

from .utils import (
    DataLoader,
    MetricsCalculator,
    ChartHelpers,
    export_to_excel,
    get_market_status,
    get_metric_rating,
    METRIC_THRESHOLDS
)

__all__ = [
    'DataLoader',
    'MetricsCalculator',
    'ChartHelpers',
    'export_to_excel',
    'get_market_status',
    'get_metric_rating',
    'METRIC_THRESHOLDS'
]
