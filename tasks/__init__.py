"""
Task implementations for the quantitative trading framework.
"""

from .market_analysis_task import MarketAnalysisTask
from .order_execution_task import OrderExecutionTask

__all__ = [
    'MarketAnalysisTask',
    'OrderExecutionTask'
]