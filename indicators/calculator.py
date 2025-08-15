"""
技术指标计算器 - 所有指标的具体计算逻辑
"""

import numpy as np
from typing import List, Optional, Dict, Any


class IndicatorCalculator:
    """技术指标计算器"""
    
    # ============================================================================
    # 基础价格指标
    # ============================================================================
    
    @staticmethod
    def price_change(current: float, previous: float) -> float:
        """价格变化百分比"""
        if previous == 0:
            return 0.0
        return (current - previous) / previous
    
    @staticmethod
    def price_change_from_list(prices: List[float], periods: int = 1) -> Optional[float]:
        """从价格列表计算N周期价格变化"""
        if len(prices) < periods + 1:
            return None
        return IndicatorCalculator.price_change(prices[-1], prices[-(periods + 1)])
    
    # ============================================================================
    # 移动平均指标
    # ============================================================================
    
    @staticmethod
    def sma(values: List[float], period: int) -> Optional[float]:
        """简单移动平均线"""
        if len(values) < period:
            return None
        return sum(values[-period:]) / period
    
    @staticmethod
    def ema(values: List[float], period: int, previous_ema: Optional[float] = None) -> Optional[float]:
        """指数移动平均线"""
        if not values:
            return None
        
        if previous_ema is None:
            # 第一次计算，使用SMA作为初始值
            if len(values) < period:
                return None
            return IndicatorCalculator.sma(values[:period], period)
        
        # EMA公式：EMA = (Close * α) + (Previous EMA * (1 - α))
        # α = 2 / (period + 1)
        alpha = 2.0 / (period + 1)
        return (values[-1] * alpha) + (previous_ema * (1 - alpha))
    
    # ============================================================================
    # 动量指标
    # ============================================================================
    
    @staticmethod
    def rsi(prices: List[float], period: int = 14) -> Optional[float]:
        """相对强弱指标"""
        if len(prices) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return None
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def macd(prices: List[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, Optional[float]]:
        """MACD指标计算"""
        if len(prices) < slow_period:
            return {"macd": None, "signal": None, "histogram": None}
        
        # 计算快线和慢线EMA
        ema_fast = IndicatorCalculator.ema(prices, fast_period)
        ema_slow = IndicatorCalculator.ema(prices, slow_period)
        
        if ema_fast is None or ema_slow is None:
            return {"macd": None, "signal": None, "histogram": None}
        
        macd_line = ema_fast - ema_slow
        
        # 这里简化signal线计算，实际应该用MACD线的EMA
        signal_line = macd_line  # 简化版本
        histogram = macd_line - signal_line
        
        return {
            "macd": macd_line,
            "signal": signal_line,
            "histogram": histogram
        }
    
    # ============================================================================
    # 波动率指标
    # ============================================================================
    
    @staticmethod
    def atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> Optional[float]:
        """平均真实波动幅度"""
        if len(high) < period + 1 or len(low) < period + 1 or len(close) < period + 1:
            return None
        
        true_ranges = []
        for i in range(1, len(high)):
            tr1 = high[i] - low[i]
            tr2 = abs(high[i] - close[i-1])
            tr3 = abs(low[i] - close[i-1])
            true_ranges.append(max(tr1, tr2, tr3))
        
        if len(true_ranges) < period:
            return None
        
        return sum(true_ranges[-period:]) / period
    
    @staticmethod
    def bollinger_bands(prices: List[float], period: int = 20, std_dev: float = 2.0) -> Dict[str, Optional[float]]:
        """布林带指标"""
        if len(prices) < period:
            return {"upper": None, "middle": None, "lower": None}
        
        # 中轨：简单移动平均
        middle = IndicatorCalculator.sma(prices, period)
        if middle is None:
            return {"upper": None, "middle": None, "lower": None}
        
        # 计算标准差
        recent_prices = prices[-period:]
        variance = sum((p - middle) ** 2 for p in recent_prices) / period
        std = variance ** 0.5
        
        # 上下轨
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return {"upper": upper, "middle": middle, "lower": lower}
    
    # ============================================================================
    # 成交量指标
    # ============================================================================
    
    @staticmethod
    def volume_spike(volumes: List[float], period: int = 20, threshold: float = 2.0) -> float:
        """成交量异常检测"""
        if len(volumes) < period + 1:
            return 0.0
        
        current_volume = volumes[-1]
        avg_volume = sum(volumes[-period-1:-1]) / period  # 排除当前成交量
        
        if avg_volume == 0:
            return 0.0
        
        ratio = current_volume / avg_volume
        return ratio if ratio >= threshold else 0.0
    
    @staticmethod
    def volume_weighted_price(prices: List[float], volumes: List[float], period: int = 20) -> Optional[float]:
        """成交量加权价格"""
        if len(prices) < period or len(volumes) < period:
            return None
        
        recent_prices = prices[-period:]
        recent_volumes = volumes[-period:]
        
        total_volume = sum(recent_volumes)
        if total_volume == 0:
            return None
        
        weighted_sum = sum(p * v for p, v in zip(recent_prices, recent_volumes))
        return weighted_sum / total_volume
    
    # ============================================================================
    # 订单簿指标
    # ============================================================================
    
    @staticmethod
    def bid_ask_spread(bid_price: float, ask_price: float) -> float:
        """买卖价差"""
        if bid_price <= 0 or ask_price <= 0:
            return 0.0
        return (ask_price - bid_price) / ask_price
    
    @staticmethod
    def order_book_imbalance(bid_depth: float, ask_depth: float) -> float:
        """订单簿失衡度"""
        total_depth = bid_depth + ask_depth
        if total_depth == 0:
            return 0.0
        return (bid_depth - ask_depth) / total_depth
    
    @staticmethod
    def depth_pressure(depth_data: List[Dict[str, Any]], percentage_threshold: float = 5.0) -> Dict[str, float]:
        """深度压力计算"""
        bid_pressure = 0.0
        ask_pressure = 0.0
        
        for row in depth_data:
            percentage = row.get("percentage", 0)
            notional = row.get("notional", 0)
            
            if abs(percentage) <= percentage_threshold:
                if percentage < 0:  # 买盘
                    bid_pressure += notional
                else:  # 卖盘
                    ask_pressure += notional
        
        return {"bid_pressure": bid_pressure, "ask_pressure": ask_pressure}
    
    # ============================================================================
    # 市场情绪指标
    # ============================================================================
    
    @staticmethod
    def funding_rate_trend(funding_rates: List[float], period: int = 8) -> Optional[float]:
        """资金费率趋势"""
        if len(funding_rates) < period:
            return None
        
        recent_rates = funding_rates[-period:]
        # 简单的趋势计算：最近值 - 平均值
        avg_rate = sum(recent_rates) / len(recent_rates)
        return recent_rates[-1] - avg_rate
    
    @staticmethod 
    def open_interest_change(current_oi: float, previous_oi: float) -> float:
        """持仓量变化"""
        if previous_oi == 0:
            return 0.0
        return (current_oi - previous_oi) / previous_oi