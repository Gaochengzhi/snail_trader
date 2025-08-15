"""
指标计算引擎 - 内存中的高效指标计算和缓存
"""

import asyncio
import numpy as np
from collections import deque, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Deque
from dataclasses import dataclass

from indicators.types import IndicatorType, IndicatorConfig
from utils.data_query import IndicatorUpdate
    

class RollingWindow:
    """滚动窗口数据结构"""
    
    def __init__(self, max_size: int):
        self.data: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self.max_size = max_size
    
    def append(self, item: Dict[str, Any]):
        """添加新数据点 - 基于时间戳去重"""
        if not item:
            return
            
        if not isinstance(item, dict):
            print(f"[ERROR] RollingWindow.append: item is not a dict, type={type(item)}, value={item}")
            return
            
        if 'open_time' not in item:
            print(f"[ERROR] RollingWindow.append: item missing open_time key, item={item}")
            return
        
        new_timestamp = item['open_time']
        
        # 检查是否已存在相同时间戳的数据
        for existing_item in self.data:
            if existing_item.get('open_time') == new_timestamp:
                return  # 跳过重复数据
        
        self.data.append(item)
    
    def get_values(self, field: str) -> List[float]:
        """获取指定字段的值列表"""
        return [float(item[field]) for item in self.data if field in item]
    
    def get_latest(self, field: str) -> Optional[float]:
        """获取最新值"""
        if self.data and field in self.data[-1]:
            return float(self.data[-1][field])
        return None
    
    def is_full(self) -> bool:
        """检查窗口是否已满"""
        return len(self.data) == self.max_size
    
    def size(self) -> int:
        """当前窗口大小"""
        return len(self.data)


class IndicatorCalculator:
    """指标计算器 - 包含各种技术指标的计算逻辑"""
    
    @staticmethod
    def price_change(current: float, previous: float) -> float:
        """价格变化百分比"""
        if previous == 0:
            return 0.0
        return (current - previous) / previous
    
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
        
        # 使用EMA公式：EMA = (Close * (2/(period+1))) + (Previous EMA * (1 - (2/(period+1))))
        multiplier = 2.0 / (period + 1)
        return (values[-1] * multiplier) + (previous_ema * (1 - multiplier))
    
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


class IndicatorEngine:
    """指标计算引擎主控制器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # 指标配置
        self.indicator_configs = self._load_indicator_configs()
        
        # 滚动窗口数据：symbol -> {interval -> RollingWindow}
        self.rolling_windows: Dict[str, Dict[str, RollingWindow]] = defaultdict(lambda: defaultdict(lambda: None))
        
        # 计算结果缓存：symbol -> {indicator -> {timestamp -> value}}
        self.indicator_cache: Dict[str, Dict[str, Dict[float, float]]] = defaultdict(lambda: defaultdict(dict))
        
        # 最后更新时间：indicator -> timestamp
        self.last_update_times: Dict[str, float] = {}
        
        # 计算器实例
        self.calculator = IndicatorCalculator()
    
    def _load_indicator_configs(self) -> Dict[str, IndicatorConfig]:
        """加载指标配置"""
        return IndicatorConfig.get_default_configs()
    
    def update_rolling_window(self, symbol: str, interval: str, new_data: Dict[str, Any]):
        """更新滚动窗口数据"""
        # 确保窗口已初始化
        if self.rolling_windows[symbol][interval] is None:
            # 找到所有相关指标需要的最大窗口大小
            max_window_size = max(
                config.window_size for config in self.indicator_configs.values()
            )
            self.rolling_windows[symbol][interval] = RollingWindow(max_window_size)
        
        # 添加新数据
        self.rolling_windows[symbol][interval].append(new_data)
    
    async def get_indicator(self, symbol: str, indicator: str, timestamp: float, interval: str = "15m") -> Optional[float]:
        """获取指标值 - 支持智能缓存"""
        # 检查缓存
        if (symbol in self.indicator_cache and 
            indicator in self.indicator_cache[symbol] and 
            timestamp in self.indicator_cache[symbol][indicator]):
            return self.indicator_cache[symbol][indicator][timestamp]
        
        # 检查是否需要更新
        if not self._should_update(indicator, timestamp):
            # 返回最近的缓存值
            return self._get_latest_cached(symbol, indicator)
        
        # 计算新值
        computed_value = await self._compute_indicator(symbol, indicator, interval)
        
        # 缓存结果
        if computed_value is not None:
            self.indicator_cache[symbol][indicator][timestamp] = computed_value
            
            # 清理旧缓存（保留最近100个值）
            cache_dict = self.indicator_cache[symbol][indicator]
            if len(cache_dict) > 100:
                oldest_timestamps = sorted(cache_dict.keys())[:-100]
                for ts in oldest_timestamps:
                    del cache_dict[ts]
        
        return computed_value
    
    def _should_update(self, indicator: str, timestamp: float) -> bool:
        """判断指标是否需要更新"""
        if indicator not in self.indicator_configs:
            return True  # 未知指标，每次都计算
        
        config = self.indicator_configs[indicator]
        last_update = self.last_update_times.get(indicator, 0)
        
        # 根据更新频率判断
        if config.update_frequency == "15m":
            min_interval = 15 * 60  # 15分钟
        elif config.update_frequency == "1h":
            min_interval = 60 * 60  # 1小时
        elif config.update_frequency == "4h":
            min_interval = 4 * 60 * 60  # 4小时
        else:
            min_interval = 15 * 60  # 默认15分钟
        
        return timestamp >= last_update + min_interval
    
    def _get_latest_cached(self, symbol: str, indicator: str) -> Optional[float]:
        """获取最近的缓存值"""
        if (symbol not in self.indicator_cache or 
            indicator not in self.indicator_cache[symbol]):
            return None
        
        cache_dict = self.indicator_cache[symbol][indicator]
        if not cache_dict:
            return None
        
        # 返回最新时间戳的值
        latest_timestamp = max(cache_dict.keys())
        return cache_dict[latest_timestamp]
    
    async def _compute_indicator(self, symbol: str, indicator: str, interval: str) -> Optional[float]:
        """计算指标值"""
        window = self.rolling_windows[symbol][interval]
        if window is None or window.size() == 0:
            print(f"DEBUG: No rolling window data for {symbol}_{interval}")
            return None
        
        config = self.indicator_configs.get(indicator)
        if config is None:
            print(f"DEBUG: No config for indicator {indicator}")
            return None
        
        # 检查数据是否足够
        if window.size() < config.window_size:
            print(f"DEBUG: Not enough data for {indicator}: have {window.size()}, need {config.window_size}")
            return None
        
        try:
            # 根据指标类型进行计算
            if indicator == IndicatorType.PRICE_CHANGE_15M:
                closes = window.get_values('close')
                if len(closes) >= 2:
                    return self.calculator.price_change(closes[-1], closes[-2])
            
            elif indicator == IndicatorType.PRICE_CHANGE_1H:
                # 1小时价格变化需要特殊处理，可能需要多个15分钟数据
                closes = window.get_values('close')
                if len(closes) >= 4:  # 假设4个15分钟数据点
                    return self.calculator.price_change(closes[-1], closes[-4])
            
            elif indicator == IndicatorType.RSI_14:
                closes = window.get_values('close')
                return self.calculator.rsi(closes, 14)
            
            elif indicator == IndicatorType.SMA_20:
                closes = window.get_values('close')
                return self.calculator.sma(closes, 20)
            
            elif indicator == IndicatorType.SMA_200:
                closes = window.get_values('close')
                return self.calculator.sma(closes, 200)
            
            elif indicator == IndicatorType.EMA_200:
                closes = window.get_values('close')
                return self.calculator.ema(closes, 200)
            
            elif indicator == IndicatorType.VOLUME_SPIKE:
                volumes = window.get_values('volume')
                return self.calculator.volume_spike(volumes, 20, 2.0)
            
            elif indicator == IndicatorType.ATR_14:
                highs = window.get_values('high')
                lows = window.get_values('low')
                closes = window.get_values('close')
                return self.calculator.atr(highs, lows, closes, 14)
            
            # 记录更新时间
            self.last_update_times[indicator] = datetime.now().timestamp()
            
        except Exception as e:
            # 计算出错，返回None但不中断程序
            print(f"Error computing {indicator} for {symbol}: {e}")
            return None
        
        return None
    
    async def compute_indicators_for_symbol(self, symbol: str, indicators: List[str], interval: str = "15m") -> Dict[str, float]:
        """为单个交易对计算多个指标"""
        results = {}
        timestamp = datetime.now().timestamp()
        
        for indicator in indicators:
            try:
                value = await self.get_indicator(symbol, indicator, timestamp, interval)
                if value is not None:
                    results[indicator] = value
            except Exception as e:
                print(f"Failed to compute {indicator} for {symbol}: {e}")
        
        return results
    
    async def create_indicator_update(self, symbol: str, interval: str, indicators: List[str]) -> Optional[IndicatorUpdate]:
        """创建指标更新消息"""
        timestamp = datetime.now().timestamp()
        computed_indicators = await self.compute_indicators_for_symbol(symbol, indicators, interval)
        
        if not computed_indicators:
            return None
        
        # 获取最新的原始数据快照
        window = self.rolling_windows[symbol][interval]
        raw_data_snapshot = None
        if window and window.size() > 0:
            raw_data_snapshot = dict(window.data[-1])  # 复制最新数据
        
        return IndicatorUpdate(
            symbol=symbol,
            timestamp=timestamp,
            interval=interval,
            indicators=computed_indicators,
            raw_data_snapshot=raw_data_snapshot
        )
    
    async def cleanup(self):
        """清理资源"""
        self.rolling_windows.clear()
        self.indicator_cache.clear()
        self.last_update_times.clear()