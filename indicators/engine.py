"""
指标计算引擎 - 从services中移出的独立模块
"""

import asyncio
from collections import deque, defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional, Deque

from .calculator import IndicatorCalculator
from .types import IndicatorType, IndicatorConfig
from utils.data_query import IndicatorUpdate


class RollingWindow:
    """滚动窗口数据结构"""
    
    def __init__(self, max_size: int):
        self.data: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self.max_size = max_size
    
    def append(self, item: Dict[str, Any]):
        """添加新数据点"""
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


class IndicatorEngine:
    """指标计算引擎主控制器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # 指标配置
        self.indicator_configs = IndicatorConfig.get_default_configs()
        
        # 滚动窗口数据：symbol -> {interval -> RollingWindow}
        max_window = config.get("window", {}).get("max_window_size", 250)
        self.rolling_windows: Dict[str, Dict[str, RollingWindow]] = defaultdict(
            lambda: defaultdict(lambda: RollingWindow(max_window))
        )
        
        # 计算结果缓存：symbol -> {indicator -> {timestamp -> value}}
        self.indicator_cache: Dict[str, Dict[str, Dict[float, float]]] = defaultdict(lambda: defaultdict(dict))
        
        # 最后更新时间：indicator -> timestamp
        self.last_update_times: Dict[str, float] = {}
        
        # 计算器实例
        self.calculator = IndicatorCalculator()
    
    def update_rolling_window(self, symbol: str, interval: str, new_data: Dict[str, Any]):
        """更新滚动窗口数据"""
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
            
            # 清理旧缓存
            cache_limit = self.config.get("cache", {}).get("max_entries_per_symbol", 100)
            cache_dict = self.indicator_cache[symbol][indicator]
            if len(cache_dict) > cache_limit:
                oldest_timestamps = sorted(cache_dict.keys())[:-cache_limit]
                for ts in oldest_timestamps:
                    del cache_dict[ts]
        
        return computed_value
    
    def _should_update(self, indicator: str, timestamp: float) -> bool:
        """判断指标是否需要更新"""
        if indicator not in self.indicator_configs:
            return True
        
        config = self.indicator_configs[indicator]
        last_update = self.last_update_times.get(indicator, 0)
        
        # 根据更新频率判断
        if config.update_frequency == "15m":
            min_interval = 15 * 60
        elif config.update_frequency == "1h":
            min_interval = 60 * 60
        elif config.update_frequency == "4h":
            min_interval = 4 * 60 * 60
        else:
            min_interval = 15 * 60
        
        return timestamp >= last_update + min_interval
    
    def _get_latest_cached(self, symbol: str, indicator: str) -> Optional[float]:
        """获取最近的缓存值"""
        if (symbol not in self.indicator_cache or 
            indicator not in self.indicator_cache[symbol]):
            return None
        
        cache_dict = self.indicator_cache[symbol][indicator]
        if not cache_dict:
            return None
        
        latest_timestamp = max(cache_dict.keys())
        return cache_dict[latest_timestamp]
    
    async def _compute_indicator(self, symbol: str, indicator: str, interval: str) -> Optional[float]:
        """计算指标值"""
        window = self.rolling_windows[symbol][interval]
        if window.size() == 0:
            return None
        
        config = self.indicator_configs.get(indicator)
        if config is None:
            return None
        
        # 检查数据是否足够
        if window.size() < config.window_size:
            return None
        
        try:
            # 根据指标类型进行计算
            if indicator == IndicatorType.PRICE_CHANGE_15M:
                closes = window.get_values('close')
                if len(closes) >= 2:
                    return self.calculator.price_change(closes[-1], closes[-2])
            
            elif indicator == IndicatorType.RSI_14:
                closes = window.get_values('close')
                return self.calculator.rsi(closes, 14)
            
            elif indicator == IndicatorType.SMA_20:
                closes = window.get_values('close')
                return self.calculator.sma(closes, 20)
            
            elif indicator == IndicatorType.SMA_200:
                closes = window.get_values('close')
                return self.calculator.sma(closes, 200)
            
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
        if window.size() > 0:
            raw_data_snapshot = dict(list(window.data)[-1])  # 复制最新数据
        
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