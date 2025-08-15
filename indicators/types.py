"""
指标类型定义和配置类
"""

from dataclasses import dataclass
from typing import List


class IndicatorType:
    """技术指标类型常量"""
    
    # 基础价格指标
    PRICE_CHANGE_15M = "price_change_15m"
    PRICE_CHANGE_1H = "price_change_1h"
    PRICE_CHANGE_24H = "price_change_24h"
    
    # 移动平均指标
    SMA_5 = "sma_5"
    SMA_10 = "sma_10"
    SMA_20 = "sma_20"
    SMA_50 = "sma_50"
    SMA_200 = "sma_200"
    EMA_12 = "ema_12"
    EMA_26 = "ema_26"
    EMA_50 = "ema_50"
    EMA_200 = "ema_200"
    
    # 动量指标
    RSI_14 = "rsi_14"
    RSI_21 = "rsi_21"
    MACD = "macd"
    MACD_SIGNAL = "macd_signal"
    MACD_HISTOGRAM = "macd_histogram"
    
    # 成交量指标
    VOLUME_SMA_20 = "volume_sma_20"
    VOLUME_SPIKE = "volume_spike"
    VOLUME_RATIO = "volume_ratio"
    VOLUME_WEIGHTED_PRICE = "volume_weighted_price"
    
    # 波动率指标
    ATR_14 = "atr_14"
    ATR_21 = "atr_21"
    BOLLINGER_UPPER = "bb_upper_20"
    BOLLINGER_LOWER = "bb_lower_20"
    BOLLINGER_MIDDLE = "bb_middle_20"
    
    # 订单簿深度指标
    BID_ASK_SPREAD = "bid_ask_spread"
    ORDER_BOOK_IMBALANCE = "order_book_imbalance"
    DEPTH_PRESSURE_5PCT = "depth_pressure_5pct"
    
    # 市场情绪指标
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"
    LONG_SHORT_RATIO = "long_short_ratio"
    TOP_TRADER_RATIO = "top_trader_ratio"
    
    @classmethod
    def get_price_indicators(cls) -> List[str]:
        """获取价格相关指标"""
        return [
            cls.PRICE_CHANGE_15M, cls.PRICE_CHANGE_1H, cls.PRICE_CHANGE_24H,
            cls.SMA_20, cls.SMA_50, cls.SMA_200,
            cls.EMA_12, cls.EMA_26, cls.EMA_50, cls.EMA_200,
            cls.RSI_14, cls.RSI_21, cls.MACD,
            cls.ATR_14, cls.BOLLINGER_UPPER, cls.BOLLINGER_LOWER
        ]
    
    @classmethod
    def get_volume_indicators(cls) -> List[str]:
        """获取成交量相关指标"""
        return [
            cls.VOLUME_SMA_20, cls.VOLUME_SPIKE, cls.VOLUME_RATIO,
            cls.VOLUME_WEIGHTED_PRICE
        ]
    
    @classmethod
    def get_orderbook_indicators(cls) -> List[str]:
        """获取订单簿相关指标"""
        return [
            cls.BID_ASK_SPREAD, cls.ORDER_BOOK_IMBALANCE, cls.DEPTH_PRESSURE_5PCT
        ]
    
    @classmethod
    def get_market_indicators(cls) -> List[str]:
        """获取市场情绪指标"""
        return [
            cls.FUNDING_RATE, cls.OPEN_INTEREST, cls.LONG_SHORT_RATIO, cls.TOP_TRADER_RATIO
        ]
    
    @classmethod
    def get_all_indicators(cls) -> List[str]:
        """获取所有指标"""
        return (cls.get_price_indicators() + cls.get_volume_indicators() + 
                cls.get_orderbook_indicators() + cls.get_market_indicators())


@dataclass
class IndicatorConfig:
    """指标配置"""
    name: str                   # 指标名称
    window_size: int           # 计算窗口大小
    update_frequency: str      # 更新频率："15m", "1h", "4h"
    dependencies: List[str]    # 依赖的其他指标
    data_types: List[str]      # 需要的数据类型
    
    @classmethod
    def get_default_configs(cls) -> dict:
        """获取默认指标配置"""
        return {
            # 价格变化指标
            IndicatorType.PRICE_CHANGE_15M: cls(
                name=IndicatorType.PRICE_CHANGE_15M,
                window_size=2,
                update_frequency="15m",
                dependencies=[],
                data_types=["klines"]
            ),
            
            # RSI指标
            IndicatorType.RSI_14: cls(
                name=IndicatorType.RSI_14,
                window_size=15,  # 14 + 1
                update_frequency="15m",
                dependencies=[],
                data_types=["klines"]
            ),
            
            # 移动平均线
            IndicatorType.SMA_20: cls(
                name=IndicatorType.SMA_20,
                window_size=21,  # 20 + 1
                update_frequency="15m",
                dependencies=[],
                data_types=["klines"]
            ),
            
            IndicatorType.SMA_200: cls(
                name=IndicatorType.SMA_200,
                window_size=201,  # 200 + 1
                update_frequency="1h",  # 长周期指标更新频率低
                dependencies=[],
                data_types=["klines"]
            ),
            
            IndicatorType.EMA_200: cls(
                name=IndicatorType.EMA_200,
                window_size=201,  # 200 + 1
                update_frequency="1h",  # 长周期指标更新频率低
                dependencies=[],
                data_types=["klines"]
            ),
            
            # 成交量指标
            IndicatorType.VOLUME_SPIKE: cls(
                name=IndicatorType.VOLUME_SPIKE,
                window_size=21,  # 20 + 1
                update_frequency="15m",
                dependencies=[],
                data_types=["klines"]
            ),
            
            # ATR指标
            IndicatorType.ATR_14: cls(
                name=IndicatorType.ATR_14,
                window_size=15,  # 14 + 1
                update_frequency="15m",
                dependencies=[],
                data_types=["klines"]
            ),
            
            # 订单簿指标
            IndicatorType.BID_ASK_SPREAD: cls(
                name=IndicatorType.BID_ASK_SPREAD,
                window_size=1,   # 实时计算
                update_frequency="1m",
                dependencies=[],
                data_types=["bookDepth"]
            ),
            
            # 市场指标
            IndicatorType.OPEN_INTEREST: cls(
                name=IndicatorType.OPEN_INTEREST,
                window_size=1,   # 直接从metrics读取
                update_frequency="5m",
                dependencies=[],
                data_types=["metrics"]
            ),
        }