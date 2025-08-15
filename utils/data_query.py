"""
数据查询接口和时间范围工具类
支持多种数据类型：K线、订单簿深度、市场指标等
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from enum import Enum


class IntervalType(Enum):
    """K线时间间隔类型"""
    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1d"
    WEEK_1 = "1w"


class DataType(Enum):
    """支持的数据类型"""
    KLINES = "klines"                    # K线数据
    INDEX_PRICE_KLINES = "indexPriceKlines"  # 指数价格K线
    MARK_PRICE_KLINES = "markPriceKlines"    # 标记价格K线
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"  # 溢价指数K线
    BOOK_DEPTH = "bookDepth"             # 订单簿深度
    BOOK_TICKER = "bookTicker"           # 最优挂单
    TRADES = "trades"                    # 单笔交易
    AGG_TRADES = "aggTrades"             # 聚合交易
    METRICS = "metrics"                  # 市场指标


@dataclass
class TimeRange:
    """时间范围定义"""

    start: datetime
    end: datetime

    @classmethod
    def last_minutes(cls, minutes: int) -> "TimeRange":
        """最近N分钟"""
        end = datetime.now()
        start = end - timedelta(minutes=minutes)
        return cls(start, end)

    @classmethod
    def last_hours(cls, hours: int) -> "TimeRange":
        """最近N小时"""
        end = datetime.now()
        start = end - timedelta(hours=hours)
        return cls(start, end)

    @classmethod
    def last_days(cls, days: int) -> "TimeRange":
        """最近N天"""
        end = datetime.now()
        start = end - timedelta(days=days)
        return cls(start, end)

    @classmethod
    def last_months(cls, months: int) -> "TimeRange":
        """最近N个月（近似）"""
        end = datetime.now()
        start = end - timedelta(days=months * 30)
        return cls(start, end)

    def is_historical(self, threshold_hours: int = 1) -> bool:
        """判断是否为历史数据（超过阈值时间）"""
        return self.end < datetime.now() - timedelta(hours=threshold_hours)

    def is_realtime(self, threshold_hours: int = 1) -> bool:
        """判断是否为实时数据（在阈值时间内）"""
        return self.start > datetime.now() - timedelta(hours=threshold_hours)

    def to_timestamp(self) -> tuple:
        """转换为时间戳元组"""
        return (self.start.timestamp(), self.end.timestamp())


@dataclass
class DataQuery:
    """统一数据查询接口"""
    symbols: List[str]           # 交易对列表，["*"]表示全市场
    time_range: TimeRange        # 时间范围
    intervals: List[str]         # K线间隔列表，如["15m", "1h"]
    indicators: List[str]        # 指标列表，如["rsi_14", "sma_200"]
    data_types: List[str] = None # 数据类型列表，默认为["klines"]

    def __post_init__(self):
        """验证查询参数"""
        if not self.symbols:
            raise ValueError("symbols不能为空")
        if not self.intervals:
            raise ValueError("intervals不能为空")
        
        # 设置默认数据类型
        if self.data_types is None:
            self.data_types = [DataType.KLINES.value]

        # 验证时间间隔格式
        valid_intervals = [e.value for e in IntervalType]
        for interval in self.intervals:
            if interval not in valid_intervals:
                raise ValueError(f"无效的时间间隔: {interval}")
        
        # 验证数据类型
        valid_data_types = [e.value for e in DataType]
        for data_type in self.data_types:
            if data_type not in valid_data_types:
                raise ValueError(f"无效的数据类型: {data_type}")

    def is_all_symbols(self) -> bool:
        """是否查询全市场"""
        return "*" in self.symbols

    def get_specific_symbols(self) -> List[str]:
        """获取具体的交易对列表（排除通配符）"""
        return [s for s in self.symbols if s != "*"]
    
    def needs_intervals(self) -> bool:
        """是否需要时间间隔（K线类数据需要，其他数据类型不需要）"""
        kline_types = {
            DataType.KLINES.value,
            DataType.INDEX_PRICE_KLINES.value,
            DataType.MARK_PRICE_KLINES.value,
            DataType.PREMIUM_INDEX_KLINES.value
        }
        return any(dt in kline_types for dt in self.data_types)


@dataclass
class DataResponse:
    """数据查询响应（同步REQ/REP模式）"""

    query: DataQuery  # 原始查询
    raw_data: Dict[str, Any]  # 原始K线数据
    indicators: Dict[str, float]  # 计算后的指标值
    timestamp: float  # 响应时间戳
    data_sources: List[str]  # 数据来源列表（"duckdb", "api"）

    def get_indicator(self, symbol: str, indicator: str) -> Optional[float]:
        """获取特定指标值"""
        key = f"{symbol}_{indicator}"
        return self.indicators.get(key)

    def get_symbol_data(self, symbol: str, interval: str) -> Optional[Dict]:
        """获取特定交易对的K线数据"""
        key = f"{symbol}_{interval}"
        return self.raw_data.get(key)


@dataclass
class IndicatorUpdate:
    """指标更新消息（异步推送模式）"""

    symbol: str  # 交易对
    timestamp: float  # 计算时间戳
    interval: str  # K线间隔
    indicators: Dict[str, float]  # 指标值字典
    raw_data_snapshot: Optional[Dict]  # 最新的原始数据快照（可选）

    def to_message(self) -> Dict[str, Any]:
        """转换为消息格式供message bus传输"""
        return {
            "type": "indicator_update",
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "interval": self.interval,
            "indicators": self.indicators,
            "raw_data_snapshot": self.raw_data_snapshot,
        }


@dataclass
class MarketScanResult:
    """市场扫描结果（异步推送模式）"""

    timestamp: float
    scan_type: str  # "volume_spike", "price_breakout", "custom"
    results: List[Dict[str, Any]]  # 扫描结果列表
    metadata: Dict[str, Any]  # 扫描元数据

    def to_message(self) -> Dict[str, Any]:
        """转换为消息格式供message bus传输"""
        return {
            "type": "market_scan_result",
            "timestamp": self.timestamp,
            "scan_type": self.scan_type,
            "results": self.results,
            "metadata": self.metadata,
        }


class IndicatorType:
    """内置指标类型常量"""

    # 价格指标
    PRICE_CHANGE_15M = "price_change_15m"
    PRICE_CHANGE_1H = "price_change_1h"
    PRICE_CHANGE_24H = "price_change_24h"

    # 技术指标
    RSI_14 = "rsi_14"
    RSI_21 = "rsi_21"
    SMA_20 = "sma_20"
    SMA_50 = "sma_50"
    SMA_200 = "sma_200"
    EMA_12 = "ema_12"
    EMA_26 = "ema_26"

    # 成交量指标
    VOLUME_SMA_20 = "volume_sma_20"
    VOLUME_SPIKE = "volume_spike"  # 成交量异常检测
    VOLUME_RATIO = "volume_ratio"  # 成交量比率

    # 波动率指标
    ATR_14 = "atr_14"
    BOLLINGER_UPPER = "bb_upper"
    BOLLINGER_LOWER = "bb_lower"

    @classmethod
    def get_all_indicators(cls) -> List[str]:
        """获取所有可用指标"""
        return [
            cls.PRICE_CHANGE_15M,
            cls.PRICE_CHANGE_1H,
            cls.PRICE_CHANGE_24H,
            cls.RSI_14,
            cls.RSI_21,
            cls.SMA_20,
            cls.SMA_50,
            cls.SMA_200,
            cls.EMA_12,
            cls.EMA_26,
            cls.VOLUME_SMA_20,
            cls.VOLUME_SPIKE,
            cls.VOLUME_RATIO,
            cls.ATR_14,
            cls.BOLLINGER_UPPER,
            cls.BOLLINGER_LOWER,
        ]
