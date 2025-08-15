"""
数据服务 - 简单的REQ/REP接口，连接IndicatorEngine和DataAccessLayer
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional

from core import AbstractService, MessageBus
from services.data_access_layer import DataAccessLayer
from services.indicator_engine import IndicatorEngine
from utils.data_query import DataQuery, DataResponse


class DataService(AbstractService):
    """数据服务 - 纯REQ/REP接口，不做任何主动操作"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("data_service", config)
        
        self.data_layer = DataAccessLayer(getattr(config, "data_access", {}))
        self.indicator_engine = IndicatorEngine(getattr(config, "indicators", {}))
        self.message_bus = MessageBus("data_service")

    async def initialize(self):
        """初始化服务"""
        await super().initialize()
        await self.data_layer.initialize()
        self.log("INFO", "Data service initialized - REQ/REP mode only")

    async def async_run(self):
        """主服务循环 - 纯请求响应模式，无需循环"""
        self.log("INFO", "Data service ready - REQ/REP mode only")
        
        # 只等待取消信号，不做任何主动操作
        try:
            await asyncio.Event().wait()  # 无限等待直到被取消
        except asyncio.CancelledError:
            self.log("INFO", "Data service cancelled")

    async def query_data_and_indicators(self, symbols: List[str], intervals: List[str], 
                                      time_range, indicators: List[str]) -> Dict[str, Any]:
        """查询数据和指标 - 唯一的对外接口"""
        try:
            # 1. 获取原始数据
            raw_data = await self.data_layer.fetch_raw_data(
                symbols, intervals, time_range
            )

            # 2. 更新指标引擎的滚动窗口
            self.log("DEBUG", f"raw_data keys: {list(raw_data.keys())}")
            for key, data_list in raw_data.items():
                self.log("DEBUG", f"Processing key: {key}, data_list type: {type(data_list)}")
                if not data_list:
                    continue
                    
                parts = key.split("_")
                if len(parts) == 2:
                    symbol, interval = parts[0], parts[1]
                    self.log("DEBUG", f"Processing {symbol}_{interval} with {len(data_list)} data points")
                    # 更新所有历史数据点（EMA200需要足够的历史数据）
                    for i, data_point in enumerate(data_list):
                        if not isinstance(data_point, dict):
                            self.log("ERROR", f"data_point[{i}] is not a dict: type={type(data_point)}, value={data_point}")
                            continue
                        self.indicator_engine.update_rolling_window(
                            symbol, interval, data_point
                        )
                    self.log("DEBUG", f"Updated rolling window for {symbol}_{interval} with {len(data_list)} data points")

            # 3. 计算指标
            indicators_result = {}
            timestamp = datetime.now().timestamp()

            for symbol in symbols:
                for indicator in indicators:
                    try:
                        value = await self.indicator_engine.get_indicator(
                            symbol, indicator, timestamp, intervals[0]
                        )
                        if value is not None:
                            indicators_result[f"{symbol}_{indicator}"] = value
                    except Exception as e:
                        self.log("ERROR", f"Failed to compute {indicator} for {symbol}: {e}")

            return {
                "raw_data": raw_data,
                "indicators": indicators_result,
                "timestamp": timestamp
            }

        except Exception as e:
            self.log("ERROR", f"Query processing failed: {e}")
            raise

    async def cleanup(self):
        """清理资源"""
        await super().cleanup()
        await self.data_layer.cleanup()
        await self.indicator_engine.cleanup()
        await self.message_bus.cleanup()
        self.log("INFO", "DataService cleaned up")
