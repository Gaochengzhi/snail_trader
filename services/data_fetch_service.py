"""
DataFetchService: 最简化版本，只验证基本数据流功能
"""

import asyncio
import random
from datetime import datetime
from typing import Dict, Any

from core import AbstractService, MessageBus, Topics, Ports
import duckdb


class DataFetchService(AbstractService):

    def __init__(self, config: Dict[str, Any]):
        super().__init__("data_fetch", config)
        self.message_bus = MessageBus("data_fetch")
        self.fetch_interval = config.framework.fetch_interval_minutes

    async def initialize(self):
        await super().initialize()
        self.database = duckdb.connect(self.config.db.duckdb_path)

    async def async_run(self):

        while self._running:
            market_data = {
                "timestamp": datetime.now().timestamp(),
                "BTC/USDT": round(random.uniform(40000, 70000), 2),
                "ETH/USDT": round(random.uniform(2000, 4000), 2),
            }

            timestamp_str = datetime.fromtimestamp(market_data["timestamp"]).strftime(
                "%H:%M:%S"
            )
            self.log(
                "INFO",
                f"[{timestamp_str}] BTC: ${market_data['BTC/USDT']:.2f}, ETH: ${market_data['ETH/USDT']:.2f}",
            )

            try:
                await self.message_bus.publish(
                    Topics.MARKET_DATA, market_data, port=Ports.MARKET_DATA
                )
            except Exception:
                pass

            await asyncio.sleep(self.fetch_interval)

    async def cleanup(self):
        """清理资源"""
        await super().cleanup()
        await self.message_bus.cleanup()
        self.log("INFO", "DataFetchService cleaned up")
