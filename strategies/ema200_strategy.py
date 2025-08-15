"""
EMA200突破策略 - 简化版本
监控BTC和ETH，计算200天EMA，突破时发送交易任务
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

from core import AbstractService
from utils.time_manager import get_time_manager
from utils.data_query import TimeRange
from utils.config_utils import Config
from services.data_service import DataService
from indicators.types import IndicatorType


class EMA200Strategy(AbstractService):
    """EMA200突破策略 - Linus式简洁实现"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("ema200_strategy", config)

        # 策略参数（使用点号访问配置，符合规范）
        self.symbols = getattr(config, "symbols", ["BTCUSDT", "ETHUSDT"])
        self.ema_period = 200  # 固定EMA200周期
        self.interval = getattr(config, "interval", "4h")  # 默认4小时

        # 数据服务（延迟初始化）
        self.data_service = None
        self.data_service_config = config

        # 时间管理
        self.time_manager = get_time_manager()

        # 策略状态
        self.last_prices: Dict[str, float] = {}
        self.last_emas: Dict[str, float] = {}

        # 执行间隔
        time_control = getattr(config, "time_control", None)
        if time_control:
            self.execution_interval = getattr(
                time_control, "execution_interval_seconds", 300
            )
        else:
            self.execution_interval = 300

    async def initialize(self):
        """初始化策略"""
        await super().initialize()

        # 初始化数据服务
        self.data_service = DataService(self.data_service_config)
        await self.data_service.initialize()

        self.log("INFO", f"EMA200策略初始化完成")
        self.log("INFO", f"监控币种: {self.symbols}")
        self.log("INFO", f"执行模式: {self.time_manager.mode.value}")
        self.log("INFO", f"当前虚拟时间: {self.time_manager.now()}")

    async def async_run(self):
        """主策略循环"""
        import time

        while self._running and self.time_manager.should_continue():
            try:
                execution_start = time.time()
                current_time = self.time_manager.now()
                self.log("INFO", f"[{current_time}] 执行EMA200突破检测")

                # 检查每个交易对
                for symbol in self.symbols:
                    await self._check_symbol_breakout(symbol)

                # 推进时间（回测模式）
                if self.time_manager.is_backtest():
                    self.time_manager.advance_time(4.0)  # 推进4小时

                # 软间隔逻辑：通用的自适应等待
                execution_time = time.time() - execution_start
                wait_time = max(0, self.execution_interval - execution_time)
                await asyncio.sleep(wait_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log("ERROR", f"策略执行错误: {e}")
                await asyncio.sleep(60)

        self.log("INFO", "EMA200策略运行结束")

    async def _check_symbol_breakout(self, symbol: str):
        """检查单个交易对的EMA突破 - 使用IndicatorEngine"""
        try:
            current_time = self.time_manager.now()
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
            
            # 查询足够的历史数据用于EMA200计算
            start_time = current_time - timedelta(days=250)
            time_range = TimeRange(start_time, current_time)

            result = await self.data_service.query_data_and_indicators(
                symbols=[symbol],
                intervals=[self.interval],
                time_range=time_range,
                indicators=[IndicatorType.EMA_200],
            )

            # 获取当前价格
            key = f"{symbol}_{self.interval}"
            self.log("DEBUG", f"result keys: {list(result.keys())}")
            self.log("DEBUG", f"raw_data keys: {list(result['raw_data'].keys())}")
            self.log("DEBUG", f"looking for key: {key}")
            
            klines = result["raw_data"].get(key, [])
            self.log("DEBUG", f"klines type: {type(klines)}, length: {len(klines) if hasattr(klines, '__len__') else 'N/A'}")
            
            if not klines:
                self.log("WARNING", f"{symbol} 无数据返回")
                return

            # 在回测模式下，根据当前时间定位相应的K线记录
            if self.time_manager.is_backtest():
                current_time = self.time_manager.now()
                current_timestamp = int(current_time.timestamp() * 1000)
                
                # 确保klines是列表类型
                if not isinstance(klines, list):
                    self.log("ERROR", f"klines is not a list, type: {type(klines)}, value: {klines}")
                    return
                
                # 找到最接近当前时间的K线记录
                target_kline = None
                for kline in reversed(klines):  # 从最新往前找
                    if not isinstance(kline, dict):
                        self.log("ERROR", f"kline is not a dict, type: {type(kline)}, value: {kline}")
                        continue
                    if "close_time" not in kline:
                        self.log("ERROR", f"kline missing close_time: {kline}")
                        continue
                    if kline["close_time"] <= current_timestamp:
                        target_kline = kline
                        break
                
                if target_kline is None:
                    self.log("WARNING", f"{symbol} 在时间 {current_time} 无对应K线数据")
                    return
                    
                current_price = float(target_kline["close"])
                self.log("DEBUG", f"{symbol} 回测时间 {current_time}, 使用K线时间 {datetime.fromtimestamp(target_kline['close_time']/1000)}, 价格 {current_price}")
            else:
                # 实时模式：使用最新价格
                current_price = float(klines[-1]["close"])

            # 获取EMA200指标
            ema_key = f"{symbol}_{IndicatorType.EMA_200}"
            current_ema = result["indicators"].get(ema_key)

            if current_ema is None:
                self.log("WARNING", f"{symbol} EMA200计算失败")
                return

            # 检查突破
            await self._check_breakout(symbol, current_price, current_ema)

        except Exception as e:
            self.log("ERROR", f"{symbol} 突破检测失败: {e}")

    async def _check_breakout(self, symbol: str, price: float, ema: float):
        """检查突破并发送交易信号"""
        last_price = self.last_prices.get(symbol, 0)
        last_ema = self.last_emas.get(symbol, 0)

        # 更新状态
        self.last_prices[symbol] = price
        self.last_emas[symbol] = ema

        current_time = self.time_manager.now()
        timestamp = int(current_time.timestamp())

        # 发送K线数据（标准OHLC格式）
        kline_data = {
            "timestamp": timestamp,
            "time_str": current_time.strftime("%Y-%m-%d %H:%M"),
            "open": price,  # 简化：使用当前价格
            "high": price,
            "low": price,
            "close": price,
            "volume": 1000,  # 模拟成交量
            "ema200": ema,
        }
        await self.send_webui_data("kline", symbol, kline_data)

        # 发送进度数据
        progress = self._calculate_progress()
        progress_data = {
            "timestamp": timestamp,
            "progress": progress,
            "symbol_count": len(self.symbols),
        }
        await self.send_webui_data("progress", "ALL", progress_data)

        # 首次运行，不检查突破
        if last_price == 0:
            self.log("INFO", f"{symbol} 初始化: 价格={price:.2f}, EMA200={ema:.2f}")
            return

        # 检查向上突破
        if last_price <= last_ema and price > ema:
            await self._send_buy_signal(symbol, price, ema, timestamp)

        # 检查向下跌破
        elif last_price >= last_ema and price < ema:
            await self._send_sell_signal(symbol, price, ema, timestamp)

        else:
            # 无突破，记录当前状态
            position = "上方" if price > ema else "下方"
            self.log(
                "DEBUG",
                f"{symbol} 价格{price:.2f}在EMA200({ema:.2f}){position}，无突破",
            )

    async def _send_buy_signal(
        self, symbol: str, price: float, ema: float, timestamp: int
    ):
        """发送买入信号"""
        current_time = self.time_manager.now()

        self.log("INFO", f"🟢 [{current_time}] {symbol} 向上突破EMA200!")
        self.log("INFO", f"   价格: {price:.2f} > EMA200: {ema:.2f}")
        self.log("INFO", f"   📈 发送买入任务 (模拟下单)")

        # 发送交易信号到WebUI
        signal_data = {
            "timestamp": timestamp,
            "signal_type": "BUY",
            "price": price,
            "ema200": ema,
            "reason": "价格向上突破EMA200",
        }
        await self.send_webui_data("signal", symbol, signal_data)

    async def _send_sell_signal(
        self, symbol: str, price: float, ema: float, timestamp: int
    ):
        """发送卖出信号"""
        current_time = self.time_manager.now()

        self.log("INFO", f"🔴 [{current_time}] {symbol} 向下跌破EMA200!")
        self.log("INFO", f"   价格: {price:.2f} < EMA200: {ema:.2f}")
        self.log("INFO", f"   📉 发送卖出任务 (模拟下单)")

        # 发送交易信号到WebUI
        signal_data = {
            "timestamp": timestamp,
            "signal_type": "SELL",
            "price": price,
            "ema200": ema,
            "reason": "价格向下跌破EMA200",
        }
        await self.send_webui_data("signal", symbol, signal_data)

    def _calculate_progress(self) -> float:
        """计算回测进度百分比"""
        if not self.time_manager.is_backtest():
            return 0.0

        current_time = self.time_manager.now()
        start_time = getattr(self.time_manager, "_start_time", None)
        end_time = getattr(self.time_manager, "backtest_end_time", None)

        if not start_time or not end_time:
            return 0.0

        total_duration = (end_time - start_time).total_seconds()
        elapsed_duration = (current_time - start_time).total_seconds()

        progress = (elapsed_duration / total_duration) * 100
        return min(max(progress, 0.0), 100.0)

    async def cleanup(self):
        """清理资源"""
        if self.data_service is not None:
            await self.data_service.cleanup()
        await super().cleanup()
