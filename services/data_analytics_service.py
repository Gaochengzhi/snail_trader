"""
DataAnalyticsService: Processes market data and generates analytics.

Features:
- Subscribes to MARKET_DATA events from DataFetchService
- Performs real-time data analysis (moving averages, volatility, etc.)
- Publishes analytics results for strategies to consume
- Integrates with LogService for structured output
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque

from core import AbstractService, MessageBus, Topics, Ports
from utils.log_utils import get_log_utils


class DataAnalyticsService(AbstractService):
    """
    Service responsible for processing market data and generating analytics.

    Subscribes to market data events and performs real-time analysis:
    - Moving averages (SMA, EMA)
    - Volatility calculations
    - Price change detection
    - Volume analysis
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("data_analytics", config)
        self.message_bus = MessageBus("data_analytics")

        # Analytics configuration
        analytics_config = config.get("analytics", {})
        self.window_sizes = analytics_config.get("sma_windows", [5, 10, 20])
        self.ema_alpha = analytics_config.get("ema_alpha", 0.3)
        self.volatility_window = analytics_config.get("volatility_window", 20)

        # Data storage for calculations
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.volume_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.analytics_results: Dict[str, Dict[str, Any]] = {}

        # Integration with LogService
        self.log_service = get_log_utils()
        self.analytics_counter = 0

    async def initialize(self):
        """Initialize analytics service and message subscriptions."""
        await super().initialize()
        await self.message_bus.initialize()

        # Subscribe to market data events
        self.message_bus.register_handler(Topics.MARKET_DATA, self._handle_market_data)

        # Start subscription loop in background
        asyncio.create_task(
            self.message_bus.subscribe_loop(Ports.MARKET_DATA, [Topics.MARKET_DATA])
        )

        self.logger.info("DataAnalytics service initialized")

    async def async_run(self):
        """Main analytics service loop."""
        await self.initialize()

        try:
            self.log_service.log_message(
                "Analytics", "INFO", "Starting Data Analytics Service", "SYSTEM"
            )

            # Main service loop - most work is done in event handlers
            while self._running:
                await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"DataAnalytics service error: {e}")
            raise
        finally:
            await self.cleanup()

    async def _handle_market_data(self, message: Dict[str, Any]):
        """
        Handle incoming market data and perform analytics.

        Args:
            message: ZeroMQ message containing market data
        """
        try:
            data = message.get("data", {})
            symbols_data = data.get("symbols", {})
            timestamp = data.get("timestamp", datetime.now().timestamp())

            # Process each symbol
            analytics_results = {}
            for symbol, symbol_data in symbols_data.items():
                result = await self._analyze_symbol(symbol, symbol_data, timestamp)
                if result:
                    analytics_results[symbol] = result

            # Store and publish results
            if analytics_results:
                self.analytics_results.update(analytics_results)
                await self._publish_analytics(analytics_results, timestamp)

                # Log analytics via LogService
                self._log_analytics_results(analytics_results)

        except Exception as e:
            self.logger.error(f"Error handling market data: {e}")

    async def _analyze_symbol(
        self, symbol: str, symbol_data: Dict[str, Any], timestamp: float
    ) -> Optional[Dict[str, Any]]:
        """
        Perform analytics on a single symbol.

        Args:
            symbol: Symbol identifier (e.g., 'BTC/USDT')
            symbol_data: Price and volume data for the symbol
            timestamp: Data timestamp

        Returns:
            Dictionary containing analytics results or None on error
        """
        try:
            price = symbol_data.get("price", 0)
            volume = symbol_data.get("volume", 0)

            if price <= 0:
                return None

            # Update price and volume history
            self.price_history[symbol].append(price)
            self.volume_history[symbol].append(volume)

            # Calculate analytics
            result = {
                "symbol": symbol,
                "timestamp": timestamp,
                "current_price": price,
                "current_volume": volume,
                "analytics": {},
            }

            # Simple Moving Averages
            sma_results = self._calculate_sma(symbol, self.window_sizes)
            if sma_results:
                result["analytics"]["sma"] = sma_results

            # Exponential Moving Average
            ema_result = self._calculate_ema(symbol, self.ema_alpha)
            if ema_result is not None:
                result["analytics"]["ema"] = ema_result

            # Price volatility (standard deviation)
            volatility = self._calculate_volatility(symbol, self.volatility_window)
            if volatility is not None:
                result["analytics"]["volatility"] = volatility

            # Price momentum (rate of change)
            momentum = self._calculate_momentum(symbol, 5)  # 5-period momentum
            if momentum is not None:
                result["analytics"]["momentum"] = momentum

            # Volume analysis
            volume_analysis = self._analyze_volume(symbol)
            if volume_analysis:
                result["analytics"]["volume"] = volume_analysis

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing symbol {symbol}: {e}")
            return None

    def _calculate_sma(self, symbol: str, windows: List[int]) -> Dict[str, float]:
        """Calculate Simple Moving Averages for different window sizes."""
        prices = list(self.price_history[symbol])
        if len(prices) < min(windows):
            return {}

        sma_results = {}
        for window in windows:
            if len(prices) >= window:
                sma = sum(prices[-window:]) / window
                sma_results[f"sma_{window}"] = round(sma, 2)

        return sma_results

    def _calculate_ema(self, symbol: str, alpha: float) -> Optional[float]:
        """Calculate Exponential Moving Average."""
        prices = list(self.price_history[symbol])
        if len(prices) < 2:
            return None

        # Simple EMA calculation
        ema = prices[0]
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema

        return round(ema, 2)

    def _calculate_volatility(self, symbol: str, window: int) -> Optional[float]:
        """Calculate price volatility (standard deviation)."""
        prices = list(self.price_history[symbol])
        if len(prices) < window:
            return None

        recent_prices = prices[-window:]
        mean_price = sum(recent_prices) / len(recent_prices)

        # Calculate standard deviation
        variance = sum((price - mean_price) ** 2 for price in recent_prices) / len(
            recent_prices
        )
        volatility = variance**0.5

        # Return as percentage of mean price
        return round((volatility / mean_price) * 100, 2)

    def _calculate_momentum(self, symbol: str, periods: int) -> Optional[float]:
        """Calculate price momentum (rate of change)."""
        prices = list(self.price_history[symbol])
        if len(prices) < periods + 1:
            return None

        current_price = prices[-1]
        past_price = prices[-(periods + 1)]

        momentum = ((current_price - past_price) / past_price) * 100
        return round(momentum, 2)

    def _analyze_volume(self, symbol: str) -> Dict[str, Any]:
        """Analyze volume patterns."""
        volumes = list(self.volume_history[symbol])
        if len(volumes) < 5:
            return {}

        current_volume = volumes[-1]
        avg_volume = sum(volumes[-5:]) / 5  # 5-period average

        return {
            "current": round(current_volume, 2),
            "average_5": round(avg_volume, 2),
            "volume_ratio": round(
                current_volume / avg_volume if avg_volume > 0 else 0, 2
            ),
        }

    async def _publish_analytics(
        self, analytics_results: Dict[str, Any], timestamp: float
    ):
        """Publish analytics results via message bus."""
        try:
            analytics_message = {
                "timestamp": timestamp,
                "analytics": analytics_results,
                "service": "data_analytics",
            }

            await self.message_bus.publish(
                Topics.DATA_PROCESSED,  # Use DATA_PROCESSED topic for analytics
                analytics_message,
                port=Ports.GLOBAL_EVENTS,
            )

            self.logger.debug(
                f"Published analytics for {len(analytics_results)} symbols"
            )

        except Exception as e:
            self.logger.error(f"Failed to publish analytics: {e}")

    def _log_analytics_results(self, analytics_results: Dict[str, Any]):
        """Log analytics results via LogService."""
        if not self.log_service:
            return

        self.analytics_counter += 1

        # Log header
        self.log_service.log_message(
            "Analytics",
            "INFO",
            f"--- Analytics Update #{self.analytics_counter} ---",
            "DATA",
        )

        # Log each symbol's analytics
        for symbol, result in analytics_results.items():
            current_price = result.get("current_price", 0)
            analytics = result.get("analytics", {})

            # Format analytics summary
            summary_parts = [f"Price: ${current_price:,.2f}"]

            # SMA values
            sma_data = analytics.get("sma", {})
            if sma_data:
                sma_str = ", ".join(
                    [f"SMA{k.split('_')[1]}: ${v:,.2f}" for k, v in sma_data.items()]
                )
                summary_parts.append(sma_str)

            # EMA value
            ema = analytics.get("ema")
            if ema is not None:
                summary_parts.append(f"EMA: ${ema:,.2f}")

            # Volatility and momentum
            volatility = analytics.get("volatility")
            momentum = analytics.get("momentum")
            if volatility is not None:
                summary_parts.append(f"Vol: {volatility:.2f}%")
            if momentum is not None:
                summary_parts.append(f"Mom: {momentum:+.2f}%")

            summary = " | ".join(summary_parts)

            self.log_service.log_message(
                "Analytics", "INFO", f"  {symbol}: {summary}", "DATA"
            )

        # Empty line for readability
        self.log_service.log_message("Analytics", "INFO", "", "DATA")

    async def cleanup(self):
        """Clean up analytics service resources."""
        await super().cleanup()
        await self.message_bus.cleanup()

        if self.log_service:
            self.log_service.log_message(
                "Analytics",
                "INFO",
                f"Analytics service stopped. Processed {self.analytics_counter} updates.",
                "SYSTEM",
            )

        self.logger.info("DataAnalytics service cleaned up")
