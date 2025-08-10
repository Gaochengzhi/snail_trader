"""
DataFetchService: Provides market data to strategies.

Live mode: Fetch real-time market data from external APIs
Backtest mode: Read historical data from local DuckDB files
"""

import asyncio
import json
from typing import Dict, Any, Optional
import duckdb

from core import AbstractService, MessageBus, Topics, Ports


class DataFetchService(AbstractService):
    """
    Service responsible for providing market data to the system.
    
    Behavior depends on operating mode:
    - Live mode: Fetches real-time data from external sources
    - Backtest mode: Streams historical data from local storage
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("data_fetch", config)
        self.message_bus = MessageBus("data_fetch")
        
        # Configuration
        self.mode = config.get('mode', 'live')
        self.data_sources = config.get('data_sources', [])
        self.historical_data_path = config.get('historical_data_path', 'data/historical.db')
        self.fetch_interval = config.get('data_fetch_interval_seconds', 30)
        
        # Database connection for backtest mode
        self.db_conn: Optional[duckdb.DuckDBPyConnection] = None
        
        # Backtest state
        self.backtest_cursor = None
        self.backtest_complete = False
    
    async def initialize(self):
        """Initialize data service and message bus."""
        await super().initialize()
        await self.message_bus.initialize()
        
        if self.mode == 'backtest':
            await self._initialize_backtest_database()
    
    async def _initialize_backtest_database(self):
        """Initialize DuckDB connection for historical data."""
        try:
            self.db_conn = duckdb.connect(self.historical_data_path)
            self.logger.info(f"Connected to historical database: {self.historical_data_path}")
            
            # TODO: Set up cursor for streaming historical data
            # This would typically query ordered by timestamp
            print(f"[PLACEHOLDER] Initialize historical data cursor from {self.historical_data_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to historical database: {e}")
            raise
    
    async def async_run(self):
        """Main data service loop."""
        await self.initialize()
        self._running = True
        
        try:
            if self.mode == 'live':
                await self._run_live_mode()
            else:
                await self._run_backtest_mode()
        
        except Exception as e:
            self.logger.error(f"DataFetch service error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def _run_live_mode(self):
        """
        Live mode: Continuously fetch and broadcast real-time market data.
        
        Fetches data from configured sources and publishes via MARKET_DATA topic.
        """
        self.logger.info("DataFetch running in live mode")
        
        while self._running:
            try:
                # Fetch data from all configured sources
                market_data = await self._fetch_live_data()
                
                if market_data:
                    await self._publish_market_data(market_data)
                
                # Wait before next fetch
                await asyncio.sleep(self.fetch_interval)
                
            except Exception as e:
                self.logger.error(f"Error fetching live data: {e}")
                await asyncio.sleep(self.fetch_interval)  # Continue on error
    
    async def _run_backtest_mode(self):
        """
        Backtest mode: Stream historical data in chronological order.
        
        Reads from historical database and publishes data events.
        After each data point, publishes DATA_PROCESSED to trigger next step.
        """
        self.logger.info("DataFetch running in backtest mode")
        
        if not self.db_conn:
            raise RuntimeError("Database connection not initialized for backtest mode")
        
        try:
            await self._stream_historical_data()
        except Exception as e:
            self.logger.error(f"Backtest data streaming error: {e}")
            raise
    
    async def _fetch_live_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch real-time market data from configured sources.
        
        Returns:
            Dictionary containing current market data or None on failure
        """
        try:
            # TODO: Implement actual data fetching from APIs
            # This would typically involve HTTP requests to:
            # - Cryptocurrency exchanges (Binance, Coinbase, etc.)
            # - Stock data providers (Alpha Vantage, Yahoo Finance, etc.)
            # - Forex data providers
            
            self.logger.debug("Fetching live market data")
            
            # Placeholder data structure
            market_data = {
                'timestamp': asyncio.get_event_loop().time(),
                'symbols': {},
                'source': 'placeholder'
            }
            
            # TODO: For each configured data source:
            for source in self.data_sources:
                print(f"[PLACEHOLDER] Fetching from {source}")
                # market_data['symbols'][symbol] = await fetch_symbol_data(source, symbol)
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch live data: {e}")
            return None
    
    async def _stream_historical_data(self):
        """
        Stream historical data from database in chronological order.
        
        This method reads historical data points sequentially and publishes
        them as MARKET_DATA events, followed by DATA_PROCESSED signals.
        """
        try:
            # TODO: Execute query to get historical data ordered by timestamp
            # query = "SELECT * FROM market_data ORDER BY timestamp"
            # results = self.db_conn.execute(query)
            
            self.logger.info("Starting historical data streaming")
            
            # Placeholder loop - in reality this would iterate over query results
            data_points_count = 100  # Placeholder
            for i in range(data_points_count):
                if not self._running:
                    break
                
                # TODO: Get next historical data point from query results
                historical_data = {
                    'timestamp': 1000000 + i * 60,  # Placeholder timestamps
                    'symbols': {
                        'BTC/USD': {'price': 50000 + i, 'volume': 1000},
                        'ETH/USD': {'price': 3000 + i * 0.5, 'volume': 500}
                    },
                    'source': 'historical'
                }
                
                # Publish the historical data point
                await self._publish_market_data(historical_data)
                
                # Signal that data has been processed
                await self._publish_data_processed(historical_data)
                
                # Small delay to prevent overwhelming the system
                await asyncio.sleep(0.01)
            
            self.backtest_complete = True
            self.logger.info("Historical data streaming completed")
            
        except Exception as e:
            self.logger.error(f"Error streaming historical data: {e}")
            raise
    
    async def _publish_market_data(self, data: Dict[str, Any]):
        """Publish market data to MARKET_DATA topic."""
        await self.message_bus.publish(
            Topics.MARKET_DATA, 
            data, 
            port=Ports.MARKET_DATA
        )
        
        self.logger.debug(f"Published market data: {len(data.get('symbols', {}))} symbols")
    
    async def _publish_data_processed(self, data: Dict[str, Any]):
        """
        Signal that a data point has been processed (backtest mode).
        
        This triggers SchedulerService to execute the next global step.
        """
        processed_event = {
            'timestamp': data['timestamp'],
            'symbols_count': len(data.get('symbols', {}))
        }
        
        await self.message_bus.publish(
            Topics.DATA_PROCESSED,
            processed_event,
            port=Ports.GLOBAL_EVENTS
        )
        
        self.logger.debug(f"Published DATA_PROCESSED for timestamp {data['timestamp']}")
    
    async def cleanup(self):
        """Clean up data service resources."""
        await super().cleanup()
        await self.message_bus.cleanup()
        
        if self.db_conn:
            self.db_conn.close()
            self.logger.info("Closed database connection")
        
        self.logger.info("DataFetch service cleaned up")