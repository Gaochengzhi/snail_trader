import asyncio

from utils.config_utils import Config
from utils.welcome import run_welcome


class TradeLauncher:
    """Trade launcher for managing different execution modes and component initialization."""

    def __init__(self, config_name=None):
        self.config = Config(config_name)
        self.settings = self.config.settings
        self.config_name = config_name

    def run(self):
        """Main execution method."""
        mode = run_welcome(self.config_name)

        if mode == "live":
            self._run_live_mode()
        elif mode == "backtest":
            self._run_backtest_mode()
        elif mode == "test":
            self._run_component_mode()

    def _run_live_mode(self):
        """Initialize and run live trading mode."""
        print("🔴 Live Trading mode - Ready for real-time execution")
        # Add live trading initialization here

    def _run_backtest_mode(self):
        """Initialize and run backtest mode."""
        print("📊 Backtest mode - Ready for strategy testing")
        # Add backtesting initialization here

    def _run_component_mode(self):
        """Initialize and run component testing mode."""
        print("🔧 Component mode - Ready for development and debugging")

        if self.config_name == "data_serves_test":
            self._run_data_service_test()
        else:
            self._show_available_components()

    def _run_data_service_test(self):
        """Run data service component test."""
        from services.data_fetch_service import DataFetchService
        from services.data_analytics_service import DataAnalyticsService
        from utils.log_utils import LogUtils

        async def run_component_test():
            """Run component test with all services."""
            try:
                # Ensure settings are fully initialized first
                _ = self.settings  # This triggers the property getter and full initialization
                
                # Pass the settings object directly (it supports dot notation)
                # No need to copy since services should treat config as read-only
                service_config = self.settings
                
                data_service = DataFetchService(service_config)

                # Run all services concurrently
                await asyncio.gather(
                    data_service.run(),
                    return_exceptions=True,
                )

            except KeyboardInterrupt:
                print("\nComponent test stopped by user")
            except Exception as e:
                print(f"Component test error: {e}")
                import traceback

                traceback.print_exc()

        try:
            asyncio.run(run_component_test())
        except KeyboardInterrupt:
            print("\nShutdown complete")

    def _show_available_components(self):
        """Show available component test configurations."""
        print("Available components:")
        print("  - data_serves_test: Mock data service testing with analytics")
        print("Usage: python main.py data_serves_test")
