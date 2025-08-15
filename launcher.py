import asyncio
from datetime import datetime

from utils.config_utils import Config
from utils.welcome import run_welcome
from utils.time_manager import init_time_manager, TimeMode


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

        async def run_live_trading():
            try:
                # 初始化时间管理器为实时模式
                init_time_manager(mode=TimeMode.LIVE)

                # 启动EMA200策略
                from strategies.ema200_strategy import EMA200Strategy

                strategy = EMA200Strategy(self.settings)

                print("🚀 Starting EMA200 Strategy in Live Mode")
                await strategy.run()

            except KeyboardInterrupt:
                print("\n⏹️ Live trading stopped by user")
            except Exception as e:
                print(f"❌ Live trading error: {e}")
                import traceback

                traceback.print_exc()

        asyncio.run(run_live_trading())

    def _run_backtest_mode(self):
        """Initialize and run backtest mode."""
        print("📊 Backtest mode - Ready for strategy testing")

        async def run_backtest():
            try:
                # 初始化时间管理器为回测模式
                def parse_datetime(dt_str):
                    return (
                        datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                        if dt_str
                        else None
                    )

                time_control = self.settings.time_control
                run_from_datetime = parse_datetime(time_control.run_from_datetime)
                run_to_datetime = parse_datetime(
                    getattr(time_control, "run_to_datetime", None)
                )

                print(f"📅 回测起始时间: {run_from_datetime or '默认(当前时间)'}")
                print(f"📅 回测终止时间: {run_to_datetime or '默认(起始时间+30天)'}")

                init_time_manager(run_from_datetime, TimeMode.BACKTEST, run_to_datetime)

                # 启动WebUI服务（如果启用）
                webui_service = None
                if getattr(self.settings, "webui", {}).get("enabled", False):
                    from services.webui_service import WebUIService

                    webui_config = dict(self.settings.webui)
                    webui_service = WebUIService(webui_config)

                    # 先初始化WebUI服务
                    await webui_service.initialize()
                    port = webui_config.get("port", 8080)
                    print(f"🌐 WebUI服务已启动: http://localhost:{port}")

                # 启动EMA200策略
                from strategies.ema200_strategy import EMA200Strategy

                strategy = EMA200Strategy(self.settings)

                # 建立策略和WebUI的连接（WebUI已经ready）
                if webui_service:
                    strategy.webui_service = webui_service

                print("🚀 Starting EMA200 Strategy in Backtest Mode")

                # 并发运行策略和WebUI服务的主循环
                if webui_service:
                    await asyncio.gather(
                        strategy.run(),
                        webui_service.async_run(),  # 只运行主循环，不重复初始化
                        return_exceptions=True,
                    )
                else:
                    await strategy.run()

            except KeyboardInterrupt:
                print("\n⏹️ Backtest stopped by user")
            except Exception as e:
                print(f"❌ Backtest error: {e}")
                import traceback

                traceback.print_exc()

        asyncio.run(run_backtest())

    def _run_component_mode(self):
        if self.config_name == "data_serves_test":
            self._run_data_service_test()
        else:
            self._show_available_components()

    def _run_data_service_test(self):
        """Run unified data service test with strategy configuration."""
        from services.data_service import DataService

        # from strategies.volume_spike_strategy import VolumeSpikeStrategy
        from utils.strategy_config_loader import StrategyConfigLoader

        async def run_data_service_test():
            """Run unified data service test."""
            try:
                print("🚀 Starting Unified Data Service Test")

                # 1. 创建策略配置加载器
                config_loader = StrategyConfigLoader(self.config_name)

                # 2. 直接测试 volume_spike_strategy
                strategy_name = "volume_spike_strategy"
                print(f"Loading strategy: {strategy_name}")

                # 3. 加载策略配置
                strategy_config = config_loader.load_strategy_config(strategy_name)
                requirements = config_loader.get_strategy_data_requirements(
                    strategy_name
                )
                print(f"Strategy requirements: {requirements}")

                # 4. 为策略创建精确的数据服务配置
                data_service_config = config_loader.create_strategy_data_config(
                    strategy_name
                )
                print("Strategy-specific data service config created")

                # 5. 启动数据服务
                data_service = DataService(data_service_config)
                print("Data Service created with strategy-specific config")

                # 6. 启动测试策略
                test_strategy = VolumeSpikeStrategy(strategy_config)
                print("Volume Spike Strategy created")

                # 7. 并发运行服务
                print("Starting services...")
                await asyncio.gather(
                    data_service.run(),
                    test_strategy.run(),
                    return_exceptions=True,
                )

            except KeyboardInterrupt:
                print("\n⏹️  Data service test stopped by user")
            except Exception as e:
                print(f"❌ Data service test error: {e}")
                import traceback

                traceback.print_exc()

        try:
            asyncio.run(run_data_service_test())
        except KeyboardInterrupt:
            print("\nShutdown complete")

    def _show_available_components(self):
        """Show available component test configurations."""
        print("Available components:")
        print("  - data_serves_test: Mock data service testing with analytics")
        print("Usage: python main.py data_serves_test")
