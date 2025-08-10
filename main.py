import fire

from utils.config_utils import Config
from utils.welcome import run_welcome


def main(config_name=None):
    """
    Main entry point for Snail Trader

    Args:
        config_name: Optional config environment name (e.g., 'ollama', 'prod')
    """
    mode = run_welcome()
    config = Config(config_name)
    settings = config.settings

    if mode == "live_trading":
        print("🔴 Live Trading mode - Ready for real-time execution")
        # Add live trading initialization here
    elif mode == "backtest":
        print("📊 Backtest mode - Ready for strategy testing")
        # Add backtesting initialization here
    elif mode == "component":
        print("🔧 Component mode - Ready for development and debugging")
        # Add component testing initialization here


if __name__ == "__main__":
    fire.Fire(main)
