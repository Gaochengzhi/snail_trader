import fire

from launcher import TradeLauncher


def main(config_name=None):
    """
    Main entry point for Snail Trader

    Args:
        config_name: Optional config environment name (e.g., 'ollama', 'prod')
    """
    launcher = TradeLauncher(config_name)
    launcher.run()


if __name__ == "__main__":
    fire.Fire(main)
