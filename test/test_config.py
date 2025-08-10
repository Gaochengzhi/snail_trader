#!/usr/bin/env python3
"""Test script for configuration system"""

import fire
from utils.config_utils import Config


def show_config(config_name: str = "base"):
    """Show configuration details"""
    config = Config(config_name)
    settings = config.settings  # 获取配置设置实例

    print(f"Configuration: {config_name}")
    print("=" * 50)

    # Show market config
    print("Market Configuration:")
    print(f"  Universe: {settings.market.universe}")
    print(f"  Window: {settings.market.window}")
    print(f"  Fee: {settings.market.fee}")
    print()

    # Show LLM config
    if settings.llm:
        print("LLM Configuration:")
        print(f"  Active fallback list: {settings.llm.get('active_fallback_list', [])}")
        print(f"  Enable streaming: {settings.llm.get('enable_streaming', False)}")
        print(f"  Max retries: {settings.llm.get('max_retries', 3)}")
        print()

        # Show providers
        providers = settings.llm.get("providers", {})
        print("LLM Providers:")
        for name, provider in providers.items():
            print(f"  {name}:")
            print(f"    Provider: {provider.get('provider')}")
            print(f"    Model: {provider.get('model')}")
            print(f"    Max concurrent: {provider.get('max_concurrent', 1)}")
            if provider.get("base_url"):
                print(f"    Base URL: {provider.get('base_url')}")
            print()
    else:
        print("No LLM configuration found")


def test_configs():
    """Test different configurations"""
    print("Testing base config:")
    show_config("base")

    print("\n" + "=" * 50 + "\n")

    print("Testing ollama config:")
    show_config("ollama")


def get_llm_instance(config: str = "base"):
    """Get LLM instance with specified config"""
    from llm_api.universal_llm import UniversalLLM

    # Create LLM instance with specific config
    llm = UniversalLLM(config_name=config)
    print(f"LLM status with {config} config:")
    status = llm.get_status()
    print(f"Active providers: {status['active_fallback_list']}")
    print(f"Current provider: {status['current_provider']}")

    return llm


if __name__ == "__main__":
    fire.Fire(
        {
            "test": test_configs,
            "show": show_config,
            "llm": get_llm_instance,
        }
    )
