"""
策略配置加载器 - 专注于策略相关的配置管理逻辑
"""

from typing import Dict, Any, List
from pathlib import Path
from .config_utils import Config, DotDict


class StrategyConfigLoader:
    """策略配置加载器 - 使用Config基础功能"""

    def __init__(self, config_name: str = None):
        self.config = Config(config_name)
        self.strategies_dir = Path(__file__).resolve().parent.parent / "strategies"

    def load_strategy_config(self, strategy_name: str) -> DotDict:
        """加载策略配置并与基础配置合并"""
        strategy_config_path = f"strategies/{strategy_name}.yaml"
        return self.config.load_additional_config(strategy_config_path)

    def get_strategy_data_requirements(self, strategy_name: str) -> Dict[str, Any]:
        """获取策略的数据需求

        从策略配置文件中提取策略运行所需的数据参数，这些参数将用于配置数据服务
        确保数据服务能够提供策略所需的准确数据类型、交易对、指标和时间间隔
        """
        # 加载策略的完整配置（基础配置 + 策略特定配置）
        config = self.load_strategy_config(strategy_name)

        # 获取策略专属配置段落
        strategy_section = config.get(strategy_name, {})

        return {
            # 交易对列表 - 策略要监控的币种对，如 ["BTCUSDT", "ETHUSDT"]
            "symbols": strategy_section.get("symbols", []),
            # 技术指标列表 - 策略需要的指标，如 ["RSI", "MACD", "BOLL"]
            "indicators": strategy_section.get("indicators", []),
            # 数据类型列表 - 需要的数据格式，默认K线数据
            "data_types": strategy_section.get("data_types", ["klines"]),
            # 时间周期列表 - K线周期，如 ["1m", "5m", "15m"]
            "active_intervals": strategy_section.get("active_intervals", ["15m"]),
            # 🔄 策略执行间隔 (默认15分钟) - 统一执行周期
            "execution_interval_seconds": strategy_section.get(
                "execution_interval_seconds", 900
            ),
        }

    def create_strategy_data_config(self, strategy_name: str) -> DotDict:
        """为单个策略创建精确的数据服务配置

        这是核心方法：将策略的数据需求转换为数据服务的具体配置
        避免了旧版本中使用多策略最小值导致的资源浪费问题
        每个策略获得恰好满足其需求的数据服务配置
        """
        # 提取策略的具体数据需求
        requirements = self.get_strategy_data_requirements(strategy_name)

        # 获取系统基础配置作为起点
        base_config = self.config.settings.model_dump()

        # 创建策略专用配置副本，避免修改原始配置
        strategy_config = base_config.copy()

        # 确保存在数据服务配置节点
        if "data_service" not in strategy_config:
            strategy_config["data_service"] = {}

        # 用策略的精确需求覆盖数据服务配置
        strategy_config["data_service"].update(
            {
                # 监控的交易对 - 精确匹配策略需要的币种
                "monitored_symbols": requirements["symbols"],
                # 需要计算的技术指标 - 只计算策略用到的指标
                "required_indicators": requirements["indicators"],
                # 支持的数据类型 - 避免获取不需要的数据格式
                "supported_data_types": requirements["data_types"],
                # 活跃的时间周期 - 只获取策略分析用的K线周期
                "active_intervals": requirements["active_intervals"],
                # 策略执行频率 - 统一的数据处理周期，简单高效
                "execution_interval_seconds": requirements[
                    "execution_interval_seconds"
                ],
            }
        )

        # 转换为DotDict以支持点号访问
        return DotDict(strategy_config)

    def create_unified_data_config(self, strategy_names: List[str]) -> Dict[str, Any]:
        """为多个策略创建统一的数据服务配置（每个策略独立配置）

        用于多策略并行运行的场景，每个策略都有独立的数据配置
        这种设计避免了策略间的相互干扰，每个策略获得最适合自己的数据服务
        """
        # 存储所有策略的独立配置
        strategies_configs = {}

        # 为每个策略生成独立的数据服务配置
        for strategy_name in strategy_names:
            # 每个策略获得一个完全独立的配置，包含其特定的数据需求
            strategies_configs[strategy_name] = self.create_strategy_data_config(
                strategy_name
            )

        # 返回统一管理的多策略配置结构
        return {
            # 策略配置字典 - 键是策略名称，值是该策略的完整配置
            "strategies": strategies_configs,
            # 运行模式标识 - 标明这是多策略统一模式
            "mode": "unified_multi_strategy",
        }
