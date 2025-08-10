import os
import sys
import yaml
import re
from pathlib import Path
from typing import Any, Dict, Optional
from typing import Any, Dict, Optional, List
from pydantic import Field, BaseModel
from typing import List

try:
    from dotenv import load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

try:
    import fire

    FIRE_AVAILABLE = True
except ImportError:
    FIRE_AVAILABLE = False


class DotDict(dict):
    """支持点号访问的字典类"""

    def __init__(self, *args, **kwargs):
        super(DotDict, self).__init__(*args, **kwargs)
        # 递归地将所有嵌套的字典转换为DotDict
        for key, value in self.items():
            if isinstance(value, dict):
                self[key] = DotDict(value)
            elif isinstance(value, list):
                self[key] = [
                    DotDict(item) if isinstance(item, dict) else item for item in value
                ]

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'DotDict' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        self[key] = value


class Config:
    """简化的配置管理类 - 支持变量引用和环境覆盖"""

    def __init__(self, config_name: Optional[str] = None):
        self._cfg_dir = Path(__file__).resolve().parent.parent / "configs"
        self._settings: Optional[StrategySettings] = None
        self._config_name = config_name or self._get_config_from_args()

        # Load .env file if available
        if DOTENV_AVAILABLE:
            env_file = Path(__file__).resolve().parent.parent / ".env"
            if env_file.exists():
                load_dotenv(env_file)

    def _get_config_from_args(self) -> Optional[str]:
        """从命令行参数获取配置名称"""
        # 简单的参数解析，避免与主程序的argparse冲突
        if "--config" in sys.argv:
            try:
                config_index = sys.argv.index("--config")
                if config_index + 1 < len(sys.argv):
                    return sys.argv[config_index + 1]
            except (IndexError, ValueError):
                pass

        # 回退到环境变量
        return os.getenv("CONFIG_ENV")

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        """安全加载yaml文件"""
        if not path.exists():
            return {}
        return yaml.safe_load(path.read_text()) or {}

    def _merge_configs(self, base: dict, overlay: dict) -> dict:
        """合并配置字典"""
        result = base.copy()
        result.update(overlay)
        return result

    def _resolve_variables(self, config: dict) -> dict:
        """解析配置中的变量引用 (如 ${var_name})，支持环境变量"""

        def resolve_value(value):
            if isinstance(value, dict):
                # Recursively resolve nested dictionaries
                return self._resolve_variables(value)
            elif isinstance(value, list):
                # Recursively resolve list items
                return [resolve_value(item) for item in value]
            elif (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                var_name = value[2:-1]
                # First try config internal variables, then environment variables
                resolved = config.get(var_name)
                if resolved is None:
                    resolved = os.getenv(var_name)
                return resolved if resolved is not None else value
            return value

        return {k: resolve_value(v) for k, v in config.items()}

    @property
    def settings(self) -> "StrategySettings":
        """获取配置实例"""
        if self._settings is None:
            base_config = self._load_yaml(self._cfg_dir / "base.yaml")
            if self._config_name:
                env_config = self._load_yaml(
                    self._cfg_dir / f"{self._config_name}.yaml"
                )
                merged = self._merge_configs(base_config, env_config)
            else:
                merged = base_config

            resolved = self._resolve_variables(merged)

            # 将所有配置数据转换为支持点号访问的格式
            # 递归地将嵌套字典转换为DotDict
            processed_data = {}
            for key, value in resolved.items():
                if isinstance(value, dict):
                    processed_data[key] = DotDict(value)
                else:
                    processed_data[key] = value
            
            # 由于设置了extra="allow"，Pydantic会自动处理所有字段
            self._settings = StrategySettings(**processed_data)

        return self._settings


class MarketConfig(BaseModel):
    """市场配置"""

    universe: str = Field("Binance", description="市场")
    window: int = Field(365, description="窗口天数")
    fee: float = Field(0.0003, description="手续费")


class StrategySettings(BaseModel):
    """策略配置模型"""

    market: MarketConfig = Field(default_factory=MarketConfig, description="市场配置")
    llm: Optional[Dict[str, Any]] = Field(None, description="LLM配置")

    class Config:
        env_prefix = "QLIB_"
        extra = "allow"  # Allow extra fields

    def __getattr__(self, name):
        """允许通过点号访问所有额外字段"""
        # 首先检查对象字典中是否有该属性
        if name in self.__dict__:
            return self.__dict__[name]

        # 在Pydantic v2中，检查额外字段
        if hasattr(self, '__pydantic_extra__') and name in self.__pydantic_extra__:
            value = self.__pydantic_extra__[name]
            # 如果是字典，转换为DotDict以支持嵌套点号访问
            if isinstance(value, dict):
                return DotDict(value)
            return value

        # 作为备用方案，检查是否直接设置在对象上
        if hasattr(self, '_' + name):
            return getattr(self, '_' + name)

        raise AttributeError(f"'StrategySettings' object has no attribute '{name}'")


# 全局配置实例


if __name__ == "__main__":
    config = Config()
    settings = config.settings
    print("配置对象:", settings)
    print("配置字典:", settings.model_dump())
    print("单个变量:", settings.fee)
    print("JSON格式:", settings.model_dump_json(indent=2))

    # 测试变量引用功能
    print("\n测试变量引用功能:")
    test_config = Config()
    test_data = {"base_fee": 0.001, "trading_fee": "${base_fee}", "universe": "Binance"}
    resolved = test_config._resolve_variables(test_data)
    print("原始:", test_data)
    print("解析后:", resolved)
