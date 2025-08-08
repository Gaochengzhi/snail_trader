import os
import yaml
import re
from pathlib import Path
from typing import Any, Dict, Optional
from pydantic import Field, BaseModel
from typing import List


class Config:
    """简化的配置管理类 - 支持变量引用和环境覆盖"""

    def __init__(self):
        self._cfg_dir = Path(__file__).resolve().parent.parent / "configs"
        self._settings: Optional[StrategySettings] = None

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
        """解析配置中的变量引用 (如 ${var_name})"""

        def resolve_value(value):
            if (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                var_name = value[2:-1]
                return config.get(var_name, value)
            return value

        return {k: resolve_value(v) for k, v in config.items()}

    @property
    def settings(self) -> "StrategySettings":
        """获取配置实例 - 单例模式"""
        if self._settings is None:
            base_config = self._load_yaml(self._cfg_dir / "base.yaml")
            env = os.getenv("CONFIG_ENV", "")
            if env:
                env_config = self._load_yaml(self._cfg_dir / f"{env}.yaml")
                merged = self._merge_configs(base_config, env_config)
            else:
                merged = base_config

            resolved = self._resolve_variables(merged)

            self._settings = StrategySettings(**resolved)

        return self._settings


class StrategySettings(BaseModel):
    """策略配置模型"""

    universe: str = Field("Binance", description="回测池")
    window: int = Field(365, ge=1, description="回看窗口天数")
    fee: float = Field(0.0003, description="手续费")
    llm: Optional[Dict[str, Any]] = Field(None, description="LLM配置")

    class Config:
        env_prefix = "QLIB_"
        extra = "allow"  # Allow extra fields


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
