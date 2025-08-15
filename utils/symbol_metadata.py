"""
币种元数据管理工具
"""

import json
import asyncio
import aiohttp
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Dict, Optional, Any


class SymbolMetadataManager:
    """币种元数据管理器"""

    def __init__(self, metadata_file: str = "metadata/symbols.json"):
        self.metadata_file = Path(metadata_file)
        self.metadata = {}
        self._load_metadata()

    def _load_metadata(self):
        """加载元数据文件"""
        if self.metadata_file.exists():
            with open(self.metadata_file, "r", encoding="utf-8") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {"last_updated": "2025-01-01", "symbols": {}}

    def _save_metadata(self):
        """保存元数据文件"""
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)

    def should_update_today(self) -> bool:
        """检查今天是否需要更新元数据"""
        last_updated = self.metadata.get("last_updated", "2025-01-01")
        today = date.today().strftime("%Y-%m-%d")
        return last_updated != today

    def get_listing_date(self, symbol: str) -> Optional[datetime]:
        """获取币种上市日期"""
        symbol_info = self.metadata.get("symbols", {}).get(symbol)
        if symbol_info and "listing_date" in symbol_info:
            return datetime.strptime(symbol_info["listing_date"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        return None

    def is_symbol_known(self, symbol: str) -> bool:
        """检查币种是否已知"""
        return symbol in self.metadata.get("symbols", {})

    def add_symbol(self, symbol: str, listing_date: str, status: str = "active"):
        """添加新币种"""
        if "symbols" not in self.metadata:
            self.metadata["symbols"] = {}

        self.metadata["symbols"][symbol] = {
            "listing_date": listing_date,
            "status": status,
        }
        self._save_metadata()

    async def update_symbols_from_api(self) -> bool:
        """从Binance API更新币种列表"""
        if not self.should_update_today():
            print(
                f"Symbol metadata already updated today: {self.metadata.get('last_updated')}"
            )
            return False

        print("Fetching symbol metadata from Binance API...")
        try:
            async with aiohttp.ClientSession() as session:
                # 获取所有交易对信息
                url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        print(f"API request failed with status: {response.status}")
                        return False

                    data = await response.json()
                    symbols = data.get("symbols", [])

                    if not symbols:
                        print("No symbols returned from API")
                        return False

                    print(f"Found {len(symbols)} total symbols from API")

                    # 清理旧数据，重新构建
                    if "symbols" not in self.metadata:
                        self.metadata["symbols"] = {}

                    # 更新元数据
                    updated_count = 0
                    usdt_count = 0

                    for symbol_info in symbols:
                        symbol = symbol_info["symbol"]
                        status = symbol_info.get("status", "UNKNOWN")

                        # 只处理USDT交易对
                        if symbol.endswith("USDT"):
                            usdt_count += 1

                            # 获取上市日期 - Binance期货API可能没有onboardDate
                            listing_date = symbol_info.get("onboardDate")
                            if listing_date:
                                try:
                                    listing_date = datetime.fromtimestamp(
                                        listing_date / 1000
                                    ).strftime("%Y-%m-%d")
                                except (ValueError, TypeError):
                                    listing_date = (
                                        "2019-09-25"  # Binance期货默认开始日期
                                    )
                            else:
                                # 根据币种推测大致上市时间
                                if symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
                                    listing_date = "2019-09-25"  # 早期主流币
                                elif symbol in ["ADAUSDT", "DOTUSDT", "LINKUSDT"]:
                                    listing_date = "2020-01-01"  # 第二批
                                else:
                                    listing_date = "2021-01-01"  # 新币种保守估计

                            # 更新或添加符号
                            if symbol not in self.metadata["symbols"]:
                                updated_count += 1

                            self.metadata["symbols"][symbol] = {
                                "listing_date": listing_date,
                                "status": (
                                    "active" if status == "TRADING" else "inactive"
                                ),
                            }

                    # 更新最后更新日期
                    self.metadata["last_updated"] = date.today().strftime("%Y-%m-%d")
                    self._save_metadata()

                    print(
                        f"Updated symbol metadata: {usdt_count} USDT pairs, {updated_count} new symbols"
                    )
                    return True

        except Exception as e:
            print(f"Failed to update symbols from API: {e}")
            import traceback

            traceback.print_exc()
            return False

    def classify_missing_data(self, symbol: str, time_range) -> str:
        """分类数据缺失原因"""
        if not self.is_symbol_known(symbol):
            return "unknown_symbol"

        listing_date = self.get_listing_date(symbol)
        if not listing_date:
            return "unknown_symbol"

        if time_range.start < listing_date:
            return "new_coin"  # 查询时间早于上市时间
        else:
            return "data_corruption"  # 应该有数据但没有


# 全局实例
symbol_metadata = SymbolMetadataManager()


async def ensure_symbols_updated():
    """确保币种元数据是最新的（每日最多更新一次）"""
    return await symbol_metadata.update_symbols_from_api()


async def main():
    """直接运行脚本，强制更新币种元数据"""
    print("🔄 正在强制更新币种元数据...")

    # 删除现有文件以强制更新
    metadata_file = Path("metadata/symbols.json")
    if metadata_file.exists():
        metadata_file.unlink()
        print(f"✅ 已删除旧的metadata文件: {metadata_file}")

    # 创建新的管理器实例
    manager = SymbolMetadataManager()

    # 强制更新
    success = await manager.update_symbols_from_api()

    if success:
        print("✅ 币种元数据更新成功!")
        symbol_count = len(manager.metadata.get("symbols", {}))
        print(f"📊 共获取到 {symbol_count} 个USDT交易对")
        print(f"📁 文件保存位置: {manager.metadata_file.absolute()}")
    else:
        print("❌ 币种元数据更新失败!")

    return success


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
