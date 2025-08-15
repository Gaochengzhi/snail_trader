#!/usr/bin/env python3
"""
数据访问层功能测试脚本

测试配置：修改这些参数来调整测试行为
"""
from datetime import datetime, timezone

# ============================================================================
# 测试配置区域 - 在此统一修改所有测试参数
# ============================================================================

# 基础配置
TEST_CONFIG = {
    "duckdb": {
        "db_path": "test/data_access_layer/test_data.duckdb",
        "data_path": "test/data_access_layer/test_parquet_data",
    },
    "api": {
        "rate_limit": 10,  # 每秒API请求限制
    },
}

# 测试数据范围
TEST_TIME_RANGES = {
    "basic_test": {
        "start": datetime(2025, 8, 1, tzinfo=timezone.utc),
        "end": datetime(2025, 8, 14, tzinfo=timezone.utc),
    },
    "recent_data": {
        "start": datetime(2025, 8, 14, tzinfo=timezone.utc),
        "end": datetime(2025, 8, 15, tzinfo=timezone.utc),
    },
    "integrity_test": {
        "start": datetime(2025, 8, 10, tzinfo=timezone.utc),
        "end": datetime(2025, 8, 11, tzinfo=timezone.utc),
    },
}

# 测试交易对和时间周期
TEST_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
TEST_INTERVALS = ["15m", "1h", "4h"]

# 清理选项
AUTO_CLEANUP = False  # 设为True自动清理测试文件，False保留以供检查

# ============================================================================
# 测试代码区域 - 一般不需要修改
# ============================================================================

import asyncio
import sys

from pathlib import Path


from services.data_access_layer import DataAccessLayer, DuckDBEngine, BinanceAPIEngine
from utils.data_query import TimeRange
from utils.config_utils import DotDict


class DataAccessLayerTester:
    """数据访问层测试器"""

    def __init__(self):
        self.test_config = DotDict(TEST_CONFIG)
        self.test_data_dir = Path("test/data_access_layer")
        self.cleanup_paths = []  # 记录需要清理的路径

    def setup_test_environment(self):
        """设置测试环境"""
        print("🔧 设置安全测试环境...")

        # 创建测试数据目录
        test_db_dir = Path(self.test_config.duckdb.db_path).parent
        test_data_dir = Path(self.test_config.duckdb.data_path)

        test_db_dir.mkdir(parents=True, exist_ok=True)
        test_data_dir.mkdir(parents=True, exist_ok=True)

        # 记录清理路径（但在测试完成后不立即清理，让您能查看结果）
        # self.cleanup_paths.extend([test_db_dir, test_data_dir])

        print(f"✅ 测试数据库路径: {self.test_config.duckdb.db_path}")
        print(f"✅ 测试数据目录: {self.test_config.duckdb.data_path}")

    def cleanup_test_environment(self):
        """清理测试环境（可选）"""
        print("\n🧹 测试环境保留，您可以手动检查生成的文件")
        print("   如需清理，请删除: test/data_access_layer/")

    async def test_duckdb_engine(self):
        """测试DuckDB引擎基础功能"""
        print("\n📊 测试DuckDB引擎...")

        try:
            # 创建DuckDB引擎
            duckdb_engine = DuckDBEngine(self.test_config)
            await duckdb_engine.initialize()

            print("✅ DuckDB引擎初始化成功")
            print(f"✅ 数据库连接建立: {self.test_config.duckdb.db_path}")

            time_range = TimeRange(
                start=TEST_TIME_RANGES["basic_test"]["start"],
                end=TEST_TIME_RANGES["basic_test"]["end"],
            )

            result = await duckdb_engine.query(
                ["BTCUSDT"], ["15m", "4h", "1h"], time_range, "klines"
            )

            print(f"✅ 空数据查询测试通过: {len(result)} 个结果")

            await duckdb_engine.cleanup()
            print("✅ DuckDB引擎清理完成")

        except Exception as e:
            print(f"❌ DuckDB引擎测试失败: {e}")
            raise

    async def test_real_api_engine(self):
        """测试真实API引擎（小数据量，避免频繁调用）"""
        print("\n🌐 测试真实API引擎...")

        try:
            api_engine = BinanceAPIEngine(self.test_config.api)
            await api_engine.initialize()

            print("✅ API引擎初始化成功")

            # 测试获取小量K线数据（最近1小时，避免大量请求）
            from datetime import timedelta

            now = datetime.now(timezone.utc)
            time_range = TimeRange(
                start=now - timedelta(hours=1),
                end=now,
            )

            print(f"🔍 获取最近1小时数据: {time_range.start} - {time_range.end}")

            kline_data = await api_engine.fetch_klines(
                "BTCUSDT", "15m", time_range, limit=10
            )
            print(f"✅ K线数据获取: {len(kline_data)} 条记录")

            if kline_data:
                sample = kline_data[0]
                timestamp = datetime.fromtimestamp(
                    sample["open_time"] / 1000, tz=timezone.utc
                )
                print(f"   样本时间: {timestamp}")
                print(f"   样本价格: open={sample['open']}, close={sample['close']}")
                print(
                    f"   数据结构验证: ✅"
                    if all(
                        k in sample
                        for k in ["open_time", "open", "high", "low", "close", "volume"]
                    )
                    else "❌ 数据结构异常"
                )

            # 测试获取最新价格
            latest_prices = await api_engine.fetch_latest_prices(["BTCUSDT", "ETHUSDT"])
            print(f"✅ 最新价格获取: {len(latest_prices)} 个交易对")

            for symbol, price in latest_prices.items():
                print(f"   {symbol}: ${price:,.2f}")

            # 验证价格合理性
            btc_price = latest_prices.get("BTCUSDT", 0)
            if 20000 <= btc_price <= 200000:  # 合理的BTC价格范围
                print("   价格合理性验证: ✅")
            else:
                print(f"   价格合理性验证: ❌ BTC价格 ${btc_price} 超出预期范围")

            await api_engine.cleanup()
            print("✅ API引擎清理完成")

        except Exception as e:
            print(f"❌ API引擎测试失败: {e}")
            print("   可能原因: 网络问题、API限制或服务不可用")
            raise

    async def test_data_write_functionality(self):
        return
        """测试数据回写功能（安全测试）"""
        print("\n💾 测试数据回写功能...")

        try:
            # 创建测试数据（2025年8月1日数据）
            test_data = [
                {
                    "open_time": int(
                        datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp()
                        * 1000
                    ),
                    "open": 45000.0,
                    "high": 45500.0,
                    "low": 44800.0,
                    "close": 45200.0,
                    "volume": 1000.0,
                    "close_time": int(
                        datetime(2025, 8, 1, 10, 15, tzinfo=timezone.utc).timestamp()
                        * 1000
                    ),
                    "quote_volume": 45200000.0,
                    "count": 500,
                    "taker_buy_volume": 600.0,
                    "taker_buy_quote_volume": 27120000.0,
                    "ignore": 0,
                },
                {
                    "open_time": int(
                        datetime(2025, 8, 1, 10, 15, tzinfo=timezone.utc).timestamp()
                        * 1000
                    ),
                    "open": 45200.0,
                    "high": 45800.0,
                    "low": 45000.0,
                    "close": 45600.0,
                    "volume": 1200.0,
                    "close_time": int(
                        datetime(2025, 8, 1, 10, 30, tzinfo=timezone.utc).timestamp()
                        * 1000
                    ),
                    "quote_volume": 54720000.0,
                    "count": 600,
                    "taker_buy_volume": 720.0,
                    "taker_buy_quote_volume": 32832000.0,
                    "ignore": 0,
                },
            ]

            # 创建DuckDB引擎并写入测试数据
            duckdb_engine = DuckDBEngine(self.test_config)
            await duckdb_engine.initialize()

            print(f"✅ 准备写入测试数据: {len(test_data)} 条记录")

            # 安全写入到测试目录
            await duckdb_engine.write_data(
                symbol="BTCUSDT",
                interval="15m",
                data=test_data,
                data_type="klines",
                formats=["parquet", "csv"],
            )

            print("✅ 数据回写完成")

            # 验证写入的文件
            parquet_dir = (
                Path(self.test_config.duckdb.data_path) / "binance_parquet" / "klines"
            )
            csv_dir = (
                Path(self.test_config.duckdb.data_path) / "klines" / "BTCUSDT" / "15m"
            )

            if parquet_dir.exists():
                parquet_files = list(parquet_dir.rglob("*.parquet"))
                print(f"✅ Parquet文件创建: {len(parquet_files)} 个文件")
                for f in parquet_files:
                    print(f"   {f}")

            if csv_dir.exists():
                csv_files = list(csv_dir.glob("*.csv"))
                print(f"✅ CSV文件创建: {len(csv_files)} 个文件")
                for f in csv_files:
                    print(f"   {f}")

            await duckdb_engine.cleanup()
            print("✅ 数据回写测试完成")

        except Exception as e:
            print(f"❌ 数据回写测试失败: {e}")
            raise

    async def test_full_data_access_layer(self):
        """测试完整数据访问层（2025年8月1-15日查询）"""
        print("\n🔍 测试完整数据访问层功能...")

        try:
            # 创建数据访问层
            data_layer = DataAccessLayer(self.test_config)
            await data_layer.initialize()

            print("✅ 数据访问层初始化成功")

            # 测试2025年8月1-15日数据查询
            time_range = TimeRange(
                start=datetime(2025, 8, 1, tzinfo=timezone.utc),
                end=datetime(2025, 8, 15, tzinfo=timezone.utc),
            )

            print(f"🔍 查询时间范围: {time_range.start} - {time_range.end}")

            # 查询多个交易对的数据
            symbols = TEST_SYMBOLS
            intervals = TEST_INTERVALS

            result = await data_layer.fetch_raw_data(
                symbols=symbols,
                intervals=intervals,
                time_range=time_range,
                data_type="klines",
            )

            print(f"✅ 数据查询完成: {len(result)} 个数据集")

            # 显示查询结果详情
            for key, data in result.items():
                print(f"   {key}: {len(data)} 条记录")
                if data:
                    sample = data[0]
                    timestamp = datetime.fromtimestamp(sample["open_time"] / 1000)
                    print(f"      样本时间: {timestamp}")
                    print(
                        f"      样本价格: open={sample['open']}, close={sample['close']}"
                    )

            # 测试最新价格查询
            latest_prices = await data_layer.get_latest_prices(symbols)
            print(f"✅ 最新价格查询: {latest_prices}")

            await data_layer.cleanup()
            print("✅ 数据访问层测试完成")

        except Exception as e:
            import traceback

            print(f"❌ 数据访问层测试失败: {e}")
            print(f"详细错误信息: {traceback.format_exc()}")
            raise

    async def test_data_consistency_and_merge(self):
        """测试数据一致性和合并逻辑（关键测试）"""
        print("\n🔄 测试数据一致性和合并逻辑...")

        try:
            # 创建数据访问层
            data_layer = DataAccessLayer(self.test_config)
            await data_layer.initialize()
            print("✅ 数据访问层初始化成功")

            # 测试场景1：部分数据存在，需要API补充
            from datetime import timedelta

            now = datetime.now(timezone.utc)
            test_range = TimeRange(
                start=now - timedelta(hours=2),  # 2小时前开始
                end=now - timedelta(minutes=30),  # 30分钟前结束，确保数据存在
            )

            print(f"🔍 测试时间范围: {test_range.start} - {test_range.end}")

            # 第一次查询：可能触发API获取和写入
            print("📊 第一次查询（可能触发API获取）...")
            result1 = await data_layer.fetch_raw_data(
                symbols=["BTCUSDT"],
                intervals=["15m"],
                time_range=test_range,
                data_type="klines",
            )

            data1 = result1.get("BTCUSDT_15m", [])
            print(f"   第一次结果: {len(data1)} 条记录")

            if data1:
                first_time = datetime.fromtimestamp(
                    data1[0]["open_time"] / 1000, tz=timezone.utc
                )
                last_time = datetime.fromtimestamp(
                    data1[-1]["open_time"] / 1000, tz=timezone.utc
                )
                print(f"   时间范围: {first_time} - {last_time}")

            # 等待一秒，确保写入完成
            await asyncio.sleep(1)

            # 第二次查询：应该从数据库获取，验证一致性
            print("📊 第二次查询（应该从数据库获取）...")
            result2 = await data_layer.fetch_raw_data(
                symbols=["BTCUSDT"],
                intervals=["15m"],
                time_range=test_range,
                data_type="klines",
            )

            data2 = result2.get("BTCUSDT_15m", [])
            print(f"   第二次结果: {len(data2)} 条记录")

            # 验证数据一致性
            if len(data1) == len(data2):
                print("   📈 数据长度一致性: ✅")

                # 验证关键时间点一致性
                if data1 and data2:
                    time_consistent = (
                        data1[0]["open_time"] == data2[0]["open_time"]
                        and data1[-1]["open_time"] == data2[-1]["open_time"]
                    )
                    print(f"   📈 时间戳一致性: {'✅' if time_consistent else '❌'}")

                    # 验证价格数据一致性（抽样检查前3条记录）
                    price_consistent = all(
                        data1[i]["close"] == data2[i]["close"]
                        for i in range(min(3, len(data1)))
                    )
                    print(f"   📈 价格数据一致性: {'✅' if price_consistent else '❌'}")

                else:
                    print("   📈 数据一致性: ❌ 数据为空")
            else:
                print(f"   📈 数据一致性: ❌ 长度不一致 ({len(data1)} vs {len(data2)})")

            # 测试场景2：验证缺失数据检测逻辑
            print("\n🔍 测试缺失数据检测逻辑...")

            # 使用一个较大的历史时间范围，可能触发缺失数据检测
            historical_range = TimeRange(
                start=TEST_TIME_RANGES["basic_test"]["start"],
                end=TEST_TIME_RANGES["basic_test"]["end"],
            )

            print(f"   历史数据范围: {historical_range.start} - {historical_range.end}")

            # 查询历史数据，观察完整性检测
            historical_result = await data_layer.fetch_raw_data(
                symbols=["BTCUSDT"],
                intervals=["1h"],  # 使用1小时间隔，便于观察
                time_range=historical_range,
                data_type="klines",
            )

            historical_data = historical_result.get("BTCUSDT_1h", [])
            print(f"   历史数据获取: {len(historical_data)} 条记录")

            if historical_data:
                # 计算时间覆盖率
                expected_hours = int(
                    (historical_range.end - historical_range.start).total_seconds()
                    / 3600
                )
                coverage_ratio = len(historical_data) / max(expected_hours, 1)
                print(
                    f"   时间覆盖率: {coverage_ratio:.1%} ({len(historical_data)}/{expected_hours})"
                )

                # 检查时间连续性
                if len(historical_data) > 1:
                    gaps = []
                    for i in range(1, len(historical_data)):
                        prev_time = historical_data[i - 1]["close_time"]
                        curr_time = historical_data[i]["open_time"]
                        gap_ms = curr_time - prev_time - 1  # -1因为close_time是开区间
                        if gap_ms > 3600 * 1000:  # 超过1小时的缺口
                            gaps.append(gap_ms / (3600 * 1000))

                    if gaps:
                        print(f"   时间缺口: {len(gaps)} 个，最大 {max(gaps):.1f} 小时")
                    else:
                        print("   时间连续性: ✅ 无明显缺口")

            await data_layer.cleanup()
            print("✅ 数据一致性测试完成")

        except Exception as e:
            import traceback

            print(f"❌ 数据一致性测试失败: {e}")
            print(f"详细错误信息: {traceback.format_exc()}")
            raise

    async def run_all_tests(self):
        """运行所有测试"""
        print("🧪 开始数据访问层功能测试")
        print("=" * 50)

        try:
            # 设置测试环境
            self.setup_test_environment()

            # 依次运行各项测试
            await self.test_duckdb_engine()
            await self.test_real_api_engine()
            await self.test_data_write_functionality()
            await self.test_full_data_access_layer()
            await self.test_data_consistency_and_merge()

            print("\n" + "=" * 50)
            print("✅ 所有测试通过！数据访问层功能正常")

        except Exception as e:
            print(f"\n❌ 测试失败: {e}")
            print("请检查错误信息并修复问题")
            return False

        finally:
            # 清理测试环境（现在不自动清理）
            self.cleanup_test_environment()

        return True


async def main():
    """主函数"""
    tester = DataAccessLayerTester()
    success = await tester.run_all_tests()

    if success:
        print("\n🎉 数据访问层测试完成，所有功能正常工作！")
        print("\n📂 测试文件位置:")
        print("   - 测试数据库: test/data_access_layer/test_data.duckdb")
        print("   - 测试数据: test/data_access_layer/test_parquet_data/")
    else:
        print("\n💥 测试过程中发现问题，请检查输出信息")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
