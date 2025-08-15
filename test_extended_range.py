#!/usr/bin/env python3
"""
测试扩大查询范围的效果
"""
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from services.data_access_layer import DataAccessLayer
from utils.data_query import TimeRange
from utils.config_utils import DotDict


async def test_extended_range():
    """测试扩大查询范围"""
    
    # 使用与测试相同的配置
    test_config = DotDict({
        "duckdb": {
            "db_path": "test/data_access_layer/test_data.duckdb",
            "data_path": "test/data_access_layer/test_parquet_data"
        },
        "api": {
            "rate_limit": 5,
            "mock_mode": True
        }
    })
    
    print("🔍 测试扩大查询范围的效果")
    print("=" * 50)
    
    # 测试不同的时间范围
    test_ranges = [
        {
            "name": "原测试范围 (2025-08-01 到 08-15)",
            "start": datetime(2025, 8, 1, tzinfo=timezone.utc),
            "end": datetime(2025, 8, 15, tzinfo=timezone.utc)
        },
        {
            "name": "扩大到7月 (2025-07-15 到 08-15)", 
            "start": datetime(2025, 7, 15, tzinfo=timezone.utc),
            "end": datetime(2025, 8, 15, tzinfo=timezone.utc)
        },
        {
            "name": "扩大到9月 (2025-08-01 到 09-15)",
            "start": datetime(2025, 8, 1, tzinfo=timezone.utc),
            "end": datetime(2025, 9, 15, tzinfo=timezone.utc)
        },
        {
            "name": "历史数据测试 (2024-12-01 到 12-31)",
            "start": datetime(2024, 12, 1, tzinfo=timezone.utc),
            "end": datetime(2024, 12, 31, tzinfo=timezone.utc)
        }
    ]
    
    try:
        # 创建数据访问层
        data_layer = DataAccessLayer(test_config)
        await data_layer.initialize()
        print("✅ 数据访问层初始化成功")
        
        for test_range in test_ranges:
            print(f"\n📊 {test_range['name']}")
            print("-" * 40)
            
            time_range = TimeRange(
                start=test_range["start"],
                end=test_range["end"]
            )
            
            print(f"🔍 查询时间范围: {time_range.start} - {time_range.end}")
            
            # 查询单个交易对单个间隔，避免过多输出
            result = await data_layer.fetch_raw_data(
                symbols=["BTCUSDT"],
                intervals=["15m"],
                time_range=time_range,
                data_type="klines"
            )
            
            for key, data in result.items():
                print(f"   {key}: {len(data)} 条记录")
                if data:
                    # 显示时间范围
                    first_time = datetime.fromtimestamp(data[0]['open_time'] / 1000)
                    last_time = datetime.fromtimestamp(data[-1]['close_time'] / 1000)
                    print(f"      实际数据时间范围: {first_time} - {last_time}")
                    print(f"      样本价格: open={data[0]['open']}, close={data[-1]['close']}")
                else:
                    print(f"      无数据")
        
        await data_layer.cleanup()
        print("\n✅ 扩大范围测试完成")
        
    except Exception as e:
        import traceback
        print(f"\n❌ 测试失败: {e}")
        print(f"详细错误: {traceback.format_exc()}")


if __name__ == "__main__":
    asyncio.run(test_extended_range())