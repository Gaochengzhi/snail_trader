"""
数据访问层 - 统一历史数据和实时数据的访问接口
"""

import asyncio
import aiohttp
import duckdb
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path

from utils.data_query import TimeRange
from utils.symbol_metadata import symbol_metadata, ensure_symbols_updated
from utils.data_integrity_checker import integrity_checker
from utils.rate_limiter import get_rate_limiter_manager
from utils.time_manager import get_time_manager
from core.base import AbstractService


class DuckDBEngine(AbstractService):
    def __init__(self, config):
        self.db_path = config.duckdb.db_path
        self.data_path = config.duckdb.data_path
        self.connection = None
        super().__init__("data_analytics", config)

    async def initialize(self):
        await super().initialize()
        self.connection = duckdb.connect(self.db_path)

    async def async_run(self):
        pass

    async def query(
        self,
        symbols: List[str],
        intervals: List[str],
        time_range: TimeRange,
        data_type: str = "klines",
    ) -> Dict[str, Any]:
        """查询历史数据"""
        if not self.connection:
            raise RuntimeError("DuckDB connection not initialized")

        valid_types = [
            "klines",
            "indexPriceKlines",
            "markPriceKlines",
            "premiumIndexKlines",
        ]
        if data_type not in valid_types:
            raise ValueError(
                f"Unsupported data_type: {data_type}. Valid types: {valid_types}"
            )

        results = {}
        start_ts, end_ts = time_range.to_timestamp()

        for symbol in symbols:
            for interval in intervals:
                try:
                    # 修复路径：使用binance_parquet子目录
                    parquet_path = f"{self.data_path}/binance_parquet/{data_type}/interval={interval}/date=*/symbol={symbol}.parquet"

                    query_sql = f"""
                    SELECT * FROM read_parquet('{parquet_path}')
                    WHERE open_time/1000 >= {start_ts} AND close_time/1000 <= {end_ts}
                    ORDER BY open_time
                    """

                    data = self.connection.execute(query_sql).fetchall()
                    columns = [desc[0] for desc in self.connection.description]

                    results[f"{symbol}_{interval}"] = [
                        dict(zip(columns, row)) for row in data
                    ]
                    
                    self.log("DEBUG", f"Found {len(data)} records for {symbol}_{interval} from parquet")

                except Exception as e:
                    # 改进异常处理：区分文件不存在 vs 其他错误
                    if "No files found" in str(e) or "does not exist" in str(e):
                        self.log("DEBUG", f"No parquet files found for {symbol}_{interval}: {e}")
                    else:
                        self.log("ERROR", f"DuckDB query failed for {symbol}_{interval}: {e}")
                        await self._handle_exception(e)
                    results[f"{symbol}_{interval}"] = []

        return results

    async def write_data(
        self,
        symbol: str,
        interval: str,
        data: List[Dict[str, Any]],
        data_type: str = "klines",
        formats: List[str] = ["parquet"],
    ):
        """将API数据写入指定格式"""
        if not data:
            return

        print(f"[WRITE_DATA] {symbol} {interval}: 准备写入 {len(data)} 条记录")

        # 按日期分组数据
        date_groups = {}
        for row in data:
            date_key = datetime.fromtimestamp(row["open_time"] / 1000).strftime(
                "%Y-%m-%d"
            )
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(row)

        print(f"[WRITE_DATA] {symbol} {interval}: 按日期分组结果:")
        for date_key, date_data in date_groups.items():
            print(f"  - {date_key}: {len(date_data)} 条记录")
            if date_data:
                first_time = datetime.fromtimestamp(date_data[0]["open_time"] / 1000)
                last_time = datetime.fromtimestamp(date_data[-1]["close_time"] / 1000)
                print(f"    时间范围: {first_time} - {last_time}")

        # 为每个日期写入指定格式文件
        for date_key, date_data in date_groups.items():
            print(f"[WRITE_DATA] {symbol} {interval}: 写入 {date_key} 的 {len(date_data)} 条记录")
            await self._write_parquet_data(
                symbol, interval, date_key, date_data, data_type
            )

            if "csv" in formats:
                await self._write_csv_data(
                    symbol, interval, date_key, date_data, data_type
                )

    async def _write_parquet_data(
        self,
        symbol: str,
        interval: str,
        date_key: str,
        date_data: List[Dict],
        data_type: str,
    ):
        """写入Parquet格式数据"""
        output_path = f"{self.data_path}/binance_parquet/{data_type}/interval={interval}/date={date_key}/"
        Path(output_path).mkdir(parents=True, exist_ok=True)

        file_path = f"{output_path}symbol={symbol}.parquet"
        temp_path = f"{file_path}.tmp"

        import pandas as pd

        df = pd.DataFrame(date_data)
        df.to_parquet(temp_path, compression="zstd", index=False)
        Path(temp_path).rename(file_path)

    async def _write_csv_data(
        self,
        symbol: str,
        interval: str,
        date_key: str,
        date_data: List[Dict],
        data_type: str,
    ):
        """写入CSV格式数据（原始数据结构）"""
        output_path = f"{self.data_path}/{data_type}/{symbol}/{interval}/"
        Path(output_path).mkdir(parents=True, exist_ok=True)

        file_path = f"{output_path}{symbol}-{interval}-{date_key}.csv"
        temp_path = f"{file_path}.tmp"

        import pandas as pd

        df = pd.DataFrame(date_data)
        df.to_csv(temp_path, index=False)
        Path(temp_path).rename(file_path)

    async def cleanup(self):
        """清理资源"""
        if self.connection:
            self.connection.close()


class BinanceAPIEngine:
    """Binance API实时数据引擎"""

    def __init__(self, config: Dict[str, Any]):
        self.base_url = "https://fapi.binance.com"
        self.session = None
        self.rate_limiter = get_rate_limiter_manager()
        self._request_semaphore = asyncio.Semaphore(10)  # 最大并发请求数

    async def initialize(self):
        """初始化HTTP会话"""
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def fetch_klines(
        self, symbol: str, interval: str, time_range: TimeRange, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """获取K线数据"""
        if not self.session:
            raise RuntimeError("HTTP session not initialized")

        endpoint = "klines"
        if not await self.rate_limiter.acquire(endpoint, timeout=30.0):
            raise Exception(f"Rate limiter timeout for {endpoint}")

        async with self._request_semaphore:
            # 确保时间都是UTC时区
            start_utc = time_range.start
            end_utc = time_range.end
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
            if end_utc.tzinfo is None:
                end_utc = end_utc.replace(tzinfo=timezone.utc)
                
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": int(start_utc.timestamp() * 1000),
                "endTime": int(end_utc.timestamp() * 1000),
                "limit": limit,
            }
            
            print(f"[API_CALL] {symbol} {interval}: startTime={start_utc} ({params['startTime']}), endTime={end_utc} ({params['endTime']}), limit={limit}")

            url = f"{self.base_url}/fapi/v1/klines"
            start_time = asyncio.get_event_loop().time()

            try:
                async with self.session.get(url, params=params) as response:
                    response_time = asyncio.get_event_loop().time() - start_time
                    success = response.status == 200

                    await self.rate_limiter.report_response(
                        endpoint, response_time, success, response.status
                    )

                    if response.status == 200:
                        data = await response.json()
                        formatted_data = self._format_kline_data(data)
                        print(f"[API_RESPONSE] {symbol} {interval}: received {len(formatted_data)} records")
                        if formatted_data:
                            first_time = datetime.fromtimestamp(formatted_data[0]['open_time'] / 1000)
                            last_time = datetime.fromtimestamp(formatted_data[-1]['close_time'] / 1000)
                            print(f"[API_RESPONSE] {symbol} {interval}: data range {first_time} - {last_time}")
                        return formatted_data
                    else:
                        raise Exception(f"API request failed: {response.status}")

            except Exception as e:
                response_time = asyncio.get_event_loop().time() - start_time
                await self.rate_limiter.report_response(
                    endpoint, response_time, False, None, str(e)
                )
                raise Exception(f"Failed to fetch klines for {symbol}: {str(e)}")

    def _format_kline_data(self, raw_data: List[List]) -> List[Dict[str, Any]]:
        """格式化K线数据"""
        columns = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]

        formatted_data = []
        for row in raw_data:
            formatted_row = {}
            for i, col in enumerate(columns):
                if col in ["open_time", "close_time"]:
                    formatted_row[col] = int(row[i])
                elif col in ["count"]:
                    formatted_row[col] = int(row[i])
                else:
                    formatted_row[col] = float(row[i])

            formatted_data.append(formatted_row)

        return formatted_data

    async def fetch_latest_prices(self, symbols: List[str] = None) -> Dict[str, float]:
        """获取最新价格"""
        if not self.session:
            raise RuntimeError("HTTP session not initialized")

        endpoint = "ticker_price"
        if not await self.rate_limiter.acquire(endpoint, timeout=30.0):
            raise Exception(f"Rate limiter timeout for {endpoint}")

        async with self._request_semaphore:
            url = f"{self.base_url}/fapi/v1/ticker/price"
            start_time = asyncio.get_event_loop().time()

            try:
                async with self.session.get(url) as response:
                    response_time = asyncio.get_event_loop().time() - start_time
                    success = response.status == 200

                    await self.rate_limiter.report_response(
                        endpoint, response_time, success, response.status
                    )

                    if response.status == 200:
                        data = await response.json()

                        prices = {}
                        for item in data:
                            symbol = item["symbol"]
                            if symbols is None or symbol in symbols:
                                prices[symbol] = float(item["price"])

                        return prices
                    else:
                        raise Exception(f"API request failed: {response.status}")

            except Exception as e:
                response_time = asyncio.get_event_loop().time() - start_time
                await self.rate_limiter.report_response(
                    endpoint, response_time, False, None, str(e)
                )
                raise Exception(f"Failed to fetch latest prices: {str(e)}")

    async def cleanup(self):
        """清理资源"""
        if self.session:
            await self.session.close()


class DataAccessLayer:
    """统一数据访问层"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.duckdb_engine = DuckDBEngine(config)
        self.api_engine = BinanceAPIEngine(config["api"])

    async def initialize(self):
        """初始化所有引擎"""
        await self.duckdb_engine.initialize()
        await self.api_engine.initialize()

        # 确保币种元数据是最新的（每日最多更新一次）
        await ensure_symbols_updated()

    async def fetch_raw_data(
        self,
        symbols: List[str],
        intervals: List[str],
        time_range: TimeRange,
        data_type: str = "klines",
    ) -> Dict[str, Any]:
        """统一数据获取接口 - 直接查询数据库，无缓存"""
        results = {}

        for symbol in symbols:
            for interval in intervals:
                # 直接查询数据库，无缓存机制
                await self._fetch_with_integrity_check(
                    symbol, interval, time_range, results, data_type
                )

        return results

    async def _fetch_with_integrity_check(
        self,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        results: Dict[str, Any],
        data_type: str,
    ):
        """统一的数据获取和完整性检查 - 无缓存版本"""
        # 使用详细的完整性检查
        missing_ranges = await self._find_missing_data(
            symbol, interval, time_range, data_type
        )

        if not missing_ranges:
            # 数据库中有完整数据，直接查询
            db_data = await self.duckdb_engine.query(
                [symbol], [interval], time_range, data_type
            )
            results.update(db_data)
        else:
            # 需要从API补充数据
            await self._fetch_and_merge_data(
                symbol, interval, time_range, missing_ranges, results, data_type
            )

    async def _find_missing_data(
        self,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        data_type: str = "klines",
    ) -> List[TimeRange]:
        """检查哪些数据在数据库中缺失 - 使用专门的完整性检查器"""

        # 获取数据库中现有的数据
        existing_data = await self.duckdb_engine.query(
            [symbol], [interval], time_range, data_type
        )

        actual_data = existing_data.get(f"{symbol}_{interval}", [])
        
        # 获取当前时间（回测时为虚拟时间）
        time_manager = get_time_manager()
        current_time = time_manager.now()
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        
        # 使用专门的完整性检查器进行分析
        integrity_report = integrity_checker.analyze_data_completeness(
            symbol, interval, time_range, actual_data, current_time
        )

        print(
            f"[INTEGRITY] {symbol} {interval}: "
            f"completeness={integrity_report.completeness_ratio:.2%}, "
            f"gaps={len(integrity_report.gaps)}, "
            f"needs_repair={integrity_report.needs_repair}, "
            f"current_time={current_time}"
        )

        # 根据完整性报告决定是否需要修复
        if not integrity_report.needs_repair and integrity_report.is_healthy:
            return []

        if integrity_report.needs_repair:
            if integrity_report.completeness_ratio < 0.5:
                print(
                    f"[WARNING] Data severely corrupted for {symbol} {interval}, "
                    f"completeness only {integrity_report.completeness_ratio:.1%}. "
                    f"Will re-download entire range."
                )
                return [time_range]
            else:
                print(
                    f"[INFO] Partial data corruption detected for {symbol} {interval}, "
                    f"will repair {len(integrity_report.repair_ranges)} ranges"
                )
                return integrity_report.repair_ranges

        if len(integrity_report.gaps) > 0:
            print(
                f"[INFO] Found {len(integrity_report.gaps)} data gaps for {symbol} {interval}, "
                f"will fill {len(integrity_report.repair_ranges)} ranges"
            )
            return integrity_report.repair_ranges

        # 处理完全没有数据的情况
        if integrity_report.total_records == 0:
            missing_type = symbol_metadata.classify_missing_data(symbol, time_range)
            if missing_type == "new_coin":
                listing_date = symbol_metadata.get_listing_date(symbol)
                if listing_date and time_range.start < listing_date:
                    adjusted_start = max(listing_date, time_range.start)
                    if adjusted_start < time_range.end:
                        return [TimeRange(adjusted_start, time_range.end)]
                    else:
                        return []
            return [time_range]

        return []

    def _calculate_expected_records_for_range(self, time_range: TimeRange, interval: str) -> int:
        """计算指定时间范围的期望记录数"""
        total_seconds = (time_range.end - time_range.start).total_seconds()
        
        if interval == "15m":
            return max(1, round(total_seconds / (15 * 60)))
        elif interval == "1h":
            return max(1, round(total_seconds / 3600))
        elif interval == "4h":
            return max(1, round(total_seconds / (4 * 3600)))
        elif interval == "1d":
            return max(1, (time_range.end - time_range.start).days)
        else:
            return max(1, round(total_seconds / (15 * 60)))

    async def _fetch_and_merge_data(
        self,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        missing_ranges: List[TimeRange],
        results: Dict[str, Any],
        data_type: str = "klines",
    ):
        """从API获取缺失数据并合并"""
        # 1. 获取数据库中已有的数据
        db_data = await self.duckdb_engine.query(
            [symbol], [interval], time_range, data_type
        )

        # 2. 并发从API获取缺失的数据
        api_data = []
        if missing_ranges:
            print(f"[API_FETCH] {symbol} {interval}: 并发获取 {len(missing_ranges)} 个时间范围的数据")
            
            # 准备并发任务
            fetch_tasks = []
            for missing_range in missing_ranges:
                # 计算该时间范围需要的记录数，设置合适的limit
                expected_records = self._calculate_expected_records_for_range(missing_range, interval)
                # 添加一些冗余，确保获取到足够的数据
                api_limit = min(max(expected_records * 2, 100), 1000)
                
                print(f"[API_FETCH] {symbol} {interval}: range={missing_range.start} to {missing_range.end}, expected_records={expected_records}, limit={api_limit}")
                
                # 创建异步任务
                task = self.api_engine.fetch_klines(symbol, interval, missing_range, api_limit)
                fetch_tasks.append((task, missing_range))
            
            # 并发执行所有API调用
            try:
                results = await asyncio.gather(*[task for task, _ in fetch_tasks], return_exceptions=True)
                
                for i, (result, (task, missing_range)) in enumerate(zip(results, fetch_tasks)):
                    if isinstance(result, Exception):
                        print(f"[API_FETCH] {symbol} {interval}: 范围 {missing_range.start}-{missing_range.end} 获取失败: {result}")
                    else:
                        api_data.extend(result)
                        print(f"[API_FETCH] {symbol} {interval}: 范围 {missing_range.start}-{missing_range.end} 获取成功，{len(result)} 条记录")
                        
            except Exception as e:
                print(f"[API_FETCH] {symbol} {interval}: 并发获取失败: {e}")
                
        print(f"[API_FETCH] {symbol} {interval}: 总共获取 {len(api_data)} 条API数据")

        # 3. 智能写入策略
        if api_data:
            # 判断写入策略
            missing_type = symbol_metadata.classify_missing_data(symbol, time_range)
            if missing_type == "data_corruption":
                # 数据损坏：写入CSV+Parquet修复原始数据
                formats = ["csv", "parquet"]
                print(f"Repairing corrupted data for {symbol} {interval}")
            else:
                # 新币或正常缓存：只写Parquet
                formats = ["parquet"]

            await self.duckdb_engine.write_data(
                symbol, interval, api_data, data_type, formats
            )

        # 4. 合并数据
        key = f"{symbol}_{interval}"
        existing_data = db_data.get(key, [])

        print(f"[MERGE_DATA] {symbol} {interval}: existing_data type={type(existing_data)}, len={len(existing_data)}")
        print(f"[MERGE_DATA] {symbol} {interval}: api_data type={type(api_data)}, len={len(api_data)}")

        # 检查数据类型
        for i, item in enumerate(existing_data):
            if not isinstance(item, dict):
                print(f"[ERROR] existing_data[{i}] is not dict: type={type(item)}, value={item}")
        for i, item in enumerate(api_data):
            if not isinstance(item, dict):
                print(f"[ERROR] api_data[{i}] is not dict: type={type(item)}, value={item}")

        # 简单合并：将API数据追加到数据库数据后面
        all_data = existing_data + api_data
        print(f"[MERGE_DATA] {symbol} {interval}: all_data len={len(all_data)}")

        # 按时间排序并去重
        try:
            all_data.sort(key=lambda x: x["open_time"])
        except Exception as e:
            print(f"[ERROR] Failed to sort all_data: {e}")
            print(f"[ERROR] all_data sample: {all_data[:3] if len(all_data) >= 3 else all_data}")
            raise
            
        unique_data = []
        seen_times = set()
        for row in all_data:
            if row["open_time"] not in seen_times:
                unique_data.append(row)
                seen_times.add(row["open_time"])

        results[key] = unique_data
        print(f"[MERGE_DATA] {symbol} {interval}: final unique_data len={len(unique_data)}")

    async def get_latest_prices(self, symbols: List[str] = None) -> Dict[str, float]:
        """获取最新价格（直接从API）"""
        return await self.api_engine.fetch_latest_prices(symbols)

    async def fetch_klines(
        self, symbol: str, interval: str, time_range: TimeRange, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """获取K线数据 - 委托给API引擎"""
        return await self.api_engine.fetch_klines(symbol, interval, time_range, limit)

    async def cleanup(self):
        """清理所有资源"""
        await self.duckdb_engine.cleanup()
        await self.api_engine.cleanup()
