# csv2duckdb.py - Binance Data ETL Tool
#
# Copyright (c) 2025 Gaochengzhi
# Licensed under MIT License with Commercial Use Restriction
#
# This software is free for personal and research use.
# Commercial use by organizations requires explicit permission.
# See LICENSE file for details.
#
import duckdb, pathlib, os, re
from multiprocessing import Pool
import pyarrow.csv as pv
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from tqdm import tqdm
import logging
import yaml


# ============================================================================
# 📋 配置文件加载
# ============================================================================
def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


config = load_config()

# 从config.yaml读取配置  
output_dir = pathlib.Path(config["output_directory"]).expanduser().absolute()  # 处理~和相对路径，但保留软链接
DATA_PATH = str(output_dir / "binance_parquet")  # Parquet输出目录
SRC_DIR = output_dir  # CSV源数据目录
DUCK_DB = str(output_dir / "binance.duckdb")  # DuckDB数据库文件
LOG_DIR = config["log_directory"]  # 日志文件目录
CPU = os.cpu_count()  # 默认使用最大CPU核心数

# ============================================================================
# 🧪 测试配置区域 - 性能测试参数
# ============================================================================
# 短时间范围测试 (用于全盘扫描，数据密集型)
TEST_SHORT_START = datetime(2025, 8, 1, 0, 0, tzinfo=timezone.utc)
TEST_SHORT_END = datetime(2025, 8, 2, 4, 0, tzinfo=timezone.utc)  # 2小时窗口

# 长时间范围测试 (用于单符号时序分析，时间跨度大)
TEST_LONG_START = datetime(2024, 7, 29, 0, 0, tzinfo=timezone.utc)  # 3天数据
TEST_LONG_END = datetime(2025, 8, 1, 23, 59, tzinfo=timezone.utc)

# Convert to millisecond timestamps
TEST_SHORT_START_MS = int(TEST_SHORT_START.timestamp() * 1000)
TEST_SHORT_END_MS = int(TEST_SHORT_END.timestamp() * 1000)
TEST_LONG_START_MS = int(TEST_LONG_START.timestamp() * 1000)
TEST_LONG_END_MS = int(TEST_LONG_END.timestamp() * 1000)

# 保持原有兼容性
START_TIME = TEST_SHORT_START
END_TIME = TEST_SHORT_END
START_TIME_MS = TEST_SHORT_START_MS
END_TIME_MS = TEST_SHORT_END_MS
# ============================================================================
# 📁 目录初始化和日志配置
# ============================================================================
DST_DIR = pathlib.Path(DATA_PATH)
LOG_PATH = pathlib.Path(LOG_DIR)

# 确保必要目录存在
for directory in [DST_DIR, LOG_PATH]:
    directory.mkdir(parents=True, exist_ok=True)

# 配置日志系统
LOG_FILE = LOG_PATH / "conversion.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)

# 启动时记录配置信息
logging.info(f"=== ETL工具启动 ===")
logging.info(f"源目录: {SRC_DIR}")
logging.info(f"目标目录: {DST_DIR}")
logging.info(f"日志目录: {LOG_PATH}")
logging.info(f"数据库: {DUCK_DB}")

CSV_REGEX = re.compile(
    r"(?P<symbol>[A-Z0-9]+)-(?P<kind_or_interval>[a-zA-Z0-9]+)-(?P<date>\d{4}-\d{2}-\d{2})\.csv$"
)


# ---------- 1. 单文件 CSV → Parquet ----------
def convert_one(csv_path: pathlib.Path):
    temp_path = None
    try:
        m = CSV_REGEX.search(csv_path.name)
        if not m:
            print(f"Skip {csv_path}")
            return {
                "status": "skipped",
                "path": str(csv_path),
                "reason": "filename pattern mismatch",
            }

        symbol = m.group("symbol")
        kind_or_interval = m.group("kind_or_interval")
        date_str = m.group("date")
        date_fmt = datetime.strptime(date_str, "%Y-%m-%d").date()

        # 根据文件路径和第二个字段确定数据类型
        if any(
            kline_type in str(csv_path)
            for kline_type in [
                "klines",
                "indexPriceKlines",
                "markPriceKlines",
                "premiumIndexKlines",
            ]
        ):
            # K线数据: kind_or_interval 是时间间隔
            interval = kind_or_interval
            # 从路径中推断数据类型
            for kline_type in [
                "klines",
                "indexPriceKlines",
                "markPriceKlines",
                "premiumIndexKlines",
            ]:
                if kline_type in str(csv_path):
                    kind = kline_type
                    break
            else:
                kind = "klines"  # 默认
        else:
            # 其他数据: kind_or_interval 是数据类型
            kind = kind_or_interval
            interval = ""

        # 目标子目录
        dst = DST_DIR / kind
        if interval:
            dst = dst / f"interval={interval}"
        dst = dst / f"date={date_fmt}"
        dst.mkdir(parents=True, exist_ok=True)
        parquet_path = dst / f"symbol={symbol}.parquet"

        # 如果已存在，检查文件完整性
        if parquet_path.exists():
            try:
                # 尝试读取验证文件完整性
                pq.read_metadata(parquet_path)
                return {
                    "status": "skipped",
                    "path": str(csv_path),
                    "reason": "already exists and valid",
                }
            except Exception:
                # 文件损坏，删除重新创建
                print(f"⚠ Corrupted file detected, removing: {parquet_path}")
                parquet_path.unlink()

        # 使用临时文件确保原子性写入
        temp_path = parquet_path.with_suffix(".tmp")

        # 读取 CSV → Arrow Table
        table = pv.read_csv(
            csv_path, read_options=pv.ReadOptions(autogenerate_column_names=False)
        )

        # 检查空文件
        if len(table) == 0:
            return {"status": "skipped", "path": str(csv_path), "reason": "empty file"}

        # 添加分区列
        table = table.append_column("symbol", pa.array([symbol] * len(table)))
        if interval:
            table = table.append_column("interval", pa.array([interval] * len(table)))
        table = table.append_column("date", pa.array([str(date_fmt)] * len(table)))

        # 写 Parquet，保持按时间排序
        sorter = (
            table.column_names.index("open_time")
            if "open_time" in table.column_names
            else None
        )
        if sorter is not None:
            indices = pa.compute.sort_indices(
                table, sort_keys=[("open_time", "ascending")]
            )
            table = table.take(indices)

        pq.write_table(table, temp_path, compression="zstd")

        # 原子性重命名
        temp_path.rename(parquet_path)
        return {"status": "success", "path": str(csv_path), "output": str(parquet_path)}

    except Exception as e:
        # 清理临时文件
        if temp_path and temp_path.exists():
            temp_path.unlink()
        print(f"✗ Error processing {csv_path}: {str(e)}")
        return {"status": "error", "path": str(csv_path), "error": str(e)}


# ---------- 2. 多进程遍历 ----------
def batch_convert():
    # 确保目标目录存在
    DST_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = list(SRC_DIR.rglob("*.csv"))
    total_files = len(csv_files)

    logging.info(f"开始处理 {total_files} 个CSV文件")
    logging.info(f"源目录: {SRC_DIR}")
    logging.info(f"目标目录: {DST_DIR}")
    logging.info(f"使用CPU核心数: {CPU}")

    # 使用 tqdm 显示进度条
    with Pool(processes=CPU) as p:
        results = list(
            tqdm(
                p.imap(convert_one, csv_files),
                total=total_files,
                desc="Converting CSV→Parquet",
                unit="files",
                dynamic_ncols=True,
            )
        )

    # 统计结果
    stats = {"success": 0, "skipped": 0, "error": 0}
    errors = []
    successful_files = []

    for result in results:
        if result:
            stats[result["status"]] += 1
            if result["status"] == "error":
                errors.append(result)
                logging.error(f"Failed: {result['path']} - {result['error']}")
            elif result["status"] == "success":
                successful_files.append(result)
                # logging.info(f"Success: {result['path']} → {result['output']}")

    # 输出详细的结果报告
    print(f"\n{'='*50}")
    print(f"🎯 CONVERSION SUMMARY")
    print(f"{'='*50}")
    print(f"📁 Total files processed: {total_files}")
    print(f"✅ Successful conversions: {stats['success']}")
    print(f"⏭️  Skipped (existing/empty): {stats['skipped']}")
    print(f"❌ Failed conversions: {stats['error']}")
    print(f"📊 Success rate: {stats['success']/total_files*100:.1f}%")

    if errors:
        print(f"\n{'='*50}")
        print(f"❌ ERROR DETAILS")
        print(f"{'='*50}")

        # 按错误类型分组
        error_types = {}
        for err in errors:
            error_msg = err["error"]
            error_type = error_msg.split(":")[0] if ":" in error_msg else "Unknown"
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append(err)

        for error_type, errs in error_types.items():
            print(f"\n🔍 {error_type} ({len(errs)} files):")
            for err in errs[:5]:  # 只显示前5个
                print(f"  • {pathlib.Path(err['path']).name}")
            if len(errs) > 5:
                print(f"  ... and {len(errs) - 5} more files")

        # 将完整错误列表写入文件
        error_log = LOG_PATH / "conversion_errors.log"
        with open(error_log, "w", encoding="utf-8") as f:
            f.write(f"Conversion Errors - {datetime.now()}\n")
            f.write("=" * 80 + "\n\n")
            for err in errors:
                f.write(f"FILE: {err['path']}\n")
                f.write(f"ERROR: {err['error']}\n")
                f.write("-" * 80 + "\n")

        print(f"\n📝 Complete error log: {error_log}")
        logging.info(f"Saved detailed error log to {error_log}")

    # 生成成功文件清单
    if successful_files:
        success_log = LOG_PATH / "conversion_success.log"
        with open(success_log, "w", encoding="utf-8") as f:
            f.write(f"Successful Conversions - {datetime.now()}\n")
            f.write("=" * 80 + "\n\n")
            for succ in successful_files:
                f.write(f"CSV: {succ['path']}\n")
                f.write(f"PARQUET: {succ['output']}\n")
                f.write("-" * 40 + "\n")

    logging.info(f"转换完成: {stats['success']}/{total_files} 成功")
    return stats["error"] == 0


# ---------- 2.5. 清理损坏的Parquet文件 ----------
def cleanup_corrupted():
    """扫描并删除损坏的parquet文件"""
    parquet_files = list(DST_DIR.rglob("*.parquet"))
    corrupted = []

    logging.info(f"开始检查 {len(parquet_files)} 个parquet文件的完整性")

    # 使用进度条检查文件完整性
    for pf in tqdm(parquet_files, desc="Checking parquet integrity", unit="files"):
        try:
            pq.read_metadata(pf)
        except Exception:
            logging.warning(f"发现损坏文件: {pf}")
            corrupted.append(pf)

    if corrupted:
        print(f"\n⚠️  发现 {len(corrupted)} 个损坏的文件:")
        for cf in corrupted[:10]:  # 显示前10个
            print(f"  • {cf.relative_to(DST_DIR)}")
        if len(corrupted) > 10:
            print(f"  ... 还有 {len(corrupted) - 10} 个损坏文件")

        response = input(f"\n删除这 {len(corrupted)} 个损坏文件? [y/N]: ")
        if response.lower() == "y":
            for cf in tqdm(corrupted, desc="Deleting corrupted files", unit="files"):
                cf.unlink()
                logging.info(f"已删除损坏文件: {cf}")
            print(f"✅ 已删除 {len(corrupted)} 个损坏文件")
        else:
            print("❌ 取消清理操作")
    else:
        print("✅ 所有parquet文件都完整有效")
        logging.info("所有parquet文件完整性检查通过")


# ---------- 3. 初始化 DuckDB ----------
def init_duck_views():
    con = duckdb.connect(DUCK_DB)
    con.execute("INSTALL httpfs; LOAD httpfs;")  # 如果你要 S3，可用
    con.execute("SET enable_progress_bar=true;")

    # 性能优化设置
    con.execute("SET memory_limit='8GB';")  # 根据你的内存调整
    con.execute("SET threads=8;")  # 根据你的CPU核心数调整
    con.execute("SET enable_object_cache=true;")
    con.execute("SET preserve_insertion_order=false;")

    views = {
        "book_depth": DATA_PATH + "/bookDepth/*/*.parquet",
        "klines": DATA_PATH + "/klines/*/*/*.parquet",
        "index_price_klines": DATA_PATH + "/indexPriceKlines/*/*/*.parquet",
        "mark_price_klines": DATA_PATH + "/markPriceKlines/*/*/*.parquet",
        "premium_index_klines": DATA_PATH + "/premiumIndexKlines/*/*/*.parquet",
        "metrics": DATA_PATH + "/metrics/*/*.parquet",
    }

    for v, path in views.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {v} AS SELECT * FROM parquet_scan('{path}');"
        )
    con.close()


# ---------- 4. 中间件查询 API ----------
class BinanceDuck:
    def __init__(self, db_path=DUCK_DB):
        self.con = duckdb.connect(db_path, read_only=True)
        # 优化连接设置
        self.con.execute("SET memory_limit='8GB';")
        self.con.execute("SET threads=8;")
        self.con.execute("SET enable_object_cache=true;")
        self.con.execute("SET preserve_insertion_order=false;")

    def scan_15m(self, start_ts, end_ts, table="klines"):
        # 计算日期范围用于分区裁剪
        from datetime import datetime

        start_date = datetime.fromtimestamp(start_ts / 1000).date()
        end_date = datetime.fromtimestamp(end_ts / 1000).date()

        # 直接查询parquet文件，利用分区裁剪
        sql = f"""
        SELECT *
        FROM parquet_scan('{DATA_PATH}/{table}/interval=15m/date>={start_date}/*.parquet')
        WHERE open_time BETWEEN {start_ts} AND {end_ts}
        """
        return self.con.sql(sql).df()

    def scan_15m_optimized(self, start_ts, end_ts, symbols=None, table="klines"):
        """优化版本：支持符号过滤和更精确的分区裁剪"""
        from datetime import datetime, timedelta

        start_date = datetime.fromtimestamp(start_ts / 1000).date()
        end_date = datetime.fromtimestamp(end_ts / 1000).date()

        # 生成日期范围的parquet文件路径
        current_date = start_date
        file_patterns = []
        while current_date <= end_date:
            file_patterns.append(
                f"'{DATA_PATH}/{table}/interval=15m/date={current_date}/*.parquet'"
            )
            current_date += timedelta(days=1)

        if not file_patterns:
            return self.con.sql("SELECT * FROM (VALUES (1,1)) t(a,b) WHERE FALSE").df()

        files_str = ", ".join(file_patterns)

        sql = f"""
        SELECT *
        FROM parquet_scan([{files_str}])
        WHERE open_time BETWEEN {start_ts} AND {end_ts}
        """

        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols_str = "', '".join(symbols)
            sql += f" AND symbol IN ('{symbols_str}')"

        return self.con.sql(sql).df()

    def trace_symbol(self, symbol, since_ts, table="klines"):
        # 优化：先按日期过滤再按符号过滤
        from datetime import datetime

        since_date = datetime.fromtimestamp(since_ts / 1000).date()

        sql = f"""
        SELECT *
        FROM parquet_scan('{DATA_PATH}/{table}/*/*/date>={since_date}/*.parquet')
        WHERE symbol='{symbol}'
          AND open_time >= {since_ts}
        ORDER BY open_time
        """
        return self.con.sql(sql).df()

    def scan_aggregated(self, start_ts, end_ts, symbols=None, table="klines"):
        """聚合查询：计算OHLCV统计信息"""
        from datetime import datetime, timedelta

        start_date = datetime.fromtimestamp(start_ts / 1000).date()
        end_date = datetime.fromtimestamp(end_ts / 1000).date()

        current_date = start_date
        file_patterns = []
        while current_date <= end_date:
            file_patterns.append(
                f"'{DATA_PATH}/{table}/interval=15m/date={current_date}/*.parquet'"
            )
            current_date += timedelta(days=1)

        if not file_patterns:
            return self.con.sql("SELECT 1 WHERE FALSE").df()

        files_str = ", ".join(file_patterns)

        sql = f"""
        SELECT symbol,
               COUNT(*) as count,
               MIN(open_time) as first_time,
               MAX(open_time) as last_time,
               AVG(CAST(volume AS DOUBLE)) as avg_volume,
               MAX(CAST(high AS DOUBLE)) as max_price,
               MIN(CAST(low AS DOUBLE)) as min_price
        FROM parquet_scan([{files_str}])
        WHERE open_time BETWEEN {start_ts} AND {end_ts}
        """

        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols_str = "', '".join(symbols)
            sql += f" AND symbol IN ('{symbols_str}')"

        sql += " GROUP BY symbol ORDER BY symbol"
        return self.con.sql(sql).df()

    def scan_single_symbol(self, symbol, start_ts, end_ts, table="klines"):
        """单个符号查询"""
        from datetime import datetime, timedelta

        start_date = datetime.fromtimestamp(start_ts / 1000).date()
        end_date = datetime.fromtimestamp(end_ts / 1000).date()

        current_date = start_date
        file_patterns = []
        while current_date <= end_date:
            file_patterns.append(
                f"'{DATA_PATH}/{table}/interval=15m/date={current_date}/symbol={symbol}.parquet'"
            )
            current_date += timedelta(days=1)

        if not file_patterns:
            return self.con.sql("SELECT 1 WHERE FALSE").df()

        files_str = ", ".join(file_patterns)

        sql = f"""
        SELECT *
        FROM parquet_scan([{files_str}])
        WHERE open_time BETWEEN {start_ts} AND {end_ts}
        ORDER BY open_time
        """
        return self.con.sql(sql).df()

    def scan_top_volume(self, start_ts, end_ts, limit=10, table="klines"):
        """查询交易量最大的符号"""
        from datetime import datetime, timedelta

        start_date = datetime.fromtimestamp(start_ts / 1000).date()
        end_date = datetime.fromtimestamp(end_ts / 1000).date()

        current_date = start_date
        file_patterns = []
        while current_date <= end_date:
            file_patterns.append(
                f"'{DATA_PATH}/{table}/interval=15m/date={current_date}/*.parquet'"
            )
            current_date += timedelta(days=1)

        if not file_patterns:
            return self.con.sql("SELECT 1 WHERE FALSE").df()

        files_str = ", ".join(file_patterns)

        sql = f"""
        SELECT symbol,
               SUM(CAST(volume AS DOUBLE)) as total_volume,
               COUNT(*) as record_count
        FROM parquet_scan([{files_str}])
        WHERE open_time BETWEEN {start_ts} AND {end_ts}
        GROUP BY symbol
        ORDER BY total_volume DESC
        LIMIT {limit}
        """
        return self.con.sql(sql).df()

    def close(self):
        self.con.close()


# ---------- 5. 交互式菜单 ----------
def show_menu():
    """显示主菜单"""
    print(f"\n{'='*60}")
    print(f"🚀 Binance数据ETL工具")
    print(f"{'='*60}")
    print(f"📊 源目录: {SRC_DIR}")
    print(f"🎯 目标目录: {DST_DIR}")
    print(f"📝 日志目录: {LOG_PATH}")
    print(f"🗄️  DuckDB数据库: {DUCK_DB}")
    print(f"⚡ CPU核心数: {CPU}")
    print(f"{'='*60}")
    print("请选择要执行的功能:")
    print()
    print("1. 📂 ETL转换    - 将CSV文件转换为Parquet格式")
    print("2. 🗃️  初始化DB   - 创建DuckDB视图和表结构")
    print("3. 🧹 清理损坏   - 扫描并删除损坏的Parquet文件")
    print("4. 🔍 测试查询   - 运行测试查询验证数据")
    print("5. ℹ️  显示状态   - 显示当前数据状态和统计")
    print("6. ❌ 退出")
    print(f"{'='*60}")


def get_data_status():
    """获取当前数据状态"""
    csv_files = list(SRC_DIR.rglob("*.csv"))
    parquet_files = list(DST_DIR.rglob("*.parquet")) if DST_DIR.exists() else []

    print(f"\n📈 当前数据状态:")
    print(f"{'='*40}")
    print(f"📄 CSV文件数量: {len(csv_files):,}")
    print(f"📦 Parquet文件数量: {len(parquet_files):,}")

    if csv_files:
        print(f"📅 CSV日期范围: {get_date_range(csv_files)}")

    if parquet_files:
        print(f"💾 Parquet总大小: {get_total_size(parquet_files)}")
        print(
            f"📊 转换进度: {len(parquet_files)}/{len(csv_files)} ({len(parquet_files)/len(csv_files)*100:.1f}%)"
        )

    print(f"🗄️  DuckDB存在: {'✅' if pathlib.Path(DUCK_DB).exists() else '❌'}")


def get_date_range(files):
    """获取文件日期范围"""
    dates = []
    for f in files:
        m = CSV_REGEX.search(f.name)
        if m:
            try:
                date_obj = datetime.strptime(m.group("date"), "%Y-%m-%d").date()
                dates.append(date_obj)
            except:
                continue

    if dates:
        return f"{min(dates)} 至 {max(dates)}"
    return "无法确定"


def get_total_size(files):
    """获取文件总大小"""
    total_bytes = sum(f.stat().st_size for f in files if f.exists())

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total_bytes < 1024:
            return f"{total_bytes:.1f}{unit}"
        total_bytes /= 1024
    return f"{total_bytes:.1f}PB"


def run_performance_tests():
    """性能测试：测试不同查询模式"""
    import time

    print(f"\n{'='*60}")
    print(f"🚀 查询性能测试")
    print(f"{'='*60}")

    try:
        db = BinanceDuck()

        # 测试场景定义
        test_scenarios = [
            {
                "name": "全盘短时间扫描",
                "desc": f"扫描所有符号的{(TEST_SHORT_END - TEST_SHORT_START).seconds/3600:.1f}小时数据",
                "func": lambda: db.scan_15m_optimized(
                    TEST_SHORT_START_MS, TEST_SHORT_END_MS
                ),
                "type": "数据密集型",
            },
            {
                "name": "单符号长时序",
                "desc": f"BTCUSDT的{(TEST_LONG_END - TEST_LONG_START).days}天时间序列",
                "func": lambda: db.scan_single_symbol(
                    "BTCUSDT", TEST_LONG_START_MS, TEST_LONG_END_MS
                ),
                "type": "时间跨度型",
            },
            {
                "name": "热门符号扫描",
                "desc": "短时间内多个热门符号",
                "func": lambda: db.scan_15m_optimized(
                    TEST_SHORT_START_MS,
                    TEST_SHORT_END_MS,
                    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                ),
                "type": "精准过滤型",
            },
            {
                "name": "交易量统计",
                "desc": "聚合分析交易量排名",
                "func": lambda: db.scan_top_volume(
                    TEST_SHORT_START_MS, TEST_SHORT_END_MS, limit=20
                ),
                "type": "聚合分析型",
            },
        ]

        print(f"短时间范围: {TEST_SHORT_START} ~ {TEST_SHORT_END}")
        print(f"长时间范围: {TEST_LONG_START} ~ {TEST_LONG_END}")
        print(
            f"\n{'场景名称':<15} {'类型':<12} {'耗时':<8} {'记录数':<10} {'处理速度':<12}"
        )
        print("-" * 60)

        for scenario in test_scenarios:
            query_start = time.time()
            try:
                result = scenario["func"]()
                query_time = time.time() - query_start
                count = len(result)
                rps = count / query_time if query_time > 0 else 0

                print(
                    f"{scenario['name']:<15} {scenario['type']:<12} {query_time:.2f}s {count:<10,} {rps:>8,.0f}/s"
                )

            except Exception as e:
                query_time = time.time() - query_start
                print(
                    f"{scenario['name']:<15} {scenario['type']:<12} {query_time:.2f}s {'失败':<10} {str(e)[:20]}"
                )

        db.close()

    except Exception as e:
        print(f"❌ 测试失败: {e}")


def interactive_menu():
    """交互式菜单主循环"""
    while True:
        show_menu()
        try:
            choice = input("\n请输入选项 (1-6): ").strip()

            if choice == "1":
                print(f"\n🚀 开始ETL转换...")
                success = batch_convert()
                input(f"\n转换{'完成' if success else '完成(有错误)'}，按Enter继续...")

            elif choice == "2":
                print(f"\n🗃️ 初始化DuckDB...")
                init_duck_views()
                print("✅ DuckDB初始化完成")
                input("按Enter继续...")

            elif choice == "3":
                print(f"\n🧹 开始清理损坏文件...")
                cleanup_corrupted()
                input("按Enter继续...")

            elif choice == "4":
                print(f"\n🔍 运行性能测试...")
                run_performance_tests()
                input("按Enter继续...")

            elif choice == "5":
                get_data_status()
                input("按Enter继续...")

            elif choice == "6":
                print(f"\n👋 再见!")
                break

            else:
                print(f"❌ 无效选项 '{choice}'，请输入 1-6")
                input("按Enter继续...")

        except KeyboardInterrupt:
            print(f"\n\n👋 用户取消，再见!")
            break
        except Exception as e:
            print(f"❌ 发生错误: {e}")
            input("按Enter继续...")


# ---------- 6. CLI ----------
if __name__ == "__main__":
    import sys

    # 如果有命令行参数，直接执行对应命令
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()

        if cmd == "etl":
            print("🚀 执行ETL转换...")
            success = batch_convert()
            sys.exit(0 if success else 1)

        elif cmd == "init":
            print("🗃️ 初始化DuckDB...")
            init_duck_views()
            print("✅ DuckDB初始化完成")

        elif cmd == "cleanup":
            print("🧹 清理损坏文件...")
            cleanup_corrupted()

        elif cmd == "test":
            print("🔍 运行测试查询...")
            try:
                db = BinanceDuck()
                result = db.scan_15m(1690848000000, 1690855200000)
                print(f"✅ 测试查询成功，返回 {len(result)} 条记录")
                print(result.head())
                db.close()
            except Exception as e:
                print(f"❌ 测试查询失败: {e}")
                sys.exit(1)

        elif cmd == "status":
            get_data_status()

        elif cmd in ["help", "-h", "--help"]:
            print("\n🚀 Binance数据ETL工具")
            print("=" * 50)
            print("用法:")
            print("  python csv2duckdb.py                # 交互式菜单")
            print("  python csv2duckdb.py etl            # ETL转换")
            print("  python csv2duckdb.py init           # 初始化DuckDB")
            print("  python csv2duckdb.py cleanup        # 清理损坏文件")
            print("  python csv2duckdb.py test           # 测试查询")
            print("  python csv2duckdb.py status         # 显示状态")
            print("  python csv2duckdb.py help           # 显示帮助")

        else:
            print(f"❌ 未知命令: '{cmd}'")
            print("可用命令: etl, init, cleanup, test, status, help")
            print("或者运行 'python csv2duckdb.py' 进入交互模式")
            sys.exit(1)
    else:
        # 没有命令行参数，启动交互式菜单
        interactive_menu()
