# Services 组件使用说明

## UnifiedDataService - 统一数据服务

### 概述
UnifiedDataService 是系统的核心数据服务，提供统一的数据查询接口和实时指标计算。它集成了历史数据访问、实时API数据获取、指标计算和市场扫描功能。

### 核心特性
- **双模式运行**：同步REQ/REP查询 + 异步指标推送
- **自动数据源选择**：根据时间范围自动选择DuckDB或API数据
- **内存指标计算**：高效的滚动窗口和缓存机制
- **市场扫描**：自动检测异常信号并推送
- **智能回写**：API数据自动写入数据库供后续使用

### 使用方法

#### 1. 策略中的同步查询
```python
from utils.data_query import DataQuery, TimeRange, IndicatorType

class MyStrategy(AbstractStrategy):
    async def step(self):
        # 全市场扫描
        query = DataQuery(
            symbols=["*"],  # 全市场
            time_range=TimeRange.last_hours(4),
            intervals=["15m"],
            indicators=[IndicatorType.VOLUME_SPIKE, IndicatorType.PRICE_CHANGE_15M]
        )
        
        # 发送查询请求到数据服务
        response = await self.data_service.query(query)
        
        # 处理结果
        for symbol in self.monitored_symbols:
            volume_spike = response.get_indicator(symbol, IndicatorType.VOLUME_SPIKE)
            if volume_spike and volume_spike > 3.0:
                self.spawn_task(TrackingTask, {"symbol": symbol})
```

#### 2. 订阅异步指标推送
```python
class MyTask(AbstractTask):
    async def execute(self):
        # 订阅市场数据推送
        await self.message_bus.subscribe(
            Topics.MARKET_DATA,
            self._handle_indicator_update
        )
    
    async def _handle_indicator_update(self, message):
        if message["type"] == "indicator_update":
            symbol = message["symbol"]
            indicators = message["indicators"]
            
            # 处理指标更新
            if IndicatorType.RSI_14 in indicators:
                rsi = indicators[IndicatorType.RSI_14]
                if rsi > 80:  # 超买信号
                    await self._handle_sell_signal(symbol)
```

### 配置说明

在 `configs/data_serves_test.yaml` 中配置：

```yaml
data_access:
  duckdb:
    db_path: "/path/to/binance.duckdb"
    data_path: "/path/to/binance_parquet"
  api:
    rate_limit: 30  # 每秒请求数
    timeout: 30

indicators:
  default_symbols: ["BTCUSDT", "ETHUSDT"]  # 默认监控的交易对
  
execution_interval_seconds: 900  # 统一执行间隔（15分钟）：获取数据→计算指标→推送结果→市场扫描
```

### 内置指标类型

- **价格指标**：PRICE_CHANGE_15M, PRICE_CHANGE_1H, PRICE_CHANGE_24H
- **技术指标**：RSI_14, RSI_21, SMA_20, SMA_50, SMA_200
- **成交量指标**：VOLUME_SPIKE, VOLUME_RATIO, VOLUME_SMA_20
- **波动率指标**：ATR_14, BOLLINGER_UPPER, BOLLINGER_LOWER

### 市场扫描器

#### 成交量异常扫描器
自动检测成交量超过平均值2倍以上的交易对，推送 `volume_spike` 信号。

#### 价格突破扫描器  
自动检测15分钟价格变化超过5%的交易对，推送 `price_breakout` 信号。

### 数据流向

1. **历史数据**：DuckDB → 数据访问层 → 指标引擎 → 策略
2. **实时数据**：Binance API → 数据访问层 → 自动回写DuckDB → 指标引擎
3. **指标推送**：指标引擎 → Message Bus → 所有订阅者
4. **市场扫描**：扫描器 → Message Bus → 策略/任务

---

## LogService - 中心化日志服务

### 概述
LogService 是一个单例模式的中心化日志服务，负责收集和处理所有其他服务组件的日志消息。它提供线程安全的日志记录功能，支持同时输出到控制台和文件。

### 核心特性
- **单例模式**：全局唯一实例，确保日志统一管理
- **线程安全**：使用队列和后台线程处理控制台输出，避免阻塞异步操作
- **双重输出**：同时支持控制台实时显示和文件持久化存储
- **文件轮转**：自动管理日志文件大小和备份数量
- **ZeroMQ 集成**：通过消息总线接收其他服务的日志

### 使用方法

#### 1. 自动集成（推荐）
LogService 已经自动集成到 AbstractService 和 AbstractTask 中，无需手动初始化：

```python
from core.base import AbstractService

class MyService(AbstractService):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)  # LogService 自动初始化
    
    async def async_run(self):
        # 直接使用 self.log() 方法
        self.log("INFO", "开始执行主循环")
        try:
            # 业务逻辑
            self.log("DEBUG", "处理中...")
        except Exception as e:
            self.log("ERROR", f"执行失败: {str(e)}")
            # 异常会自动记录到日志

class MyTask(AbstractTask):
    async def execute(self) -> Dict[str, Any]:
        self.log("INFO", "执行任务逻辑")
        return {"status": "success"}
```

#### 2. 直接使用 LogService（不推荐）
如果需要直接使用，可以获取单例实例：

```python
from services.log_service import get_log_service

log_service = get_log_service()
log_service.log_message("external_component", "INFO", "外部组件消息")

config = {
    "log_service": {
        "directory": "custom_logs",
        "max_file_size_mb": 20
    }
}
log_service = get_log_service(config)
```

### 配置选项

在 YAML 配置文件中添加以下配置：

```yaml
log_service:
  directory: "logs"              # 日志文件目录，默认: "logs"
  console_output: true           # 是否输出到控制台，默认: true
  file_output: true              # 是否输出到文件，默认: true
  max_file_size_mb: 10          # 单个日志文件最大大小(MB)，默认: 10
  backup_count: 5               # 保留的日志文件备份数量，默认: 5
```

### 输出格式

#### 控制台输出格式
```
[HH:MM:SS] service_name: message
```
示例：
```
[14:23:15] data_fetch_service: 获取 BTCUSDT 数据成功
[14:23:16] scheduler_service: 执行全局步骤 #105
[14:23:17] strategy_ma: 买入信号触发，价格: $45230.50
```

#### 文件输出格式
```
YYYY-MM-DD HH:MM:SS - LEVEL - service_name: message
```
示例：
```
2024-03-15 14:23:15 - INFO - data_fetch_service: 获取 BTCUSDT 数据成功
2024-03-15 14:23:16 - INFO - scheduler_service: 执行全局步骤 #105
2024-03-15 14:23:17 - WARNING - strategy_ma: 买入信号触发，价格: $45230.50
```

### 架构设计

#### 线程模型
- **主线程**：异步服务运行，处理业务逻辑
- **后台线程**：专门处理控制台输出，避免阻塞主线程
- **消息队列**：线程间安全通信

#### 文件管理
- 单一日志文件：`logs/snail_trader.log`
- 自动轮转：文件大小超限时自动创建新文件
- 备份管理：自动删除过期的备份文件

### 线程安全和并发处理

LogService 设计为线程安全，可以处理多个服务实例的并发日志请求：

1. **单例创建安全**：使用 `threading.Lock()` 确保单例模式线程安全
2. **队列操作安全**：使用 Python 内置的线程安全 `Queue()`
3. **文件写入安全**：Python `logging` 模块的 Handler 在多线程环境下是安全的
4. **控制台输出安全**：专用后台线程串行处理，避免输出混乱

### 最佳实践

1. **使用内置方法**：优先使用 `self.log()` 方法，避免直接调用 LogService
2. **合适的日志级别**：
   - INFO：正常业务流程
   - DEBUG：调试信息和详细数据
   - WARNING：警告信息，不影响正常运行
   - ERROR：错误信息，需要关注
3. **简洁的消息格式**：日志消息应该简洁明了，便于阅读和分析
4. **避免敏感信息**：不要在日志中记录密码、API 密钥等敏感信息
5. **异常自动记录**：AbstractService 会自动记录异常，无需手动处理

### 注意事项

- LogService 使用单例模式，全局共享同一个实例
- 服务初始化时自动创建 LogService 实例，无需担心初始化时机
- 服务关闭时会自动清理资源，包括停止后台线程和刷新剩余日志
- 如果消息队列满了，会直接打印到控制台避免阻塞
- 文件输出失败不会影响控制台输出，确保日志不丢失
- **无数据竞争**：多个服务实例可以安全地并发写入日志