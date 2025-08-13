# Services 组件使用说明

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