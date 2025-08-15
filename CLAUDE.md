# Snail Trader Project

## Package Management
- This project uses **uv** for package management
- To add new packages: `source .venv/bin/activate && uv add package_name`
- Always activate virtual environment first before adding packages
- **IMPORTANT**: Project is installed as a global package, so **DO NOT** use `sys.path.append()` in any code files

## Configuration System
- Uses YAML configuration files in `configs/` directory
- Base configuration: `configs/base.yaml`
- Environment-specific configs: `configs/{env}.yaml` (e.g., `configs/ollama.yaml`)
- Environment variables are resolved from `.env` file using `${VAR_NAME}` syntax

## Documentation Organization Rules
- **Component usage instructions**: Store in the component's folder README.md (e.g., `test/README.md` for testing), Every time you update componet file you should update componet's README.md as well.
- **Project-wide rules**: Keep in root `CLAUDE.md` 
- **Exception**: Notebooks (`notebook/`) are self-contained and don't need separate README files
- **Examples**: 
  - Testing methods → `test/README.md`
  - API usage → `llm_api/README.md`
  - Utils usage → `utils/README.md`

## Dependencies
- python-dotenv: for .env file support
- pydantic: for configuration validation
- PyYAML: for YAML configuration files
- fire: for elegant CLI interface

## File Listing Rules
- Always ignore these directories when listing files: `data/`, `binance-db/`, `.venv/` unless I tell you to do so
- These directories contain large data files or environment-specific files that don't need to be shown

# 代码实践规范 (Code Practice Standards)

## 服务基础设施 (Service Infrastructure)

### 1. AbstractService 继承规则
- **MUST**: 所有服务继承自 `core.AbstractService`
- **MUST**: 在 `__init__` 中调用 `super().__init__(name, config)`
- **MUST**: 实现 `async def async_run(self)` 方法作为主循环
- **MUST**: 在 `initialize()` 中设置资源，在 `cleanup()` 中清理资源

### 2. 日志记录规范 (Logging Standards)
- **MUST**: 使用 `self.log(level, message)` 而不是 `print()` 或直接调用 `logging`
- **MUST**: 日志级别：`INFO`, `DEBUG`, `ERROR`, `WARNING`
- **NEVER**: 直接使用 `print()` 语句，除非在测试或调试代码中明确标注

### 3. 配置管理规范 (Configuration Standards)
- **MUST**: 使用点号访问配置，不使用 `.get()` 方法
- **MUST**: 配置对象默认支持 DotDict 点号访问语法
- **MUST**: 使用 `${VAR_NAME}` 语法引用环境变量
- **NEVER**: 使用 `config.get("key", default)` 或 `getattr(config, "key", default)`
- **NEVER**: 硬编码配置值，始终从配置文件或环境变量读取
- **正确示例**: `config.data_access.api.timeout`
- **错误示例**: `config.get("data_access", {}).get("api", {}).get("timeout")`

### 4. ZeroMQ 消息通信规范
- **MUST**: 使用 `core.MessageBus` 进行服务间通信
- **MUST**: 使用 `core.constants.Topics` 和 `core.constants.Ports` 定义的常量
- **MUST**: 在服务初始化时创建 MessageBus: `self.message_bus = MessageBus(service_name)`
- **MUST**: 在 cleanup 中调用 `await self.message_bus.cleanup()`


### 6. 异步编程规范
- **MUST**: 所有服务方法使用 `async/await`
- **MUST**: 使用 `asyncio.sleep()` 而不是 `time.sleep()`
- **MUST**: 正确处理 `asyncio.CancelledError` 用于优雅关闭


## 代码组织规范

### 7. 导入规范 (Import Standards)
- **MUST**: 从 `core` 导入基础类：`from core import AbstractService, MessageBus, Topics, Ports`
- **MUST**: 按以下顺序组织导入：
  1. 标准库
  2. 第三方库  
  3. 项目内部模块


# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.