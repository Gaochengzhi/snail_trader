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
- Always ignore these directories when listing files: `data/`, `binance-db/`, `.venv/`
- These directories contain large data files or environment-specific files that don't need to be shown

# Snail Trader 开发手册

## 架构概述
Snail Trader 是一个基于异步服务架构的量化交易框架，使用 ZeroMQ 消息总线进行组件间通信。

### 核心组件
- **SchedulerService** (`services/scheduler_service.py`): 全局调度器，控制策略执行时序
- **DataFetchService** (`services/data_fetch_service.py`): 市场数据获取服务  
- **StateManagementService** (`services/state_management_service.py`): 状态管理服务
- **DataAnalyticsService** (`services/data_analytics_service.py`): 数据分析服务



# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.