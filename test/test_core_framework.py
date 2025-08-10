"""
演示抽象调度框架用法的测试案例。

这些测试展示了如何使用核心框架组件的示例。
"""

import time
import logging
from typing import Any


from core import Context, Task, ScheduledTask, Lifecycle, Engine, Registry


# 示例任务实现
class DataCollectionTask(Task):
    """模拟数据收集的示例任务。"""

    def __init__(self, data_source: str = "market"):
        self.data_source = data_source

    def execute(self, context: Context) -> Any:
        # 模拟数据收集
        data = {"timestamp": time.time(), "source": self.data_source, "value": 100.0}
        context.set("market_data", data)
        return data


class StrategyTask(ScheduledTask):
    """运行策略逻辑的示例调度任务。"""

    def __init__(self, strategy_name: str = "simple"):
        self.strategy_name = strategy_name

    def should_run(self, context: Context) -> bool:
        # 只有当存在市场数据时才运行
        return context.has("market_data")

    def execute(self, context: Context) -> Any:
        market_data = context.get("market_data")

        # 简单的策略逻辑
        signal = "BUY" if market_data["value"] > 50 else "SELL"

        result = {
            "strategy": self.strategy_name,
            "signal": signal,
            "timestamp": time.time(),
        }

        context.set("strategy_signal", result)
        return result


class ConfigReloader(Lifecycle):
    """管理配置的示例生命周期组件。"""

    def __init__(self):
        self.config_version = 1
        self.step_count = 0

    def before_step(self, context: Context) -> Context:
        self.step_count += 1

        # 模拟每3步重新加载一次配置
        if self.step_count % 3 == 0:
            self.config_version += 1
            context.set("config_version", self.config_version)
            print(f"配置重新加载到版本 {self.config_version}")

        return context

    def after_step(self, context: Context, results: Any) -> None:
        print(f"第 {self.step_count} 步完成，产生了 {len(results)} 个结果")

    def on_error(self, context: Context, error: Exception) -> bool:
        print(f"发生错误: {error}")
        return True  # 继续执行


class LoggingLifecycle(Lifecycle):
    """记录执行事件的示例生命周期组件。"""

    def before_engine_start(self, context: Context) -> Context:
        print("=== 引擎启动 ===")
        context.set("engine_start_time", time.time())
        return context

    def after_engine_stop(self, context: Context) -> None:
        start_time = context.get("engine_start_time", time.time())
        duration = time.time() - start_time
        print(f"=== 引擎停止 (运行了 {duration:.2f}秒) ===")


def test_basic_framework():
    """测试基本框架功能。"""
    print("\n=== 测试基本框架 ===")

    # 创建引擎
    engine = Engine()

    # 创建并注册任务
    data_task = DataCollectionTask("test_source")
    strategy_task = StrategyTask("test_strategy")

    engine.register_task(data_task)
    engine.register_task(strategy_task)

    # 创建并注册生命周期处理器
    config_lifecycle = ConfigReloader()
    logging_lifecycle = LoggingLifecycle()

    engine.register_lifecycle(config_lifecycle)
    engine.register_lifecycle(logging_lifecycle)

    # 创建上下文提供者
    def create_context():
        context = Context()
        context.set("system_time", time.time())
        return context

    # 运行单步
    context = create_context()
    results = engine.step(context)

    print(f"结果: {results}")
    print(f"上下文数据: {dict(context.items())}")


def test_registry():
    """测试组件注册表功能。"""
    print("\n=== 测试注册表 ===")

    registry = Registry()

    # 注册类
    registry.register("data_task", DataCollectionTask)
    registry.register("strategy_task", StrategyTask)

    # 注册工厂函数
    def create_data_task(source: str):
        return DataCollectionTask(source)

    registry.register_factory("custom_data_task", create_data_task)

    # 测试创建
    task1 = registry.create("data_task", "registry_source")
    task2 = registry.create("custom_data_task", "custom_source")

    print(f"创建的任务: {task1}, {task2}")
    print(f"注册表内容: {registry.list_components()}")


def test_context_operations():
    """测试上下文容器功能。"""
    print("\n=== 测试上下文操作 ===")

    # 创建上下文
    ctx1 = Context()
    ctx1.set("key1", "value1")
    ctx1.set("key2", {"nested": "data"})
    ctx1.set_metadata("version", "1.0")

    ctx2 = Context()
    ctx2.set("key2", "overwritten")
    ctx2.set("key3", "new_value")

    # 测试合并
    merged = ctx1.merge(ctx2)
    print(f"原始 ctx1: {dict(ctx1.items())}")
    print(f"原始 ctx2: {dict(ctx2.items())}")
    print(f"合并后上下文: {dict(merged.items())}")

    # 测试复制
    copied = ctx1.copy()
    copied.set("key1", "modified")

    print(f"复制修改后的原始上下文: {ctx1.get('key1')}")
    print(f"修改后的副本: {copied.get('key1')}")


def test_scheduled_tasks():
    """测试调度任务行为。"""
    print("\n=== 测试调度任务 ===")

    engine = Engine()

    # 只在满足特定条件时运行的任务
    class ConditionalTask(ScheduledTask):
        def __init__(self, required_key: str):
            self.required_key = required_key
            self.run_count = 0

        def should_run(self, context: Context) -> bool:
            return context.has(self.required_key)

        def execute(self, context: Context) -> Any:
            self.run_count += 1
            return f"条件任务运行了 {self.run_count} 次"

    conditional_task = ConditionalTask("trigger_key")
    engine.register_task(conditional_task)

    # 测试不带触发条件
    context1 = Context()
    results1 = engine.step(context1)
    print(f"不带触发条件的结果: {results1}")

    # 测试带触发条件
    context2 = Context()
    context2.set("trigger_key", True)
    results2 = engine.step(context2)
    print(f"带触发条件的结果: {results2}")


def test_error_handling():
    """测试生命周期中的错误处理。"""
    print("\n=== 测试错误处理 ===")

    class ErrorTask(Task):
        def execute(self, context: Context) -> Any:
            raise ValueError("模拟错误")

    class ErrorHandlingLifecycle(Lifecycle):
        def __init__(self):
            self.error_count = 0

        def on_error(self, context: Context, error: Exception) -> bool:
            self.error_count += 1
            print(f"处理错误 #{self.error_count}: {error}")
            return self.error_count < 2  # 2个错误后停止

    engine = Engine()
    engine.register_task(ErrorTask())
    engine.register_lifecycle(ErrorHandlingLifecycle())

    try:
        context = Context()
        engine.step(context)
    except ValueError as e:
        print(f"处理后传播的错误: {e}")


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)

    # 运行所有测试
    test_basic_framework()
    test_registry()
    test_context_operations()
    test_scheduled_tasks()
    test_error_handling()

    print("\n=== 所有测试完成 ===")
