# MessageBus 使用指南 (README)

一个统一的 ZeroMQ 异步消息总线封装，支持三种常用通信模式：
- PUB / SUB（事件广播）
- PUSH / PULL（结果/任务流水线）
- REQ / REP（请求/响应状态查询）

特点：
- 单文件引入，易于集成
- 自动处理 REQ 并发安全
- 失败 socket 冷却与自动重建
- 可插拔序列化（JSON / orjson）
- 可选订阅消息处理并发限制
- 结构简洁，可扩展

---

## 1. 安装依赖

```bash
pip install pyzmq
# 可选更快 JSON
pip install orjson
```

如需日志辅助工具，可自实现 get_log_utils，或删除相关引用。

---

## 2. 快速开始

```python
import asyncio
from message_bus import MessageBus, Ports  # 假设文件名为 message_bus.py

async def main():
    bus = MessageBus(
        service_name="analytics",
        config={
            "hwm_outbound": 2000,
            "handler_max_concurrency": 20,
            "serializer": "orjson",  # 或 "json"
        }
    )

    # 注册 SUB 处理函数
    async def on_price(message):
        print("Price update:", message["data"])

    bus.register_handler("price.tick", on_price)

    # 启动订阅循环（监听事件端口）
    asyncio.create_task(bus.subscribe_loop(Ports.GLOBAL_EVENTS, topics=["price.tick"]))

    # 发布一条事件
    await bus.publish("price.tick", {"symbol": "BTC", "price": 43210.5})

    # 启动响应循环（例如状态服务）
    asyncio.create_task(bus.response_loop(Ports.STATE_MANAGEMENT))

    # 发送请求（示例：获取状态）
    resp = await bus.request({"op": "get_state"})
    print("State response:", resp)

    # 推送结果（用于任务流水线）
    await bus.push_result({"task_id": "abc123", "status": "done"})

    # 启动结果拉取循环
    asyncio.create_task(bus.pull_results_loop())

    await asyncio.sleep(2)
    await bus.cleanup()

asyncio.run(main())
```

---

## 3. 端口与主题（可自定义）

默认示例（你可以在 constants.py 中定义自己的 Ports / Topics）：

```python
class Ports:
    GLOBAL_EVENTS = 5555
    TASK_RESULTS = 5556
    STATE_MANAGEMENT = 5557

class Topics:
    PRICE_TICK = "price.tick"
```

---

## 4. 配置参数 config 字段

| 键                      | 说明                                    | 默认  |
| ----------------------- | --------------------------------------- | ----- |
| hwm_outbound            | PUB/PUSH 发送高水位线（SNDHWM）         | 1000  |
| hwm_inbound             | SUB/PULL 接收高水位线（RCVHWM）         | 1000  |
| pub_send_timeout        | publish 发送等待超时（秒）              | 1.0   |
| push_send_timeout       | push_result 超时                        | 1.0   |
| req_total_timeout       | request 总超时（发送+接收对半拆）       | 5.0   |
| rep_recv_timeout        | response_loop 单次接收等待              | 30.0  |
| rep_send_timeout        | response_loop 发送响应超时              | 5.0   |
| failed_socket_cooldown  | 失败 socket 冷却时间（秒）              | 10.0  |
| handler_max_concurrency | 订阅消息处理最大并发（None 表示不限制） | None  |
| log_level_no_handler    | 没有 handler 的 topic 日志级别          | DEBUG |
| serializer              | json 或 orjson                          | json  |
| close_linger_ms         | 关闭 linger 毫秒                        | 100   |

---

## 5. 常用 API

| 方法                                           | 说明                          |
| ---------------------------------------------- | ----------------------------- |
| publish(topic, data, port=Ports.GLOBAL_EVENTS) | 发布事件                      |
| subscribe_loop(port, topics=None)              | 订阅循环（需放入 task）       |
| register_handler(topic, fn)                    | 注册处理函数（同步/异步均可） |
| push_result(data, port=Ports.TASK_RESULTS)     | 发送结果                      |
| pull_results_loop(port=Ports.TASK_RESULTS)     | 拉取结果循环                  |
| request(data, port=Ports.STATE_MANAGEMENT)     | 发送请求等待响应              |
| response_loop(port=Ports.STATE_MANAGEMENT)     | 响应请求循环                  |
| cleanup(cancel_running=True)                   | 清理资源                      |
| get_metrics()                                  | 获取当前指标 dict             |

---

## 6. Handler 定义规则

支持：
- async def handler(msg):
- def handler(msg):（会自动用线程池包装）

示例：
```python
def sync_handler(msg):
    # 计算密集型任务
    pass

async def async_handler(msg):
    await asyncio.sleep(0.01)

bus.register_handler("price.tick", sync_handler)
bus.register_handler("trade.executed", async_handler)
```

限制并发：
```python
bus = MessageBus("svc", config={"handler_max_concurrency": 50})
```

---

## 7. REQ/REP 使用注意

- REQ socket 严格 send -> recv 顺序，本实现为每个端口加锁，自动序列化请求。
- 超时 / 异常会关闭并标记 socket，进入冷却（默认 10 秒）后可自动重建。
- 后端 response_loop 覆盖 _handle_request 提供业务逻辑：

```python
class StateBus(MessageBus):
    async def _handle_request(self, request):
        op = request["data"].get("op")
        if op == "get_state":
            return {"state": "OK", "ts": time.time()}
        return {"error": "unknown op"}
```

---

## 8. PUSH/PULL 常见使用场景

- 异步任务结果收集 / 流式数据处理。
- push_result 发送任意 dict；pull_results_loop 中覆盖 _handle_pulled_message：

```python
class ResultBus(MessageBus):
    async def _handle_pulled_message(self, message):
        print("Result:", message)
```

---

## 9. Metrics 指标解释

调用 get_metrics() 返回示例：

```python
{
  'messages_sent': 15,
  'messages_received': 20,
  'errors': 1,
  'outbound_dropped': 2,
  'inbound_dropped': 0,
  'backpressure_events': 2,
  'request_timeouts': 1,
  'failed_bind_count': 0,
  'active_connections': 5
}
```

说明：
- outbound_dropped：发送阶段由于超时/失败导致丢弃
- inbound_dropped：收到格式不合法或帧错误
- backpressure_events：当前实现主要由发送超时触发
- request_timeouts：REQ 请求超时次数
- failed_bind_count：bind 端口占用或失败次数
- active_connections：当前打开的 socket 数

可接入 Prometheus：定期拉取 metrics，导出为 Gauge / Counter。

---

## 10. 失败 Socket 与恢复

- 发送或请求超时、ZMQError 会触发 _fail_socket
- 记录失败时间，处于冷却期的 socket 再次调用会直接跳过（并统计 dropped）
- 冷却结束后自动尝试重建
- 可通过 config['failed_socket_cooldown'] 调整窗口

---

## 11. 自定义序列化

默认 JSON，可改为 orjson（自动检测可用性）：
```python
bus = MessageBus("svc", config={"serializer": "orjson"})
```

如果想扩展自定义：
1. 自己实现 Serializer 子类
2. 修改 build_serializer 逻辑或直接 bus.serializer = CustomSerializer()

---

## 12. 扩展点（继承）

```python
class MyBus(MessageBus):
    async def _handle_request(self, request):
        return {"echo": request["data"]}

    async def _handle_pulled_message(self, message):
        # 自定义处理
        pass
```

---

## 13. 生命周期建议

1. 创建实例
2. 注册 handler
3. 启动各 loop：subscribe_loop / pull_results_loop / response_loop 放入 create_task
4. 调用 publish / push_result / request
5. 应用退出前 await cleanup()

示例：
```python
tasks = [
    asyncio.create_task(bus.subscribe_loop(Ports.GLOBAL_EVENTS)),
    asyncio.create_task(bus.response_loop(Ports.STATE_MANAGEMENT)),
]
...
await bus.cleanup()
```

---

## 14. 日志策略

- 需要精简日志可设置 logging level > INFO
- 无 handler 的 topic 默认 DEBUG，不污染生产日志
- 异常统一统计 metrics.errors 并写日志

---

## 15. 常见问题 (FAQ)

Q: 为什么我第一条 SUB 收不到消息？  
A: PUB/SUB 连接建立需要时间。首次 publish 可 sleep(0.1) 或做“握手”消息。

Q: publish/push 超时很多？  
A: 增大 hwm_outbound 或 pub_send_timeout；检查消费者是否在运行。

Q: request 一直返回 None？  
A: 后端未启动 response_loop 或 _handle_request 未实现；检查端口一致。超时后 socket 进入冷却期，需等待或调整 failed_socket_cooldown。

Q: 如何避免 handler 阻塞？  
A: CPU 密集型逻辑用同步函数即可（to_thread 自动线程池）；或自行拆分为任务队列。

Q: 如何实现多服务共享同一个 Context？  
A: 当前类每实例创建一个 Context，你可以自行外部注入：修改 __init__ 让 context 可传入。

---

## 16. 生产建议

- 明确端口分配，避免冲突
- 配置合理的超时与 HWM（根据负载压测）
- 监控 metrics（特别是 dropped / timeouts）
- 关键通道可加消息 ID 与签名（你可以在 publish 包装 data）
- 需要可靠传输请结合上层持久化（ZeroMQ 默认不做消息存储）

---

## 17. 示例：结合 FastAPI

```python
from fastapi import FastAPI
import asyncio

bus = MessageBus("api")

app = FastAPI()

@app.on_event("startup")
async def start():
    asyncio.create_task(bus.response_loop())

@app.on_event("shutdown")
async def stop():
    await bus.cleanup()

@app.get("/state")
async def get_state():
    resp = await bus.request({"op": "get_state"})
    return {"raw": resp}
```

---

## 18. 获取完整代码

将 MessageBus 单文件放到你的工程（如 message_bus.py），按上述用法导入即可使用，后续可逐步拆分成模块化结构。

---

## 19. 后续可扩展方向

- 增加 correlation_id / trace_id
- Prometheus exporter
- 自动重试 publish（有限重试 + jitter）
- Handler 错误隔离（熔断、重试）
- 插件式中间件（发送前 / 接收后 hook）
- TLS / CURVE 安全（ZeroMQ 支持）

---

如需：
- 多文件重构版本
- 增加自动重试策略
- 增加 tracing / metrics 导出

可继续提出，我再给对应方案。祝使用顺利！