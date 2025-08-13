import asyncio
import json
import logging
import time
import inspect
import threading
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, Callable, Set, List
import zmq
import zmq.asyncio

from .constants import Topics, Ports


try:
    from utils.log_utils import get_log_utils
except Exception:

    def get_log_utils(_cfg=None):
        class Dummy:
            def log_message(self, *a, **kw):
                pass

        return Dummy()


# ===================== 默认配置 =====================
DEFAULT_CONFIG = {
    "hwm_outbound": 1000,  # PUB/SUB/PUSH 的发送高水位
    "hwm_inbound": 1000,  # PULL/REQ/REP 的接收高水位
    "pub_send_timeout": 1.0,  # PUB 发送超时（秒）
    "push_send_timeout": 1.0,  # PUSH 发送超时（秒）
    "req_total_timeout": 5.0,  # REQ 总超时（发送+接收拆半）
    "rep_recv_timeout": 30.0,  # REQ/REP 接收超时 (秒)
    "rep_send_timeout": 5.0,  # REQ/REP 响应发送超时 (秒)
    "failed_socket_cooldown": 10.0,  # 失败后多少秒后允许尝试重建
    "handler_max_concurrency": None,  # 限制订阅消息处理并发
    "log_level_no_handler": "DEBUG",
    "serializer": "json",  # 预留扩展点
    "close_linger_ms": 100,  # 关闭时 linger 时间（毫秒）
}


# ===================== 序列化器 =====================
class Serializer:
    def dumps(self, obj: Any) -> str:
        raise NotImplementedError

    def loads(self, s: str) -> Any:
        raise NotImplementedError


class JsonSerializer(Serializer):
    def dumps(self, obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))

    def loads(self, s: str) -> Any:
        return json.loads(s)


class OrjsonSerializer(Serializer):
    def __init__(self):
        import orjson

        self._orjson = orjson

    def dumps(self, obj: Any) -> str:
        return self._orjson.dumps(obj).decode()

    def loads(self, s: str) -> Any:
        return self._orjson.loads(s)


def build_serializer(kind: str) -> Serializer:
    if kind == "orjson":
        try:
            return OrjsonSerializer()
        except Exception:
            pass
    return JsonSerializer()


# ===================== Metrics =====================
@dataclass
class BusMetrics:
    messages_sent: int = 0
    messages_received: int = 0
    errors: int = 0
    outbound_dropped: int = 0
    inbound_dropped: int = 0
    backpressure_events: int = 0
    request_timeouts: int = 0
    failed_bind_count: int = 0
    active_connections: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ===================== Socket 管理 =====================
class SocketRegistry:
    """
    统一管理 socket 创建/缓存/关闭，避免重复代码。
    """

    def __init__(
        self,
        context: zmq.asyncio.Context,
        linger_ms: int,
        logger: logging.Logger,
        metrics: BusMetrics,
    ):
        self._context = context
        self._sockets: Dict[str, zmq.asyncio.Socket] = {}
        self._lock = threading.Lock()
        self._linger_ms = linger_ms
        self._logger = logger
        self._metrics = metrics

    def get_or_create(
        self, key: str, create_fn: Callable[[], zmq.asyncio.Socket]
    ) -> zmq.asyncio.Socket:
        with self._lock:
            if key not in self._sockets:
                sock = create_fn()
                self._sockets[key] = sock
                self._metrics.active_connections += 1
            return self._sockets[key]

    def pop(self, key: str):
        with self._lock:
            return self._sockets.pop(key, None)

    def close_all(self):
        with self._lock:
            for key, sock in list(self._sockets.items()):
                try:
                    sock.setsockopt(zmq.LINGER, self._linger_ms)
                    sock.close()
                except Exception as e:
                    self._logger.error(f"Error closing socket {key}: {e}")
            self._sockets.clear()
            self._metrics.active_connections = 0


# ===================== MessageBus 主体 =====================
class MessageBus:
    """
    精简版统一消息总线：
    - PUB/SUB
    - PUSH/PULL
    - REQ/REP
    重点：
    - 单文件可读
    - 失败 socket 冷却
    - REQ 并发安全
    - 处理器并发可配置
    """

    def __init__(self, service_name: str, config: Dict[str, Any] = None):
        self.service_name = service_name
        self.config = {**DEFAULT_CONFIG, **(config or {})}

        self.logger = logging.getLogger(f"messagebus.{service_name}")
        self.log_utils = get_log_utils()

        self.context = zmq.asyncio.Context()
        self.metrics = BusMetrics()

        self.serializer: Serializer = build_serializer(self.config["serializer"])

        self.sockets = SocketRegistry(
            self.context,
            linger_ms=self.config["close_linger_ms"],
            logger=self.logger,
            metrics=self.metrics,
        )

        # 失败 socket 跟踪：socket_key -> last_failed_time
        self.failed_sockets: Dict[str, float] = {}
        self.failed_bind_sockets: Set[str] = set()

        # REQ 并发锁
        self._req_locks: Dict[str, asyncio.Lock] = {}

        # 订阅处理器
        self._handlers: Dict[str, Callable] = {}
        self._handler_semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(self.config["handler_max_concurrency"])
            if self.config["handler_max_concurrency"]
            else None
        )

        # 运行中的循环任务（订阅 / pull / response）
        self._running_tasks: Set[asyncio.Task] = set()

        # 无 handler 日志级别
        self._no_handler_level = self.config["log_level_no_handler"].upper()

    # ---------- 公共工具 ----------
    def _log(self, level: str, msg: str):
        self.log_utils.log_message(self.service_name, level, msg)

    def _is_failed_and_in_cooldown(self, key: str) -> bool:
        if key not in self.failed_sockets:
            return False
        last_fail = self.failed_sockets[key]
        if (time.time() - last_fail) >= self.config["failed_socket_cooldown"]:
            # 冷却结束，允许重建
            del self.failed_sockets[key]
            return False
        return True

    def _fail_socket(self, key: str):
        # 关闭并标记
        sock = self.sockets.pop(key)
        if sock:
            try:
                sock.close(0)
            except Exception:
                pass
        self.failed_sockets[key] = time.time()

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.as_dict()

    # ---------- Socket 创建函数 ----------
    def _create_pub(self, port: int) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.PUB)
        try:
            sock.setsockopt(zmq.SO_REUSEADDR, 1)
        except Exception:
            pass
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.setsockopt(zmq.SNDHWM, self.config["hwm_outbound"])
        sock.bind(f"tcp://*:{port}")
        return sock

    def _create_sub(self, port: int, topics: Optional[List[str]]) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.SUB)
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.setsockopt(zmq.RCVHWM, self.config["hwm_inbound"])
        sock.connect(f"tcp://localhost:{port}")
        if topics:
            for t in topics:
                sock.setsockopt_string(zmq.SUBSCRIBE, t)
        else:
            sock.setsockopt_string(zmq.SUBSCRIBE, "")
        return sock

    def _create_push(self, port: int) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.PUSH)
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.setsockopt(zmq.SNDHWM, self.config["hwm_outbound"])
        sock.connect(f"tcp://localhost:{port}")
        return sock

    def _create_pull(self, port: int) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.PULL)
        try:
            sock.setsockopt(zmq.SO_REUSEADDR, 1)
        except Exception:
            pass
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.setsockopt(zmq.RCVHWM, self.config["hwm_inbound"])
        sock.bind(f"tcp://*:{port}")
        return sock

    def _create_req(self, port: int) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.connect(f"tcp://localhost:{port}")
        return sock

    def _create_rep(self, port: int) -> zmq.asyncio.Socket:
        sock = self.context.socket(zmq.REP)
        try:
            sock.setsockopt(zmq.SO_REUSEADDR, 1)
        except Exception:
            pass
        sock.setsockopt(zmq.LINGER, self.config["close_linger_ms"])
        sock.bind(f"tcp://*:{port}")
        return sock

    # ---------- PUB ----------
    async def publish(
        self, topic: str, data: Dict[str, Any], port: int = Ports.GLOBAL_EVENTS
    ):
        socket_key = f"pub:{port}"
        if self._is_failed_and_in_cooldown(socket_key):
            self.metrics.outbound_dropped += 1
            return

        try:

            def create_fn():
                try:
                    return self._create_pub(port)
                except zmq.error.ZMQError as e:
                    if "Address already in use" in str(e):
                        self.metrics.failed_bind_count += 1
                        self.failed_bind_sockets.add(socket_key)
                        raise
                    raise

            sock = self.sockets.get_or_create(socket_key, create_fn)

            msg = {
                "topic": topic,
                "sender": self.service_name,
                "ts": time.time(),
                "data": data,
            }
            payload = self.serializer.dumps(msg)
            await asyncio.wait_for(
                sock.send_multipart([topic.encode(), payload.encode()]),
                timeout=self.config["pub_send_timeout"],
            )
            self.metrics.messages_sent += 1
            self._log("DEBUG", f"Published topic={topic} data={data}")

        except asyncio.TimeoutError:
            self.metrics.errors += 1
            self.metrics.outbound_dropped += 1
            self.metrics.backpressure_events += 1
            self._fail_socket(socket_key)
            self._log("ERROR", f"Publish timeout topic={topic}")
        except zmq.error.ZMQError as e:
            self.metrics.errors += 1
            self.metrics.outbound_dropped += 1
            self._fail_socket(socket_key)
            self._log("ERROR", f"Publish ZMQError: {e}")
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Publish unexpected error: {e}")

    async def subscribe_loop(self, port: int, topics: List[str] = None):
        socket_key = f"sub:{port}"
        try:
            sock = self.sockets.get_or_create(
                socket_key, lambda: self._create_sub(port, topics)
            )
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Create SUB failed: {e}")
            return

        task = asyncio.current_task()
        if task:
            self._running_tasks.add(task)
        self._log("INFO", f"Subscribe loop started on port {port}, topics={topics}")

        try:
            while True:
                try:
                    parts = await asyncio.wait_for(sock.recv_multipart(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if len(parts) != 2:
                    self.metrics.inbound_dropped += 1
                    continue

                topic = parts[0].decode()
                raw = parts[1].decode()
                self.metrics.messages_received += 1

                try:
                    msg = self.serializer.loads(raw)
                except Exception as e:
                    self.metrics.errors += 1
                    self.metrics.inbound_dropped += 1
                    self._log("ERROR", f"JSON decode error: {e}; head={raw[:80]}")
                    continue

                await self._dispatch_handler(topic, msg)

        except asyncio.CancelledError:
            self._log("INFO", f"Subscribe loop cancelled port={port}")
            raise
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Subscribe loop error: {e}")
        finally:
            if task in self._running_tasks:
                self._running_tasks.remove(task)

    def register_handler(self, topic: str, handler: Callable):
        # 自动包装同步函数
        if not inspect.iscoroutinefunction(handler):

            async def wrapper(msg):
                return await asyncio.to_thread(handler, msg)

            self._handlers[topic] = wrapper
            self._log("INFO", f"Registered sync handler -> {topic}")
        else:
            self._handlers[topic] = handler
            self._log("INFO", f"Registered async handler -> {topic}")

    async def _dispatch_handler(self, topic: str, message: Dict[str, Any]):
        h = self._handlers.get(topic)
        if not h:
            lvl = self._no_handler_level
            if lvl == "DEBUG":
                self.logger.debug(f"No handler for topic={topic}")
            else:
                self._log(lvl, f"No handler for topic={topic}")
            return
        try:
            if self._handler_semaphore:
                async with self._handler_semaphore:
                    await h(message)
            else:
                await h(message)
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Handler error topic={topic}: {e}")

    # ---------- PUSH ----------
    async def push_result(self, data: Dict[str, Any], port: int = Ports.TASK_RESULTS):
        socket_key = f"push:{port}"
        if self._is_failed_and_in_cooldown(socket_key):
            self.metrics.outbound_dropped += 1
            return
        try:
            sock = self.sockets.get_or_create(
                socket_key, lambda: self._create_push(port)
            )
            msg = {"sender": self.service_name, "ts": time.time(), "data": data}
            payload = self.serializer.dumps(msg)
            await asyncio.wait_for(
                sock.send_string(payload), timeout=self.config["push_send_timeout"]
            )
            self.metrics.messages_sent += 1
        except asyncio.TimeoutError:
            self.metrics.errors += 1
            self.metrics.outbound_dropped += 1
            self.metrics.backpressure_events += 1
            self._fail_socket(socket_key)
            self._log("ERROR", "Push timeout")
        except zmq.error.ZMQError as e:
            self.metrics.errors += 1
            self.metrics.outbound_dropped += 1
            self._fail_socket(socket_key)
            self._log("ERROR", f"Push ZMQError: {e}")
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Push unexpected error: {e}")

    async def pull_results_loop(self, port: int = Ports.TASK_RESULTS):
        socket_key = f"pull:{port}"
        try:
            sock = self.sockets.get_or_create(
                socket_key, lambda: self._create_pull(port)
            )
        except zmq.error.ZMQError as e:
            if "Address already in use" in str(e):
                self.metrics.failed_bind_count += 1
                self.failed_bind_sockets.add(socket_key)
            self.metrics.errors += 1
            self._log("ERROR", f"Create PULL failed: {e}")
            return
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Create PULL failed: {e}")
            return

        task = asyncio.current_task()
        if task:
            self._running_tasks.add(task)
        self._log("INFO", f"Pull loop started port={port}")

        try:
            while True:
                try:
                    raw = await asyncio.wait_for(sock.recv_string(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                self.metrics.messages_received += 1
                try:
                    msg = self.serializer.loads(raw)
                except Exception as e:
                    self.metrics.errors += 1
                    self.metrics.inbound_dropped += 1
                    self._log("ERROR", f"Pull JSON decode error: {e}; head={raw[:80]}")
                    continue

                await self._handle_pulled_message(msg)

        except asyncio.CancelledError:
            self._log("INFO", f"Pull loop cancelled port={port}")
            raise
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Pull loop error: {e}")
        finally:
            if task in self._running_tasks:
                self._running_tasks.remove(task)

    async def _handle_pulled_message(self, message: Dict[str, Any]):
        # 供子类扩展
        self.logger.debug(f"Pulled message: {message}")

    # ---------- REQ ----------
    async def request(
        self,
        data: Dict[str, Any],
        port: int = Ports.STATE_MANAGEMENT,
        timeout: float = None,
    ) -> Optional[Dict[str, Any]]:
        socket_key = f"req:{port}"
        if self._is_failed_and_in_cooldown(socket_key):
            return None

        if socket_key not in self._req_locks:
            self._req_locks[socket_key] = asyncio.Lock()
        lock = self._req_locks[socket_key]

        total_timeout = timeout or self.config["req_total_timeout"]
        half = total_timeout / 2

        async with lock:
            try:
                sock = self.sockets.get_or_create(
                    socket_key, lambda: self._create_req(port)
                )
                payload = self.serializer.dumps(
                    {"sender": self.service_name, "ts": time.time(), "data": data}
                )
                await asyncio.wait_for(sock.send_string(payload), timeout=half)
                self.metrics.messages_sent += 1

                resp_raw = await asyncio.wait_for(sock.recv_string(), timeout=half)
                self.metrics.messages_received += 1
                try:
                    return self.serializer.loads(resp_raw)
                except Exception as e:
                    self.metrics.errors += 1
                    self._log("ERROR", f"Request JSON decode error: {e}")
                    return None

            except asyncio.TimeoutError:
                self.metrics.errors += 1
                self.metrics.request_timeouts += 1
                self._fail_socket(socket_key)
                self._log("ERROR", f"Request timeout port={port}")
                return None
            except zmq.error.ZMQError as e:
                self.metrics.errors += 1
                self._fail_socket(socket_key)
                self._log("ERROR", f"Request ZMQError: {e}")
                return None
            except Exception as e:
                self.metrics.errors += 1
                self._fail_socket(socket_key)
                self._log("ERROR", f"Request unexpected error: {e}")
                return None

    # ---------- REP ----------
    async def response_loop(self, port: int = Ports.STATE_MANAGEMENT):
        socket_key = f"rep:{port}"
        try:
            sock = self.sockets.get_or_create(
                socket_key, lambda: self._create_rep(port)
            )
        except zmq.error.ZMQError as e:
            if "Address already in use" in str(e):
                self.metrics.failed_bind_count += 1
                self.failed_bind_sockets.add(socket_key)
            self.metrics.errors += 1
            self._log("ERROR", f"Create REP failed: {e}")
            return
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Create REP failed: {e}")
            return

        task = asyncio.current_task()
        if task:
            self._running_tasks.add(task)
        self._log("INFO", f"Response loop started port={port}")

        try:
            while True:
                try:
                    req_raw = await asyncio.wait_for(
                        sock.recv_string(), timeout=self.config["rep_recv_timeout"]
                    )
                except asyncio.TimeoutError:
                    continue

                self.metrics.messages_received += 1
                try:
                    req = self.serializer.loads(req_raw)
                except Exception as e:
                    self.metrics.errors += 1
                    err_resp = self.serializer.dumps(
                        {"error": f"Invalid JSON: {e}", "ts": time.time()}
                    )
                    await sock.send_string(err_resp)
                    continue

                try:
                    resp_obj = await self._handle_request(req)
                except Exception as e:
                    self.metrics.errors += 1
                    resp_obj = {"error": f"Handler error: {e}", "ts": time.time()}

                resp_raw = self.serializer.dumps(resp_obj)

                try:
                    await asyncio.wait_for(
                        sock.send_string(resp_raw),
                        timeout=self.config["rep_send_timeout"],
                    )
                    self.metrics.messages_sent += 1
                except asyncio.TimeoutError:
                    self.metrics.errors += 1
                    self._log("ERROR", "Response send timeout - breaking loop")
                    break

        except asyncio.CancelledError:
            self._log("INFO", f"Response loop cancelled port={port}")
            raise
        except Exception as e:
            self.metrics.errors += 1
            self._log("ERROR", f"Response loop error: {e}")
        finally:
            if task in self._running_tasks:
                self._running_tasks.remove(task)

    async def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        # 子类覆盖
        return {"status": "not_implemented"}

    # ---------- 清理 ----------
    async def cleanup(self, cancel_running: bool = True):
        self._log("INFO", "MessageBus cleanup start")
        if cancel_running:
            for task in list(self._running_tasks):
                if not task.done():
                    task.cancel()
            if self._running_tasks:
                await asyncio.gather(*self._running_tasks, return_exceptions=True)
            self._running_tasks.clear()

        self.sockets.close_all()
        try:
            self.context.term()
        except Exception as e:
            self._log("ERROR", f"Context term error: {e}")
        self._log("INFO", f"Cleanup done. Final metrics={self.metrics.as_dict()}")
