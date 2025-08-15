"""
WebUI测试文件 - 仅用于UI开发和测试
启动带模拟数据的WebUI服务，脏乱差无所谓
"""

import asyncio
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Set, Any

import aiohttp
from aiohttp import web, WSMsgType
import aiohttp_cors


class MockWebUIService:
    """模拟WebUI服务 - 仅用于UI测试"""

    def __init__(self):
        self.websockets: Set[aiohttp.web.WebSocketResponse] = set()
        self.host = "0.0.0.0"
        self.port = 8080
        self.app = None
        self.runner = None
        self.site = None
        self.chart_data = []
        self.latest_data = {}
        self._running = True

    async def start(self):
        """启动模拟服务"""
        # 创建aiohttp应用
        self.app = web.Application()

        # 设置CORS
        cors = aiohttp_cors.setup(
            self.app,
            defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                    allow_methods="*",
                )
            },
        )

        # 添加路由
        self.app.router.add_get("/ws", self.websocket_handler)
        self.app.router.add_get("/api/status", self.status_handler)
        self.app.router.add_get("/api/data", self.data_handler)

        # 添加CORS到所有路由
        for route in list(self.app.router.routes()):
            cors.add(route)

        # 启动服务器
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()

        print(f"🌐 Mock WebUI服务启动: http://{self.host}:{self.port}")
        print("📊 前端访问: http://localhost:3000")

    async def websocket_handler(self, request):
        """WebSocket连接处理"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        self.websockets.add(ws)
        print(f"🔗 新的WebSocket连接，当前连接数: {len(self.websockets)}")

        try:
            # 发送初始数据
            if self.chart_data:
                await ws.send_str(
                    json.dumps({"type": "chart_data", "data": self.chart_data})
                )

            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    pass  # 忽略客户端消息
                elif msg.type == WSMsgType.ERROR:
                    print(f"❌ WebSocket错误: {ws.exception()}")
                    break

        except Exception as e:
            print(f"❌ WebSocket处理错误: {e}")
        finally:
            self.websockets.discard(ws)
            print(f"📤 WebSocket连接断开，当前连接数: {len(self.websockets)}")

        return ws

    async def status_handler(self, request):
        """API: 服务状态"""
        return web.json_response(
            {
                "status": "running",
                "connections": len(self.websockets),
                "timestamp": datetime.now().isoformat(),
                "mode": "mock",
            }
        )

    async def data_handler(self, request):
        """API: 获取当前数据"""
        return web.json_response(
            {"latest_data": self.latest_data, "chart_data": self.chart_data[-100:]}
        )

    async def broadcast_to_websockets(self, message: Dict[str, Any]):
        """广播消息到所有WebSocket连接"""
        if not self.websockets:
            return

        message_str = json.dumps(message)
        disconnected = []

        for ws in self.websockets:
            try:
                await ws.send_str(message_str)
            except ConnectionResetError:
                disconnected.append(ws)
            except Exception as e:
                print(f"❌ WebSocket发送失败: {e}")
                disconnected.append(ws)

        # 清理断开的连接
        for ws in disconnected:
            self.websockets.discard(ws)

    async def generate_mock_data(self):
        """生成模拟数据循环"""
        base_price = 65000
        base_ema = 64500
        start_time = datetime.now() - timedelta(hours=20)

        # 初始化一些历史数据
        for i in range(20):
            timestamp = start_time + timedelta(hours=i * 4)
            price_drift = random.randint(-100, 100) * i
            price = base_price + price_drift + random.randint(-500, 500)
            ema200 = base_ema + price_drift // 2 + random.randint(-200, 200)

            data_point = {
                "symbol": "BTCUSDT",
                "price": price,
                "ema200": ema200,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M"),
                "progress": i * 5,
            }

            # 偶尔添加交易信号
            if i > 5 and random.random() < 0.2:
                if (
                    price > ema200
                    and self.chart_data
                    and self.chart_data[-1]["price"] <= self.chart_data[-1]["ema200"]
                ):
                    data_point["signal"] = "BUY"
                elif (
                    price < ema200
                    and self.chart_data
                    and self.chart_data[-1]["price"] >= self.chart_data[-1]["ema200"]
                ):
                    data_point["signal"] = "SELL"

            self.chart_data.append(data_point)

        print(f"📈 初始化了{len(self.chart_data)}个模拟数据点")

        # 实时数据生成循环
        while self._running:
            try:
                # 生成新数据点
                last_data = self.chart_data[-1] if self.chart_data else None
                if last_data:
                    # 基于上一个数据点生成
                    price_change = random.randint(-300, 300)
                    ema_change = price_change // 3 + random.randint(-50, 50)

                    new_price = last_data["price"] + price_change
                    new_ema = last_data["ema200"] + ema_change

                    # 确保价格合理
                    new_price = max(60000, min(70000, new_price))
                    new_ema = max(60000, min(70000, new_ema))

                    current_time = datetime.now()
                    data_point = {
                        "symbol": "BTCUSDT",
                        "price": new_price,
                        "ema200": new_ema,
                        "timestamp": current_time.strftime("%Y-%m-%d %H:%M"),
                        "progress": min(
                            last_data["progress"] + random.randint(1, 3), 100
                        ),
                    }

                    # 检查交易信号
                    if (
                        last_data["price"] <= last_data["ema200"]
                        and new_price > new_ema
                    ):
                        data_point["signal"] = "BUY"
                        print(f"🟢 生成买入信号: ${new_price} > ${new_ema}")
                    elif (
                        last_data["price"] >= last_data["ema200"]
                        and new_price < new_ema
                    ):
                        data_point["signal"] = "SELL"
                        print(f"🔴 生成卖出信号: ${new_price} < ${new_ema}")

                    # 更新数据
                    self.latest_data = data_point
                    self.chart_data.append(data_point)

                    # 保持最近50个数据点
                    if len(self.chart_data) > 50:
                        self.chart_data = self.chart_data[-50:]

                    # 广播到前端
                    await self.broadcast_to_websockets(
                        {"type": "strategy_update", "data": data_point}
                    )

                    print(
                        f"📊 推送数据: ${data_point['price']} (进度: {data_point['progress']}%)"
                    )

                # 等待3-8秒随机时间
                await asyncio.sleep(random.randint(3, 8))

            except Exception as e:
                print(f"❌ 数据生成错误: {e}")
                await asyncio.sleep(5)

    async def stop(self):
        """停止服务"""
        self._running = False

        # 关闭所有WebSocket连接
        for ws in self.websockets.copy():
            await ws.close()

        # 停止Web服务器
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

        print("🛑 Mock WebUI服务已停止")


async def main():
    """主函数"""
    print("🧪 启动WebUI Mock测试服务")
    print("⚠️  这是测试服务，包含模拟数据")
    print("")

    service = MockWebUIService()

    try:
        # 启动服务
        await service.start()

        # 并发运行数据生成
        await asyncio.gather(service.generate_mock_data(), return_exceptions=True)

    except KeyboardInterrupt:
        print("\n⏹️ 用户停止服务")
    except Exception as e:
        print(f"❌ 服务错误: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await service.stop()


if __name__ == "__main__":
    # 安装依赖提示
    try:
        import aiohttp
        import aiohttp_cors
    except ImportError:
        print("❌ 缺少依赖，请安装:")
        print("pip install aiohttp aiohttp-cors")
        exit(1)

    asyncio.run(main())
