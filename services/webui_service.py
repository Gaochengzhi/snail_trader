"""
WebUI Service - 为前端React Dashboard提供实时数据
Linus式简洁：监听策略消息 -> 转发到WebSocket -> 前端显示
"""

import asyncio
import json
from typing import Dict, Set, Any
from datetime import datetime
import aiohttp
from aiohttp import web, WSMsgType
import aiohttp_cors

from core import AbstractService


class WebUIService(AbstractService):
    """WebUI服务 - 桥接策略数据与前端界面"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("webui_service", config)
        
        # WebSocket连接管理
        self.websockets: Set[aiohttp.web.WebSocketResponse] = set()
        
        # Web服务器配置
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", 8080)
        
        # 数据缓存 - 按币种分离
        self.symbol_data: Dict[str, Dict[str, Any]] = {}  # symbol -> {klines: [], signals: [], latest: {}}
        self.progress_data: Dict[str, Any] = {}
        
        # Web应用
        self.app = None
        self.runner = None
        self.site = None

    async def initialize(self):
        """初始化Web服务器和路由"""
        await super().initialize()
        
        # 创建aiohttp应用
        self.app = web.Application()
        
        # 设置CORS
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })
        
        # 添加路由
        self.app.router.add_get('/ws', self.websocket_handler)
        self.app.router.add_get('/api/status', self.status_handler)
        self.app.router.add_get('/api/data', self.data_handler)
        
        # 添加CORS到所有路由
        for route in list(self.app.router.routes()):
            cors.add(route)
        
        # 启动服务器
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        
        self.log("INFO", f"WebUI服务启动: http://{self.host}:{self.port}")

    async def async_run(self):
        """主服务循环 - 监听策略消息并转发"""
        # TODO: 集成MessageBus监听策略消息
        # 临时通过文件系统读取策略数据
        
        while self._running:
            try:
                # 保持WebSocket服务运行
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log("ERROR", f"WebUI服务错误: {e}")
                await asyncio.sleep(5)

    async def websocket_handler(self, request):
        """WebSocket连接处理"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.websockets.add(ws)
        self.log("INFO", f"新的WebSocket连接，当前连接数: {len(self.websockets)}")
        
        try:
            # 发送当前数据
            if self.symbol_data:
                await ws.send_str(json.dumps({
                    'type': 'initial_data',
                    'symbols': list(self.symbol_data.keys()),
                    'symbol_data': self.symbol_data,
                    'progress': self.progress_data
                }))
            
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    # 处理客户端消息（如果需要）
                    pass
                elif msg.type == WSMsgType.ERROR:
                    self.log("ERROR", f"WebSocket错误: {ws.exception()}")
                    break
                    
        except Exception as e:
            self.log("ERROR", f"WebSocket处理错误: {e}")
        finally:
            self.websockets.discard(ws)
            self.log("INFO", f"WebSocket连接断开，当前连接数: {len(self.websockets)}")
        
        return ws

    async def status_handler(self, request):
        """API: 服务状态"""
        return web.json_response({
            'status': 'running',
            'connections': len(self.websockets),
            'timestamp': datetime.now().isoformat()
        })

    async def data_handler(self, request):
        """API: 获取当前数据"""
        return web.json_response({
            'symbols': list(self.symbol_data.keys()),
            'symbol_data': self.symbol_data,
            'progress': self.progress_data
        })

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
                self.log("ERROR", f"WebSocket发送失败: {e}")
                disconnected.append(ws)
        
        # 清理断开的连接
        for ws in disconnected:
            self.websockets.discard(ws)


    async def handle_strategy_message(self, message: Dict[str, Any]):
        """处理来自策略的消息（真实实现）"""
        # TODO: 实际集成MessageBus时使用
        message_type = message.get('type')
        
        if message_type == 'price_update':
            # 处理价格更新
            await self._handle_price_update(message)
        elif message_type == 'trading_signal':
            # 处理交易信号
            await self._handle_trading_signal(message)
        elif message_type == 'backtest_progress':
            # 处理回测进度
            await self._handle_backtest_progress(message)

    async def _handle_price_update(self, message: Dict[str, Any]):
        """处理价格更新消息"""
        # 转换为前端格式并广播
        pass

    async def _handle_trading_signal(self, message: Dict[str, Any]):
        """处理交易信号消息"""
        # 转换为前端格式并广播
        pass

    async def _handle_backtest_progress(self, message: Dict[str, Any]):
        """处理回测进度消息"""
        # 转换为前端格式并广播
        pass

    async def _read_strategy_data(self):
        """读取策略数据并推送到前端（临时实现）"""
        import json
        import os
        
        webui_data_file = "/tmp/snail_trader_webui_data.json"
        
        try:
            if not os.path.exists(webui_data_file):
                return
                
            with open(webui_data_file, 'r') as f:
                data = json.load(f)
            
            latest_data = data.get('latest_data')
            chart_data = data.get('chart_data', [])
            
            if latest_data and chart_data:
                # 更新内部缓存
                self.latest_data = latest_data
                self.chart_data = chart_data
                
                # 推送到前端
                await self.broadcast_to_websockets({
                    'type': 'strategy_update',
                    'data': latest_data
                })
                
                self.log("DEBUG", f"推送策略数据: {latest_data['symbol']} ${latest_data['price']}")
                
        except Exception as e:
            # 静默处理文件读取错误，避免日志污染
            pass

    async def receive_data(self, message: Dict[str, Any]):
        """接收来自策略的结构化数据"""
        service = message.get('service')
        data_type = message.get('data_type')
        symbol = message.get('symbol')
        data = message.get('data')
        
        if not symbol or not data_type or not data:
            return
        
        # 按币种组织数据
        if symbol not in self.symbol_data:
            self.symbol_data[symbol] = {
                'klines': [],
                'signals': [],
                'latest': {}
            }
        
        # 根据数据类型处理
        if data_type == 'kline':
            # K线数据：保持最近100个数据点
            self.symbol_data[symbol]['klines'].append(data)
            if len(self.symbol_data[symbol]['klines']) > 100:
                self.symbol_data[symbol]['klines'] = self.symbol_data[symbol]['klines'][-100:]
            self.symbol_data[symbol]['latest'] = data
            
        elif data_type == 'signal':
            # 交易信号：保持最近20个信号
            self.symbol_data[symbol]['signals'].append(data)
            if len(self.symbol_data[symbol]['signals']) > 20:
                self.symbol_data[symbol]['signals'] = self.symbol_data[symbol]['signals'][-20:]
                
        elif data_type == 'progress':
            # 进度数据（全局）
            self.progress_data = data
        
        # 广播到前端
        await self.broadcast_to_websockets({
            'type': 'data_update',
            'data_type': data_type,
            'symbol': symbol,
            'data': data
        })
        
        self.log("DEBUG", f"接收{data_type}数据: {symbol} from {service}")

    async def cleanup(self):
        """清理资源"""
        # 关闭所有WebSocket连接
        for ws in self.websockets.copy():
            await ws.close()
        
        # 停止Web服务器
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        
        await super().cleanup()
        self.log("INFO", "WebUI服务已停止")