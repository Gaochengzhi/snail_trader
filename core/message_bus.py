"""
MessageBus wrapper for ZeroMQ communication patterns.

Encapsulates PUB/SUB, PUSH/PULL, and REQ/REP patterns used in the framework.
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, Callable
import zmq
import zmq.asyncio

from .constants import Topics, Ports


class MessageBus:
    """
    Unified interface for ZeroMQ messaging patterns.
    
    Provides methods for:
    - Publishing events (PUB/SUB)
    - Pushing task results (PUSH/PULL) 
    - Making state requests (REQ/REP)
    """
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = logging.getLogger(f"messagebus.{service_name}")
        
        # ZeroMQ context and sockets
        self.context = zmq.asyncio.Context()
        self.sockets: Dict[str, zmq.asyncio.Socket] = {}
        
        # Subscription handlers
        self.subscription_handlers: Dict[str, Callable] = {}
    
    async def initialize(self):
        """Initialize ZeroMQ sockets based on service needs."""
        self.logger.info(f"Initializing MessageBus for {self.service_name}")
        # Sockets will be created on-demand by specific methods
    
    async def cleanup(self):
        """Close all ZeroMQ sockets and context."""
        self.logger.info(f"Cleaning up MessageBus for {self.service_name}")
        
        for socket_name, socket in self.sockets.items():
            socket.close()
            self.logger.debug(f"Closed socket: {socket_name}")
        
        self.context.term()
    
    # PUB/SUB Pattern Methods
    
    def get_publisher(self, port: int) -> zmq.asyncio.Socket:
        """Get or create a PUB socket on specified port."""
        socket_key = f"pub_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.PUB)
            socket.bind(f"tcp://*:{port}")
            self.sockets[socket_key] = socket
            self.logger.info(f"Created PUB socket on port {port}")
        
        return self.sockets[socket_key]
    
    def get_subscriber(self, port: int, topics: list = None) -> zmq.asyncio.Socket:
        """Get or create a SUB socket connected to specified port."""
        socket_key = f"sub_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.SUB)
            socket.connect(f"tcp://localhost:{port}")
            
            # Subscribe to specified topics or all topics
            if topics:
                for topic in topics:
                    socket.setsockopt_string(zmq.SUBSCRIBE, topic)
            else:
                socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all
            
            self.sockets[socket_key] = socket
            self.logger.info(f"Created SUB socket for port {port}, topics: {topics}")
        
        return self.sockets[socket_key]
    
    async def publish(self, topic: str, data: Dict[str, Any], port: int = Ports.GLOBAL_EVENTS):
        """Publish a message to specified topic."""
        try:
            socket = self.get_publisher(port)
            message = {
                'topic': topic,
                'sender': self.service_name,
                'timestamp': asyncio.get_event_loop().time(),
                'data': data
            }
            
            # Send topic first, then JSON data
            await socket.send_string(topic, zmq.SNDMORE)
            await socket.send_string(json.dumps(message))
            
            self.logger.debug(f"Published to {topic}: {data}")
            
        except Exception as e:
            self.logger.error(f"Failed to publish to {topic}: {e}")
    
    async def subscribe_loop(self, port: int, topics: list = None):
        """
        Start subscription loop for specified topics.
        
        Messages are dispatched to registered handlers.
        """
        socket = self.get_subscriber(port, topics)
        
        self.logger.info(f"Starting subscription loop for port {port}")
        
        try:
            while True:
                # Receive topic and message
                topic = await socket.recv_string()
                message_json = await socket.recv_string()
                
                try:
                    message = json.loads(message_json)
                    await self._handle_subscription_message(topic, message)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON in message: {e}")
                    
        except Exception as e:
            self.logger.error(f"Subscription loop error: {e}")
    
    def register_handler(self, topic: str, handler: Callable):
        """Register a handler function for a specific topic."""
        self.subscription_handlers[topic] = handler
        self.logger.info(f"Registered handler for topic: {topic}")
    
    async def _handle_subscription_message(self, topic: str, message: Dict[str, Any]):
        """Dispatch received message to appropriate handler."""
        if topic in self.subscription_handlers:
            try:
                await self.subscription_handlers[topic](message)
            except Exception as e:
                self.logger.error(f"Handler error for topic {topic}: {e}")
        else:
            self.logger.warning(f"No handler registered for topic: {topic}")
    
    # PUSH/PULL Pattern Methods
    
    def get_pusher(self, port: int) -> zmq.asyncio.Socket:
        """Get or create a PUSH socket connected to specified port."""
        socket_key = f"push_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.PUSH)
            socket.connect(f"tcp://localhost:{port}")
            self.sockets[socket_key] = socket
            self.logger.info(f"Created PUSH socket for port {port}")
        
        return self.sockets[socket_key]
    
    def get_puller(self, port: int) -> zmq.asyncio.Socket:
        """Get or create a PULL socket bound to specified port."""
        socket_key = f"pull_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.PULL)
            socket.bind(f"tcp://*:{port}")
            self.sockets[socket_key] = socket
            self.logger.info(f"Created PULL socket on port {port}")
        
        return self.sockets[socket_key]
    
    async def push_result(self, data: Dict[str, Any], port: int = Ports.TASK_RESULTS):
        """Push task result or similar data."""
        try:
            socket = self.get_pusher(port)
            message = {
                'sender': self.service_name,
                'timestamp': asyncio.get_event_loop().time(),
                'data': data
            }
            
            await socket.send_string(json.dumps(message))
            self.logger.debug(f"Pushed result: {data}")
            
        except Exception as e:
            self.logger.error(f"Failed to push result: {e}")
    
    async def pull_results_loop(self, port: int = Ports.TASK_RESULTS):
        """
        Start loop to pull results from PUSH sockets.
        
        Used by DataAnalyticsService to collect task results.
        """
        socket = self.get_puller(port)
        
        self.logger.info(f"Starting pull loop on port {port}")
        
        try:
            while True:
                message_json = await socket.recv_string()
                
                try:
                    message = json.loads(message_json)
                    await self._handle_pulled_message(message)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON in pulled message: {e}")
                    
        except Exception as e:
            self.logger.error(f"Pull loop error: {e}")
    
    async def _handle_pulled_message(self, message: Dict[str, Any]):
        """Handle pulled message - override in subclasses."""
        self.logger.info(f"Received pulled message: {message}")
        # TODO: Implement specific handling in DataAnalyticsService
    
    # REQ/REP Pattern Methods
    
    def get_requester(self, port: int) -> zmq.asyncio.Socket:
        """Get or create a REQ socket for making requests."""
        socket_key = f"req_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            self.sockets[socket_key] = socket
            self.logger.info(f"Created REQ socket for port {port}")
        
        return self.sockets[socket_key]
    
    def get_responder(self, port: int) -> zmq.asyncio.Socket:
        """Get or create a REP socket for handling requests."""
        socket_key = f"rep_{port}"
        
        if socket_key not in self.sockets:
            socket = self.context.socket(zmq.REP)
            socket.bind(f"tcp://*:{port}")
            self.sockets[socket_key] = socket
            self.logger.info(f"Created REP socket on port {port}")
        
        return self.sockets[socket_key]
    
    async def request(self, data: Dict[str, Any], port: int = Ports.STATE_MANAGEMENT) -> Optional[Dict[str, Any]]:
        """Make a request and wait for response."""
        try:
            socket = self.get_requester(port)
            
            request = {
                'sender': self.service_name,
                'timestamp': asyncio.get_event_loop().time(),
                'data': data
            }
            
            await socket.send_string(json.dumps(request))
            response_json = await socket.recv_string()
            
            response = json.loads(response_json)
            self.logger.debug(f"Request response: {response}")
            
            return response
            
        except Exception as e:
            self.logger.error(f"Request failed: {e}")
            return None
    
    async def response_loop(self, port: int = Ports.STATE_MANAGEMENT):
        """
        Start loop to handle incoming requests.
        
        Used by StateManagementService to handle state requests.
        """
        socket = self.get_responder(port)
        
        self.logger.info(f"Starting response loop on port {port}")
        
        try:
            while True:
                request_json = await socket.recv_string()
                
                try:
                    request = json.loads(request_json)
                    response = await self._handle_request(request)
                    await socket.send_string(json.dumps(response))
                except json.JSONDecodeError as e:
                    error_response = {'error': f'Invalid JSON: {e}'}
                    await socket.send_string(json.dumps(error_response))
                    
        except Exception as e:
            self.logger.error(f"Response loop error: {e}")
    
    async def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming request - override in subclasses."""
        self.logger.info(f"Received request: {request}")
        # TODO: Implement specific handling in StateManagementService
        return {'status': 'not_implemented'}