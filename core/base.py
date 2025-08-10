"""
Abstract base classes for the quantitative trading framework.

Design decisions:
- Using asyncio + multiprocessing for better I/O handling and true parallelism
- Each service runs in its own process for fault isolation
- ZeroMQ provides inter-process communication
- State persistence through DuckDB for durability
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from multiprocessing import Process


class AbstractService(ABC, Process):
    """
    Base class for all services in the system.
    
    Each service runs as an independent process and communicates
    via ZeroMQ. This provides fault isolation and scalability.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name=name)
        self.config = config
        self.logger = self._setup_logger(name)
        self._running = False
    
    def _setup_logger(self, name: str) -> logging.Logger:
        """Setup service-specific logger."""
        logger = logging.getLogger(f"service.{name}")
        handler = logging.FileHandler(f"logs/{name}.log")
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
    def run(self):
        """Process entry point - runs the async service loop."""
        try:
            self.logger.info(f"Starting service: {self.name}")
            asyncio.run(self.async_run())
        except Exception as e:
            self.logger.error(f"Service {self.name} crashed: {e}")
            raise
        finally:
            self.logger.info(f"Service {self.name} stopped")
    
    @abstractmethod
    async def async_run(self):
        """
        Main async service loop.
        
        Services should implement their core logic here.
        This method should run indefinitely until shutdown.
        """
        pass
    
    async def initialize(self):
        """
        Initialize service resources (ZeroMQ sockets, DB connections, etc.).
        
        Called once before the main service loop starts.
        """
        self.logger.info(f"Initializing service: {self.name}")
        # TODO: Initialize ZeroMQ sockets based on service type
        pass
    
    async def cleanup(self):
        """
        Clean up service resources.
        
        Called when the service is shutting down.
        """
        self.logger.info(f"Cleaning up service: {self.name}")
        # TODO: Close ZeroMQ sockets and other resources
        pass


class AbstractStrategy(AbstractService):
    """
    Base class for trading strategies.
    
    Strategies are services that implement trading logic through
    periodic step() execution and task spawning.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any]):
        super().__init__(f"strategy_{strategy_id}", config)
        self.strategy_id = strategy_id
        self.state: Dict[str, Any] = {}
        self.step_interval = config.get('step_interval_minutes', 5) * 60  # Convert to seconds
    
    async def async_run(self):
        """
        Main strategy loop.
        
        Subscribes to GLOBAL_STEP events and executes step() accordingly.
        In live mode, waits for scheduler signals.
        In backtest mode, processes data-driven events.
        """
        await self.initialize()
        
        try:
            # Load previous state if exists
            await self.load_state()
            
            if self.config.get('mode') == 'backtest':
                await self._run_backtest_mode()
            else:
                await self._run_live_mode()
                
        except Exception as e:
            self.logger.error(f"Strategy {self.strategy_id} error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def _run_live_mode(self):
        """Run strategy in live mode - wait for global step events."""
        self.logger.info(f"Strategy {self.strategy_id} running in live mode")
        # TODO: Subscribe to GLOBAL_STEP topic
        # TODO: Wait for step signals from SchedulerService
        while self._running:
            await self.step()
            await asyncio.sleep(1)  # Placeholder - should wait for ZeroMQ events
    
    async def _run_backtest_mode(self):
        """Run strategy in backtest mode - data-driven execution."""
        self.logger.info(f"Strategy {self.strategy_id} running in backtest mode")
        # TODO: Subscribe to DATA_PROCESSED events
        # TODO: Execute step() for each historical data point
        while self._running:
            await self.step()
            await asyncio.sleep(0.1)  # Fast execution for backtest
    
    @abstractmethod
    async def step(self):
        """
        Execute one strategy step.
        
        This method contains the core strategy logic:
        1. Analyze current market conditions
        2. Make trading decisions  
        3. Spawn tasks if needed
        4. Update internal state
        5. Save state for fault recovery
        """
        self.logger.debug(f"Executing step for strategy {self.strategy_id}")
        
        # TODO: Implement strategy-specific logic
        print(f"[PLACEHOLDER] Strategy {self.strategy_id} step execution")
        
        # Always save state after each step
        await self.save_state()
    
    async def load_state(self):
        """Load strategy state from StateManagementService."""
        try:
            # TODO: Send REQ to StateManagementService to load state
            self.logger.info(f"Loading state for strategy {self.strategy_id}")
            print(f"[PLACEHOLDER] Loading state for {self.strategy_id}")
            # self.state = loaded_state or {}
        except Exception as e:
            self.logger.warning(f"Failed to load state for {self.strategy_id}: {e}")
            self.state = {}
    
    async def save_state(self):
        """Save strategy state to StateManagementService."""
        try:
            # TODO: Send REQ to StateManagementService to save state
            self.logger.debug(f"Saving state for strategy {self.strategy_id}")
            print(f"[PLACEHOLDER] Saving state for {self.strategy_id}: {json.dumps(self.state)}")
        except Exception as e:
            self.logger.error(f"Failed to save state for {self.strategy_id}: {e}")
    
    def spawn_task(self, task_class, task_params: Dict[str, Any]):
        """
        Spawn a new task with independent scheduling.
        
        Tasks run asynchronously and report results via ZeroMQ PUSH.
        """
        self.logger.info(f"Spawning task {task_class.__name__} for strategy {self.strategy_id}")
        
        # TODO: Create task instance and run it
        # task = task_class(self.strategy_id, task_params, self.config)
        # asyncio.create_task(task.run())
        print(f"[PLACEHOLDER] Spawning task {task_class.__name__}")


class AbstractTask(ABC):
    """
    Base class for trading tasks.
    
    Tasks are independent units of work that can be spawned by strategies.
    They have their own scheduling and report results asynchronously.
    """
    
    def __init__(self, strategy_id: str, params: Dict[str, Any], config: Dict[str, Any]):
        self.strategy_id = strategy_id
        self.params = params
        self.config = config
        self.task_id = f"{strategy_id}_{self.__class__.__name__}_{id(self)}"
        self.logger = logging.getLogger(f"task.{self.task_id}")
    
    async def run(self):
        """
        Main task execution method.
        
        Implements the task's independent scheduling and execution logic.
        """
        try:
            self.logger.info(f"Starting task {self.task_id}")
            
            # Independent scheduling - tasks can run more frequently than strategy steps
            while True:
                result = await self.execute()
                await self.send_result(result)
                
                # Task-specific scheduling
                sleep_interval = self.params.get('interval_seconds', 60)
                await asyncio.sleep(sleep_interval)
                
        except Exception as e:
            self.logger.error(f"Task {self.task_id} error: {e}")
            await self.send_error(str(e))
    
    @abstractmethod
    async def execute(self) -> Dict[str, Any]:
        """
        Execute the core task logic.
        
        Returns:
            Dict containing task results to be sent to DataAnalyticsService
        """
        pass
    
    async def send_result(self, result: Dict[str, Any]):
        """Send task result to DataAnalyticsService via ZeroMQ PUSH."""
        result_data = {
            'task_id': self.task_id,
            'strategy_id': self.strategy_id,
            'timestamp': asyncio.get_event_loop().time(),
            'result': result
        }
        
        # TODO: Send via ZeroMQ PUSH to TASK_RESULTS
        self.logger.info(f"Sending result for task {self.task_id}")
        print(f"[PLACEHOLDER] Task result: {json.dumps(result_data)}")
    
    async def send_error(self, error_message: str):
        """Send task error to DataAnalyticsService."""
        error_data = {
            'task_id': self.task_id,
            'strategy_id': self.strategy_id,
            'timestamp': asyncio.get_event_loop().time(),
            'error': error_message
        }
        
        # TODO: Send via ZeroMQ PUSH to TASK_RESULTS
        self.logger.error(f"Task {self.task_id} failed: {error_message}")
        print(f"[PLACEHOLDER] Task error: {json.dumps(error_data)}")