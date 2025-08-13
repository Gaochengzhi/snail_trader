"""
Abstract base classes for the quantitative trading framework.

Design decisions:
- Using asyncio with ZeroMQ for asynchronous message-driven architecture
- All services run in a single process with shared event loop for simplicity and debugging
- ZeroMQ provides decoupled inter-service communication
- State persistence through DuckDB for durability
"""

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from utils.log_utils import LogUtils


class AbstractService(ABC):
    """
    Base class for all services in the system.

    Services run as asyncio tasks in a shared event loop and communicate
    via ZeroMQ. This provides message-driven decoupling while maintaining
    simplicity for development and debugging.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._log_service: "LogUtils" = None

    async def run(self):
        """Main service entry point - handles shutdown gracefully."""
        try:
            await self.initialize()
            await self.async_run()
        except Exception as e:
            await self._handle_exception(e)
            raise
        finally:
            await self.cleanup()
            self.shutdown()

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
        from utils.log_utils import get_log_utils

        self._log_service = get_log_utils(self.config)
        await self._log_service.initialize()
        self.log("INFO", f"Initializing service {self.name}")

        self._running = True

    async def cleanup(self):
        """
        Clean up service resources.

        Called when the service is shutting down.
        """
        pass

    async def _handle_exception(self, exception: Exception):
        """
        Handle exceptions - subclasses can override for custom error handling.
        """
        import traceback

        error_msg = f"Exception: {str(exception)}"
        traceback_msg = f"Traceback:\n{traceback.format_exc()}"
        self._log_service.log_message(self.name, "ERROR", error_msg)
        self._log_service.log_message(self.name, "ERROR", traceback_msg)

    def log(self, level: str, message: str):
        """
        Log a message using the centralized LogService.

        Args:
            level: Log level (INFO, DEBUG, WARNING, ERROR)
            message: The log message
        """
        self._log_service.log_message(self.name, level, message)

    def shutdown(self):
        """Signal the service to shutdown gracefully."""
        self._running = False
        self._shutdown_event.set()


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
        self.step_interval = (
            config.get("step_interval_minutes", 5) * 60
        )  # Convert to seconds

    async def async_run(self):
        """
        Main strategy loop.

        Subscribes to GLOBAL_STEP events and executes step() according to configured interval.
        """
        await self.load_state()

        while self._running:
            await self.step()
            await asyncio.sleep(self.step_interval)

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
        # Always save state after each step
        await self.save_state()

    async def load_state(self):
        """Load strategy state from StateManagementService."""
        try:
            # TODO: Send REQ to StateManagementService to load state
            # self.state = loaded_state or {}
            pass
        except Exception as e:
            self.state = {}

    async def save_state(self):
        """Save strategy state to StateManagementService."""
        try:
            # TODO: Send REQ to StateManagementService to save state
            pass
        except Exception as e:
            await self._handle_exception(e)

    def spawn_task(self, task_class, task_params: Dict[str, Any]):
        """
        Spawn a new task with independent scheduling.

        Tasks run asynchronously and report results via ZeroMQ PUSH.
        """
        # TODO: Create task instance and run it
        # task = task_class(self.strategy_id, task_params, self.config)
        # asyncio.create_task(task.run())
        pass


class AbstractTask(ABC):
    """
    Base class for trading tasks.

    Tasks are independent units of work that can be spawned by strategies.
    They have their own scheduling and report results asynchronously.
    """

    def __init__(
        self, strategy_id: str, params: Dict[str, Any], config: Dict[str, Any]
    ):
        self.strategy_id = strategy_id
        self.params = params
        self.config = config
        self.task_id = f"{strategy_id}_{self.__class__.__name__}_{id(self)}"

    async def run(self):
        """
        Main task execution method.

        Implements the task's independent scheduling and execution logic.
        """
        try:
            # Independent scheduling - tasks can run more frequently than strategy steps
            while True:
                result = await self.execute()
                await self.send_result(result)

                # Task-specific scheduling
                sleep_interval = self.params.get("interval_seconds", 60)
                await asyncio.sleep(sleep_interval)

        except Exception as e:
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
            "task_id": self.task_id,
            "strategy_id": self.strategy_id,
            "timestamp": asyncio.get_event_loop().time(),
            "result": result,
        }

        # TODO: Send via ZeroMQ PUSH to TASK_RESULTS
        pass

    async def send_error(self, error_message: str):
        """Send task error to DataAnalyticsService."""
        error_data = {
            "task_id": self.task_id,
            "strategy_id": self.strategy_id,
            "timestamp": asyncio.get_event_loop().time(),
            "error": error_message,
        }

        # TODO: Send via ZeroMQ PUSH to TASK_RESULTS
        pass
