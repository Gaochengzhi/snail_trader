"""
SchedulerService: Controls global strategy execution timing.

In live mode: Triggers GLOBAL_STEP events every N minutes
In backtest mode: Waits for DATA_PROCESSED events to trigger next step
"""

import asyncio
from typing import Dict, Any, List, Type

from core import AbstractService, MessageBus, Topics, Ports


class SchedulerService(AbstractService):
    """
    Central scheduler that coordinates strategy execution timing.

    This service acts as the global clock for the system:
    - Live mode: Time-based scheduling (every N minutes)
    - Backtest mode: Event-driven scheduling (on data processing)
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("scheduler", config)
        self.message_bus = MessageBus("scheduler")

        # Configuration
        self.mode = config.get("mode", "live")
        self.step_interval = (
            config.get("step_interval_minutes", 5) * 60
        )  # Convert to seconds
        self.current_step = 0

        # Registered services
        self.registered_services: List[tuple] = (
            []
        )  # List of (service_class, config) tuples
        self.running_services: List[AbstractService] = []

    async def initialize(self):
        """Initialize scheduler and message bus."""
        await super().initialize()
        await self.message_bus.initialize()

        if self.mode == "backtest":
            # In backtest mode, subscribe to DATA_PROCESSED events
            self.message_bus.register_handler(
                Topics.DATA_PROCESSED, self._handle_data_processed
            )
            # Start subscription loop in background
            asyncio.create_task(
                self.message_bus.subscribe_loop(
                    Ports.GLOBAL_EVENTS, [Topics.DATA_PROCESSED]
                )
            )

    def register_service(
        self, service_class: Type[AbstractService], config: Dict[str, Any]
    ):
        """Register a service to be managed by the scheduler."""
        self.registered_services.append((service_class, config))
        self.logger.info(f"Registered service: {service_class.__name__}")

    async def _start_registered_services(self):
        """Start all registered services as background tasks."""
        for service_class, config in self.registered_services:
            service = service_class(config)
            self.running_services.append(service)

            # Start service as background task
            asyncio.create_task(service.async_run())
            self.logger.info(f"Started service: {service_class.__name__}")

    async def async_run(self):
        """Main scheduler loop."""
        await self.initialize()
        self._running = True

        try:
            # Start all registered services first
            await self._start_registered_services()

            if self.mode == "live":
                await self._run_live_mode()
            else:
                await self._run_backtest_mode()

        except Exception as e:
            self.logger.error(f"Scheduler error: {e}")
            raise
        finally:
            await self.cleanup()

    async def _run_live_mode(self):
        """
        Live mode: Time-based global step triggering.

        Publishes GLOBAL_STEP events every N minutes to coordinate
        strategy execution across all strategy services.
        """
        self.logger.info(
            f"Scheduler running in live mode, interval: {self.step_interval}s"
        )

        while self._running:
            # Publish global step event
            await self._publish_global_step()

            # Wait for next step interval
            await asyncio.sleep(self.step_interval)

    async def _run_backtest_mode(self):
        """
        Backtest mode: Event-driven step triggering.

        Waits for DATA_PROCESSED events from DataFetchService
        and triggers corresponding GLOBAL_STEP events.
        """
        self.logger.info("Scheduler running in backtest mode")

        # In backtest mode, we just keep the service alive
        # Step triggering is handled by _handle_data_processed
        while self._running:
            await asyncio.sleep(1)

    async def _publish_global_step(self):
        """Publish a GLOBAL_STEP event to coordinate strategy execution."""
        step_data = {
            "step_number": self.current_step,
            "timestamp": asyncio.get_event_loop().time(),
            "mode": self.mode,
        }

        await self.message_bus.publish(Topics.GLOBAL_STEP, step_data)

        self.logger.info(f"Published GLOBAL_STEP {self.current_step}")
        self.current_step += 1

    async def _handle_data_processed(self, message: Dict[str, Any]):
        """
        Handle DATA_PROCESSED event in backtest mode.

        Each time DataFetchService processes a historical data point,
        trigger the next global step.
        """
        self.logger.debug(f"Received DATA_PROCESSED: {message}")

        # Trigger next global step
        await self._publish_global_step()

    async def cleanup(self):
        """Clean up scheduler resources."""
        await super().cleanup()

        # Clean up all running services
        for service in self.running_services:
            try:
                await service.cleanup()
            except Exception as e:
                self.logger.error(
                    f"Error cleaning up service {type(service).__name__}: {e}"
                )

        await self.message_bus.cleanup()
        self.logger.info("Scheduler service cleaned up")
