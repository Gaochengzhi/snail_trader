"""
Constants for ZeroMQ topics and ports configuration.
"""


class Topics:
    """ZeroMQ topic constants for pub/sub communication."""

    # Global events
    GLOBAL_STEP = "GLOBAL_STEP"
    MARKET_DATA = "MARKET_DATA"
    REFLECTION_UPDATE = "REFLECTION_UPDATE"
    DATA_PROCESSED = "DATA_PROCESSED"  # For backtest mode

    # Task results
    TASK_RESULTS = "TASK_RESULTS"

    # Service control
    SERVICE_START = "SERVICE_START"
    SERVICE_STOP = "SERVICE_STOP"


class Ports:
    """Default ZeroMQ port configuration."""

    # PUB/SUB ports
    GLOBAL_EVENTS = 5555  # Global step, reflection updates
    MARKET_DATA = 5556  # Market data broadcast

    # PUSH/PULL ports
    TASK_RESULTS = 5557  # Task results collection

    # REQ/REP ports
    STATE_MANAGEMENT = 5558  # State read/write requests

    @classmethod
    def get_all_ports(cls):
        """Get all configured ports."""
        return {
            "GLOBAL_EVENTS": cls.GLOBAL_EVENTS,
            "MARKET_DATA": cls.MARKET_DATA,
            "TASK_RESULTS": cls.TASK_RESULTS,
            "STATE_MANAGEMENT": cls.STATE_MANAGEMENT,
        }
