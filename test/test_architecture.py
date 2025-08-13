#!/usr/bin/env python3
"""
Simple test script for the new ZMQ-based architecture.
"""

import asyncio
import signal
import sys

from utils.config_utils import Config
from services.data_fetch_service import DataFetchService
from services.data_analytics_service import DataAnalyticsService
from services.log_service import LogService


async def test_services():
    """Test the services independently."""
    
    # Load configuration
    config = Config('data_serves_test')
    service_config = config.settings.__dict__.copy()
    
    print("🚀 Starting architecture test...")
    
    try:
        # Test LogService first
        print("📋 Testing LogService...")
        log_service = LogService(service_config)
        await log_service.initialize()
        log_service.log_message("Test", "INFO", "LogService initialized successfully", "SYSTEM")
        
        # Test DataFetchService
        print("📊 Testing DataFetchService...")
        data_service = DataFetchService(service_config)
        await data_service.initialize()
        
        # Test DataAnalyticsService
        print("📈 Testing DataAnalyticsService...")
        analytics_service = DataAnalyticsService(service_config)
        await analytics_service.initialize()
        
        # Run services for a short period
        print("⚡ Running services for 10 seconds...")
        
        # Create tasks for each service
        log_task = asyncio.create_task(log_service.async_run())
        data_task = asyncio.create_task(data_service.async_run())
        analytics_task = asyncio.create_task(analytics_service.async_run())
        
        # Wait for 10 seconds or until interrupted
        await asyncio.wait_for(
            asyncio.gather(log_task, data_task, analytics_task, return_exceptions=True), 
            timeout=10
        )
        
    except asyncio.TimeoutError:
        print("✅ Test completed successfully after 10 seconds")
    except KeyboardInterrupt:
        print("\n⏹️  Test stopped by user")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        print("🧹 Cleaning up...")
        try:
            await log_service.cleanup()
            await data_service.cleanup() 
            await analytics_service.cleanup()
        except:
            pass


def handle_signal(signum, frame):
    """Handle shutdown signal."""
    print(f"\n📡 Received signal {signum}, shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    try:
        asyncio.run(test_services())
        print("✨ All tests completed!")
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)