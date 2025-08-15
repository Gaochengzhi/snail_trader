#!/usr/bin/env python3
"""
Simple runner script for the Binance Public Data Downloader
"""

import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import and run main
from src.main import main

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)