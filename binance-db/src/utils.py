import os
import requests
import yaml
from datetime import datetime, timedelta
from typing import List, Dict, Any
from logger_setup import get_logger


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file"""
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return config


def ensure_directory_exists(directory: str) -> None:
    """Create directory if it doesn't exist"""
    os.makedirs(directory, exist_ok=True)


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start_date and end_date (inclusive)"""
    if end_date.lower() == "latest":
        end_date = datetime.now().strftime("%Y-%m-%d")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def get_all_trading_pairs() -> List[str]:
    """Get all available trading pairs with local file priority"""
    import json
    from pathlib import Path
    import time
    
    logger = get_logger()
    
    # First try local symbols.json file
    symbols_file = Path("symbols.json")
    if symbols_file.exists():
        try:
            with open(symbols_file, 'r') as f:
                symbols_data = json.load(f)
                symbols = symbols_data.get('symbols', [])
                if symbols:
                    logger.info(f"Using local symbols.json: {len(symbols)} symbols")
                    return symbols
        except Exception as e:
            logger.debug(f"Failed to load symbols.json: {e}")
    
    # Fallback to cache file path
    cache_file = Path("trading_pairs_cache.json")
    cache_duration = 24 * 3600  # 24 hours in seconds
    
    # Try to load from cache
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                cache_time = cache_data.get('timestamp', 0)
                
                # Check if cache is still valid (less than 24 hours old)
                if time.time() - cache_time < cache_duration:
                    symbols = cache_data.get('symbols', [])
                    if symbols:
                        logger.info(f"Using cached trading pairs: {len(symbols)} symbols")
                        return symbols
        except Exception as e:
            logger.debug(f"Failed to load cached trading pairs: {e}")
    
    # Cache invalid or doesn't exist, fetch from API
    logger.info("Fetching trading pairs from Binance API...")
    
    # Multiple retry attempts with different approaches
    for attempt in range(3):
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            
            # Different request configurations for each attempt
            if attempt == 0:
                # Standard request
                response = requests.get(url, timeout=15)
            elif attempt == 1:
                # With session and headers
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Binance-Data-Downloader/2.0',
                    'Accept': 'application/json'
                })
                response = session.get(url, timeout=20)
                session.close()
            else:
                # Last attempt with longer timeout and SSL verification disabled
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                response = requests.get(url, timeout=30, verify=False)
                
            response.raise_for_status()
            data = response.json()
            
            symbols = [
                symbol["symbol"]
                for symbol in data["symbols"]
                if symbol["status"] == "TRADING" and 
                   symbol["contractType"] == "PERPETUAL" and
                   symbol["quoteAsset"] == "USDT"
            ]
            
            symbols = sorted(symbols)
            
            # Save to cache
            try:
                cache_data = {
                    'timestamp': time.time(),
                    'symbols': symbols
                }
                with open(cache_file, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                logger.info(f"Cached {len(symbols)} trading pairs")
            except Exception as e:
                logger.debug(f"Failed to save trading pairs cache: {e}")
            
            return symbols
            
        except Exception as e:
            logger.debug(f"Attempt {attempt + 1} failed: {e}")
            if attempt < 2:  # Not the last attempt
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                # Last attempt failed, try to use stale cache or fallback
                if cache_file.exists():
                    try:
                        with open(cache_file, 'r') as f:
                            cache_data = json.load(f)
                            symbols = cache_data.get('symbols', [])
                            if symbols:
                                logger.info(f"API failed, using stale cache: {len(symbols)} symbols")
                                return symbols
                    except Exception:
                        pass
                
                # Try fallback trading pairs file
                fallback_file = Path("fallback_trading_pairs.json")
                if fallback_file.exists():
                    try:
                        with open(fallback_file, 'r') as f:
                            fallback_data = json.load(f)
                            symbols = fallback_data.get('symbols', [])
                            if symbols:
                                logger.info(f"API and cache failed, using fallback list: {len(symbols)} symbols")
                                return symbols
                    except Exception as fe:
                        logger.debug(f"Failed to load fallback trading pairs: {fe}")
                
                raise Exception(f"Failed to fetch trading pairs from Binance Futures API after 3 attempts: {e}")


def build_download_url(base_url: str, data_type: str, symbol: str, date: str, interval: str = None) -> str:
    """Build download URL based on data type and parameters"""
    if data_type in ["indexPriceKlines", "klines", "markPriceKlines", "premiumIndexKlines"]:
        if interval is None:
            raise ValueError(f"Interval is required for {data_type}")
        return f"{base_url}{data_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
    else:
        return f"{base_url}{data_type}/{symbol}/{symbol}-{data_type}-{date}.zip"


def get_output_filename(symbol: str, data_type: str, date: str, interval: str = None, extension: str = "csv") -> str:
    """Generate output filename for processed data (matching Binance's actual filename format)"""
    # 只有 metrics 和 bookDepth 在文件名中包含数据类型标识
    types_with_datatype = ["metrics", "bookDepth"]
    
    if data_type in types_with_datatype:
        # 带数据类型标识：symbol-datatype-date 格式
        return f"{symbol}-{data_type}-{date}.{extension}"
    elif interval:
        # 有interval的类型（K线类型）：symbol-interval-date 格式
        return f"{symbol}-{interval}-{date}.{extension}"
    else:
        # 其他没有interval的类型：symbol-datatype-date 格式
        return f"{symbol}-{data_type}-{date}.{extension}"


def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1

    return f"{size_bytes:.2f}{size_names[i]}"


def get_file_directory(data_type: str, symbol: str, interval: str = None, base_dir: str = "./data") -> str:
    """Get the directory path for storing files"""
    if interval:
        return os.path.join(base_dir, data_type, symbol, interval)
    else:
        return os.path.join(base_dir, data_type, symbol)