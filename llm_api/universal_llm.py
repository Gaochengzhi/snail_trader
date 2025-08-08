import time
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
import requests
from openai import OpenAI
from pathlib import Path
import sys
import socket
import threading
from queue import Queue, PriorityQueue
from concurrent.futures import ThreadPoolExecutor, Future
import uuid

# 添加项目路径以导入config_utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.config_utils import Config

logger = logging.getLogger(__name__)


@dataclass
class ProviderConfig:
    """单个LLM提供商配置"""
    provider: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    model: str = ""


class ConcurrencyManager:
    """统一并发控制管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.concurrency_enabled = config.get("concurrency_control", {}).get("enabled", True)
        
        # 为每个provider创建信号量和线程池
        self.provider_semaphores: Dict[str, threading.Semaphore] = {}
        self.provider_executors: Dict[str, ThreadPoolExecutor] = {}
        
        # 全局请求队列
        self.global_queue_size = config.get("concurrency_control", {}).get("global_queue_size", 100)
        self.request_queue = PriorityQueue(maxsize=self.global_queue_size)
        self.queue_worker_running = False
        
        self._init_provider_controls()
    
    def _init_provider_controls(self):
        """为每个provider初始化并发控制"""
        providers = self.config.get("providers", {})
        for provider_name, provider_config in providers.items():
            max_concurrent = provider_config.get("max_concurrent", 3)
            
            # 创建信号量控制并发数
            self.provider_semaphores[provider_name] = threading.Semaphore(max_concurrent)
            
            # 创建线程池执行请求
            self.provider_executors[provider_name] = ThreadPoolExecutor(
                max_workers=max_concurrent,
                thread_name_prefix=f"llm-{provider_name}"
            )
            
            logger.debug(f"Provider {provider_name} 最大并发数: {max_concurrent}")
    
    def execute_request(self, provider_name: str, request_func, *args, **kwargs):
        """执行带并发控制的请求"""
        if not self.concurrency_enabled:
            return request_func(*args, **kwargs)
        
        semaphore = self.provider_semaphores.get(provider_name)
        executor = self.provider_executors.get(provider_name)
        
        if not semaphore or not executor:
            logger.warning(f"Provider {provider_name} 并发控制未初始化，直接执行")
            return request_func(*args, **kwargs)
        
        # 使用信号量控制并发
        with semaphore:
            logger.debug(f"Provider {provider_name} 获取并发许可，当前可用: {semaphore._value}")
            try:
                return request_func(*args, **kwargs)
            finally:
                logger.debug(f"Provider {provider_name} 释放并发许可")
    
    def get_provider_status(self, provider_name: str) -> Dict[str, Any]:
        """获取provider并发状态"""
        semaphore = self.provider_semaphores.get(provider_name)
        executor = self.provider_executors.get(provider_name)
        provider_config = self.config.get("providers", {}).get(provider_name, {})
        
        if not semaphore or not executor:
            return {"status": "not_initialized"}
        
        max_concurrent = provider_config.get("max_concurrent", 3)
        current_concurrent = max_concurrent - semaphore._value
        
        return {
            "max_concurrent": max_concurrent,
            "current_concurrent": current_concurrent,
            "available_slots": semaphore._value,
            "queue_size": executor._work_queue.qsize() if hasattr(executor._work_queue, 'qsize') else 0
        }
    
    def shutdown(self):
        """关闭所有线程池"""
        for executor in self.provider_executors.values():
            executor.shutdown(wait=True)


class UniversalLLM:
    """统一LLM接口，支持多个LLM提供商的fallback切换"""
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(UniversalLLM, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
            
        self.config = self._load_config_from_yaml()
        self._clients: Dict[str, Any] = {}
        self.current_provider = None
        
        # 超时配置
        self.connection_timeout = self.config.get("connection_timeout", 10)
        self.first_byte_timeout = self.config.get("first_byte_timeout", 30) 
        self.total_timeout = self.config.get("total_timeout", 120)
        
        # 初始化并发控制管理器
        self.concurrency_manager = ConcurrencyManager(self.config)
        
        # 初始化客户端
        self._init_clients()
        self._initialized = True

    def _load_config_from_yaml(self) -> Dict[str, Any]:
        """从YAML配置文件加载配置"""
        try:
            config_loader = Config()
            yaml_config = config_loader.settings.model_dump()
            return yaml_config.get("llm", {})
        except Exception as e:
            logger.error(f"加载YAML配置失败: {e}")
            raise



    def _init_clients(self):
        """初始化客户端"""
        providers = self.config.get("providers", {})
        for provider_name, provider_config in providers.items():
            try:
                provider_type = provider_config.get("provider")
                if provider_type in ["openai", "gemini", "openroute", "gemini_flash"]:
                    api_key = provider_config.get("api_key")
                    base_url = provider_config.get("base_url")
                    if api_key:
                        self._clients[provider_name] = OpenAI(
                            api_key=api_key,
                            base_url=base_url,
                            timeout=self.total_timeout
                        )
                        logger.debug(f"客户端 '{provider_name}' 初始化成功")
            except Exception as e:
                logger.warning(f"初始化客户端 '{provider_name}' 失败: {e}")

    def _call_openai_compatible(self, provider_name: str, prompt: str) -> str:
        """调用OpenAI兼容的API"""
        def _execute_request():
            client = self._clients.get(provider_name)
            if not client:
                raise ValueError(f"客户端 '{provider_name}' 未初始化")

            provider_config = self.config["providers"][provider_name]
            messages = [{"role": "user", "content": prompt}]

            kwargs = {
                "model": provider_config["model"],
                "messages": messages,
                "temperature": self.config.get("temperature", 0.7),
            }

            max_tokens = self.config.get("max_tokens")
            if max_tokens:
                kwargs["max_tokens"] = max_tokens

            response = client.chat.completions.create(**kwargs)
            return response.choices[0].message.content
        
        return self.concurrency_manager.execute_request(provider_name, _execute_request)

    def _call_ollama(self, provider_name: str, prompt: str) -> str:
        """调用Ollama API"""
        def _execute_request():
            provider_config = self.config["providers"][provider_name]
            url = f"{provider_config['base_url']}/api/generate"

            payload = {
                "model": provider_config["model"],
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": self.config.get("temperature", 0.7)},
            }

            max_tokens = self.config.get("max_tokens")
            if max_tokens:
                payload["options"]["num_predict"] = max_tokens

            # 使用分层超时：(连接超时, 读取超时)
            timeout = (self.connection_timeout, self.first_byte_timeout)
            response = requests.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            result = response.json()
            return result.get("response", "")
        
        return self.concurrency_manager.execute_request(provider_name, _execute_request)


    def generate(self, prompt: str, verbose: bool = False) -> str:
        """
        生成回复，支持fallback切换，改进的超时处理
        """
        active_fallback_list = self.config.get("active_fallback_list", [])
        max_retries = self.config.get("max_retries", 3)
        retry_delay = self.config.get("retry_delay", 1.0)
        
        for provider_name in active_fallback_list:
            if provider_name not in self.config.get("providers", {}):
                if verbose:
                    print(f"⚠️ Provider '{provider_name}' 配置未找到，跳过")
                continue

            for attempt in range(max_retries):
                try:
                    if verbose:
                        print(f"使用 {provider_name} 调用 (尝试 {attempt + 1}/{max_retries})...")
                    
                    result = self._call_provider_with_timeout(provider_name, prompt)
                    
                    if verbose:
                        print(f"✅ {provider_name} 调用成功")
                    
                    self.current_provider = provider_name
                    return result

                except (requests.exceptions.Timeout, socket.timeout) as e:
                    if verbose:
                        print(f"⏱️ {provider_name} 超时: {e}")
                    # 超时错误直接跳到下一个provider，不浪费重试次数
                    break
                    
                except Exception as e:
                    if verbose:
                        print(f"❌ {provider_name} 尝试 {attempt + 1} 失败: {e}")
                    
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        # 所有重试都失败，跳到下一个provider
                        if verbose:
                            print(f"🔄 {provider_name} 所有重试失败，切换到下一个provider")

        raise Exception("所有LLM fallback都失败")

    def _call_provider_with_timeout(self, provider_name: str, prompt: str) -> str:
        """带超时控制的provider调用"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Provider {provider_name} 首字节超时 ({self.first_byte_timeout}s)")
        
        try:
            # 设置信号超时（用于检测首字节超时）
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(self.first_byte_timeout)
            
            result = self._call_provider(provider_name, prompt)
            
            # 取消超时
            signal.alarm(0)
            return result
            
        except TimeoutError:
            signal.alarm(0)
            raise requests.exceptions.Timeout(f"首字节超时: {self.first_byte_timeout}s")
        except Exception:
            signal.alarm(0)
            raise

    def _call_provider(self, provider_name: str, prompt: str) -> str:
        """调用指定的provider"""
        provider_config = self.config["providers"][provider_name]
        provider_type = provider_config.get("provider")

        if provider_type in ["openai", "gemini", "openroute", "gemini_flash"]:
            return self._call_openai_compatible(provider_name, prompt)
        elif provider_type == "ollama":
            return self._call_ollama(provider_name, prompt)
        else:
            raise ValueError(f"不支持的provider类型: {provider_type}")

    def get_status(self) -> Dict[str, Any]:
        """获取当前状态信息"""
        providers = self.config.get("providers", {})
        current_provider_config = providers.get(self.current_provider, {})
        
        # 获取每个provider的并发状态
        provider_concurrency_status = {}
        for provider_name in providers.keys():
            provider_concurrency_status[provider_name] = self.concurrency_manager.get_provider_status(provider_name)
        
        return {
            "active_fallback_list": self.config.get("active_fallback_list", []),
            "current_provider": self.current_provider,
            "current_provider_type": current_provider_config.get("provider"),
            "current_model": current_provider_config.get("model"),
            "enable_streaming": self.config.get("enable_streaming", False),
            "max_retries": self.config.get("max_retries", 3),
            "temperature": self.config.get("temperature", 0.7),
            "connection_timeout": self.connection_timeout,
            "first_byte_timeout": self.first_byte_timeout,
            "total_timeout": self.total_timeout,
            "concurrency_enabled": self.concurrency_manager.concurrency_enabled,
            "provider_concurrency_status": provider_concurrency_status,
            "available_providers": list(providers.keys()),
            "initialized_clients": list(self._clients.keys()),
        }
    
    def __del__(self):
        """析构函数，清理资源"""
        if hasattr(self, 'concurrency_manager'):
            self.concurrency_manager.shutdown()


def create_llm() -> UniversalLLM:
    """创建LLM实例"""
    return UniversalLLM()
