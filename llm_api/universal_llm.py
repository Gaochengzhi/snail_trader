import time
import logging
from typing import Optional, Dict, Any, Callable, List, Union
from dataclasses import dataclass
import requests
from openai import OpenAI
import threading
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as ConcurrentTimeoutError,
)
from functools import wraps

from utils.config_utils import Config

logger = logging.getLogger(__name__)


@dataclass
class ProviderConfig:
    """Single LLM provider configuration"""

    name: str
    provider: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    model: str = ""
    max_concurrent: int = 3


@dataclass
class TimeoutProfile:
    """Unified timeout configuration"""

    connection: float = 10.0
    read: float = 30.0
    total: float = 120.0

    def for_requests(self) -> tuple:
        """Returns tuple (connection_timeout, read_timeout) for requests"""
        return (self.connection, self.read)


class UniversalLLM:
    """Unified LLM interface with fallback support"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, config_name: Optional[str] = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(UniversalLLM, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_name: Optional[str] = None):
        if hasattr(self, "_initialized"):
            return

        self.config = self._load_config(config_name)
        self.providers: Dict[str, ProviderConfig] = {}
        self.clients: Dict[str, Any] = {}
        self.current_provider = None

        # Unified timeout configuration
        self.timeout = TimeoutProfile(
            connection=self.config.get("connection_timeout", 10),
            read=self.config.get("first_byte_timeout", 30),
            total=self.config.get("total_timeout", 120),
        )

        # Shared thread pool for timeout handling
        self.executor = ThreadPoolExecutor(
            max_workers=5, thread_name_prefix="llm-timeout"
        )

        # Provider handler mapping
        self.provider_handlers = {
            "openai": self._call_openai_compatible,
            "gemini": self._call_openai_compatible,
            "openroute": self._call_openai_compatible,
            "gemini_flash": self._call_openai_compatible,
            "ollama": self._call_ollama,
        }

        self._init_providers()
        self._init_clients()
        self._initialized = True

    def _load_config(self, config_name: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML"""
        try:
            config_loader = Config(config_name)
            yaml_config = config_loader.settings.model_dump()
            return yaml_config.get("llm", {})
        except Exception as e:
            logger.error(f"Failed to load YAML config: {e}")
            raise

    def _init_providers(self):
        """Initialize provider configurations"""
        providers_config = self.config.get("providers", {})
        for name, config in providers_config.items():
            self.providers[name] = ProviderConfig(
                name=name,
                provider=config.get("provider", ""),
                api_key=config.get("api_key"),
                base_url=config.get("base_url"),
                model=config.get("model", ""),
                max_concurrent=config.get("max_concurrent", 3),
            )

    def _init_clients(self):
        """Initialize API clients"""
        for name, provider in self.providers.items():
            if (
                provider.provider in ["openai", "gemini", "openroute", "gemini_flash"]
                and provider.api_key
            ):
                try:
                    self.clients[name] = OpenAI(
                        api_key=provider.api_key,
                        base_url=provider.base_url,
                        timeout=self.timeout.total,
                    )
                    logger.debug(f"Client '{name}' initialized successfully")
                except Exception as e:
                    logger.warning(f"Failed to initialize client '{name}': {e}")

    def _call_with_timeout(self, provider_name: str, func: Callable, *args, **kwargs):
        """Execute function with timeout using shared thread pool"""
        future = self.executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=self.timeout.read)
        except ConcurrentTimeoutError:
            future.cancel()
            raise requests.exceptions.Timeout(
                f"Provider {provider_name} timeout: {self.timeout.read}s"
            )
        except Exception as e:
            future.cancel()
            raise e

    def _call_openai_compatible(
        self, provider_name: str, messages: Union[str, List[Dict[str, str]]]
    ) -> str:
        """Call OpenAI-compatible API"""
        client = self.clients.get(provider_name)
        if not client:
            raise ValueError(f"Client '{provider_name}' not initialized")

        provider = self.providers[provider_name]

        # Convert string prompt to messages format for backward compatibility
        if isinstance(messages, str):
            messages = [{"role": "user", "content": messages}]

        kwargs = {
            "model": provider.model,
            "messages": messages,
            "temperature": self.config.get("temperature", 0.7),
        }

        max_tokens = self.config.get("max_tokens")
        if max_tokens:
            kwargs["max_tokens"] = max_tokens

        def _make_request():
            response = client.chat.completions.create(**kwargs)
            return response.choices[0].message.content

        return self._call_with_timeout(provider_name, _make_request)

    def _call_ollama(
        self, provider_name: str, messages: Union[str, List[Dict[str, str]]]
    ) -> str:
        """Call Ollama API"""
        provider = self.providers[provider_name]

        # Try chat API first (for multi-turn), fallback to generate API (for single prompt)
        if isinstance(messages, str):
            # Use the old generate API for single string prompts
            url = f"{provider.base_url}/api/generate"
            payload = {
                "model": provider.model,
                "prompt": messages,
                "stream": False,
                "options": {"temperature": self.config.get("temperature", 0.7)},
            }

            max_tokens = self.config.get("max_tokens")
            if max_tokens:
                payload["options"]["num_predict"] = max_tokens

            def _make_request():
                response = requests.post(
                    url, json=payload, timeout=self.timeout.for_requests()
                )
                response.raise_for_status()
                result = response.json()
                return result.get("response", "")

        else:
            # Use the new chat API for message format
            url = f"{provider.base_url}/api/chat"
            payload = {
                "model": provider.model,
                "messages": messages,
                "stream": False,
                "options": {"temperature": self.config.get("temperature", 0.7)},
            }

            max_tokens = self.config.get("max_tokens")
            if max_tokens:
                payload["options"]["num_predict"] = max_tokens

            def _make_request():
                response = requests.post(
                    url, json=payload, timeout=self.timeout.for_requests()
                )
                response.raise_for_status()
                result = response.json()
                return result.get("message", {}).get("content", "")

        return self._call_with_timeout(provider_name, _make_request)

    def _call_provider(
        self, provider_name: str, messages: Union[str, List[Dict[str, str]]]
    ) -> str:
        """Call specific provider using handler mapping"""
        if provider_name not in self.providers:
            raise ValueError(f"Provider '{provider_name}' not configured")

        provider = self.providers[provider_name]
        handler = self.provider_handlers.get(provider.provider)

        if not handler:
            raise ValueError(f"Unsupported provider type: {provider.provider}")

        return handler(provider_name, messages)

    def generate(self, prompt: str, verbose: bool = False) -> str:
        """Generate response with automatic retry and fallback"""
        active_fallback_list = self.config.get("active_fallback_list", [])
        max_retries = self.config.get("max_retries", 3)
        retry_delay = self.config.get("retry_delay", 1.0)

        for provider_name in active_fallback_list:
            if provider_name not in self.providers:
                if verbose:
                    print(f"⚠️ Provider '{provider_name}' not found, skipping")
                continue

            for attempt in range(max_retries):
                try:
                    if verbose:
                        print(
                            f"Using {provider_name} (attempt {attempt + 1}/{max_retries})..."
                        )

                    result = self._call_provider(provider_name, prompt)

                    if verbose:
                        print(f"✅ {provider_name} succeeded")

                    self.current_provider = provider_name
                    return result

                except (requests.exceptions.Timeout, ConcurrentTimeoutError) as e:
                    if verbose:
                        print(f"⏱️ {provider_name} timeout: {e}")
                    # Skip to next provider for timeout errors
                    break

                except Exception as e:
                    if verbose:
                        print(f"❌ {provider_name} attempt {attempt + 1} failed: {e}")

                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    elif verbose:
                        print(
                            f"🔄 {provider_name} all retries failed, switching provider"
                        )

        raise Exception("All LLM fallback providers failed")

    def chat(self, messages: List[Dict[str, str]], verbose: bool = False) -> str:
        """Multi-turn chat with conversation history

        Args:
            messages: List of message objects with 'role' and 'content' keys
                     Example: [{'role': 'user', 'content': 'Hello'},
                              {'role': 'assistant', 'content': 'Hi!'},
                              {'role': 'user', 'content': 'How are you?'}]
            verbose: Whether to print detailed logs

        Returns:
            Assistant's response as string
        """
        if not messages:
            raise ValueError("Messages list cannot be empty")

        active_fallback_list = self.config.get("active_fallback_list", [])
        max_retries = self.config.get("max_retries", 3)
        retry_delay = self.config.get("retry_delay", 1.0)

        for provider_name in active_fallback_list:
            if provider_name not in self.providers:
                if verbose:
                    print(f"⚠️ Provider '{provider_name}' not found, skipping")
                continue

            for attempt in range(max_retries):
                try:
                    if verbose:
                        print(
                            f"Using {provider_name} (attempt {attempt + 1}/{max_retries})..."
                        )

                    result = self._call_provider(provider_name, messages)

                    if verbose:
                        print(f"✅ {provider_name} succeeded")

                    self.current_provider = provider_name
                    return result

                except (requests.exceptions.Timeout, ConcurrentTimeoutError) as e:
                    if verbose:
                        print(f"⏱️ {provider_name} timeout: {e}")
                    # Skip to next provider for timeout errors
                    break

                except Exception as e:
                    if verbose:
                        print(f"❌ {provider_name} attempt {attempt + 1} failed: {e}")

                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    elif verbose:
                        print(
                            f"🔄 {provider_name} all retries failed, switching provider"
                        )

        raise Exception("All LLM fallback providers failed")

    def get_status(self) -> Dict[str, Any]:
        """Get current status information"""
        current_provider_config = self.providers.get(self.current_provider)

        return {
            "active_fallback_list": self.config.get("active_fallback_list", []),
            "current_provider": self.current_provider,
            "current_provider_type": (
                current_provider_config.provider if current_provider_config else None
            ),
            "current_model": (
                current_provider_config.model if current_provider_config else None
            ),
            "max_retries": self.config.get("max_retries", 3),
            "temperature": self.config.get("temperature", 0.7),
            "timeout_config": {
                "connection": self.timeout.connection,
                "read": self.timeout.read,
                "total": self.timeout.total,
            },
            "available_providers": list(self.providers.keys()),
            "initialized_clients": list(self.clients.keys()),
        }

    @classmethod
    def reset_instance(cls):
        """Reset singleton instance (for testing)"""
        with cls._lock:
            if cls._instance and hasattr(cls._instance, "executor"):
                cls._instance.executor.shutdown(wait=True)
            cls._instance = None

    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, "executor"):
            self.executor.shutdown(wait=False)


def create_llm(config_name: Optional[str] = None) -> UniversalLLM:
    """Create LLM instance"""
    return UniversalLLM(config_name)
