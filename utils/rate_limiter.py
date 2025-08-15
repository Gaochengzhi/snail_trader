"""
智能限流器 - 集成监控和自适应限流的统一组件
"""

import asyncio
import time
from typing import Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from collections import deque

from utils.log_utils import get_log_utils


class LimitStatus(Enum):
    NORMAL = "normal"
    WARNING = "warning" 
    LIMITED = "limited"
    ERROR = "error"


@dataclass
class RateLimitConfig:
    initial_rate: float = 30.0
    min_rate: float = 1.0
    max_rate: float = 50.0
    backoff_factor: float = 0.5
    recovery_factor: float = 1.1
    timeout_threshold: float = 10.0
    error_threshold: int = 3


@dataclass
class RequestMetrics:
    timestamp: float
    response_time: float
    success: bool
    status_code: Optional[int] = None
    error_message: Optional[str] = None


class RateLimiter:
    """智能限流器
    
    集成功能：
    1. 令牌桶限流算法
    2. API性能监控
    3. 自适应速率调整
    4. 健康状态检查
    5. 统一告警机制
    """
    
    def __init__(self, name: str, config: RateLimitConfig = None, log_service=None):
        self.name = name
        self.config = config or RateLimitConfig()
        self.log_service = log_service or get_log_utils()
        
        # 令牌桶
        self.current_rate = self.config.initial_rate
        self.tokens = self.current_rate
        self.last_update = time.time()
        
        # 监控数据
        self.metrics_history: deque = deque(maxlen=1000)
        self.request_count = 0
        self.error_count = 0
        self.consecutive_errors = 0
        self.total_response_time = 0.0
        
        # 状态
        self.status = LimitStatus.NORMAL
        self.last_adjustment = time.time()
        self.health_status = "healthy"
        
        # 异步锁
        self._lock = asyncio.Lock()
        
        # 告警回调
        self.alert_callback: Optional[Callable] = None
    
    async def acquire(self, timeout: float = 30.0) -> bool:
        """获取请求令牌"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            async with self._lock:
                self._update_tokens()
                
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
            
            await asyncio.sleep(0.1)
        
        await self._log_and_alert("REQUEST_TIMEOUT", f"Failed to acquire token within {timeout}s")
        return False
    
    def _update_tokens(self):
        """更新令牌桶"""
        now = time.time()
        elapsed = now - self.last_update
        self.last_update = now
        
        tokens_to_add = elapsed * self.current_rate
        self.tokens = min(self.current_rate, self.tokens + tokens_to_add)
    
    async def report_response(self, response_time: float, success: bool,
                            status_code: Optional[int] = None,
                            error_message: Optional[str] = None):
        """报告API响应并自动调整"""
        async with self._lock:
            # 记录指标
            metrics = RequestMetrics(
                timestamp=time.time(),
                response_time=response_time,
                success=success,
                status_code=status_code,
                error_message=error_message
            )
            self.metrics_history.append(metrics)
            
            # 更新统计
            self.request_count += 1
            self.total_response_time += response_time
            
            if success:
                self.consecutive_errors = 0
                await self._handle_success(response_time, status_code)
            else:
                self.error_count += 1
                self.consecutive_errors += 1
                await self._handle_error(response_time, status_code, error_message)
            
            # 更新健康状态
            await self._update_health_status()
    
    async def _handle_success(self, response_time: float, status_code: Optional[int]):
        """处理成功响应"""
        if response_time > self.config.timeout_threshold:
            await self._adjust_rate_down("SLOW_RESPONSE", f"Response time {response_time:.2f}s")
        elif status_code == 429:
            await self._adjust_rate_down("RATE_LIMITED", "Received 429 status code")
        elif status_code and status_code >= 500:
            await self._adjust_rate_down("SERVER_ERROR", f"Server error: {status_code}")
        elif self._should_recover():
            await self._adjust_rate_up("GOOD_PERFORMANCE")
    
    async def _handle_error(self, response_time: float, status_code: Optional[int], error_message: Optional[str]):
        """处理错误响应"""
        if self.consecutive_errors >= self.config.error_threshold:
            await self._adjust_rate_down("CONSECUTIVE_ERRORS", f"{self.consecutive_errors} consecutive errors")
            await self._log_and_alert("HIGH_ERROR_RATE", f"Error rate: {self.error_count}/{self.request_count}")
        
        if error_message and "timeout" in error_message.lower():
            await self._adjust_rate_down("TIMEOUT", error_message)
    
    def _should_recover(self) -> bool:
        """判断是否应该恢复速率"""
        if self.request_count < 10:
            return False
            
        avg_response_time = self.total_response_time / self.request_count
        time_since_adjustment = time.time() - self.last_adjustment
        
        return (time_since_adjustment > 300 and
                self.consecutive_errors == 0 and
                avg_response_time < 1.0)
    
    async def _adjust_rate_down(self, reason: str, details: str = ""):
        """降低请求速率"""
        old_rate = self.current_rate
        self.current_rate = max(self.config.min_rate, self.current_rate * self.config.backoff_factor)
        
        if self.current_rate != old_rate:
            self.status = LimitStatus.LIMITED
            self.last_adjustment = time.time()
            
            self.log_service.log_message(self.name, "WARNING", 
                f"Rate limited: {old_rate:.1f}→{self.current_rate:.1f} ({reason}: {details})")
    
    async def _adjust_rate_up(self, reason: str):
        """提升请求速率"""
        old_rate = self.current_rate
        self.current_rate = min(self.config.max_rate, self.current_rate * self.config.recovery_factor)
        
        if self.current_rate != old_rate:
            self.status = LimitStatus.NORMAL
            self.last_adjustment = time.time()
            
            self.log_service.log_message(self.name, "INFO",
                f"Rate recovered: {old_rate:.1f}→{self.current_rate:.1f} ({reason})")
    
    async def _update_health_status(self):
        """更新健康状态"""
        if self.request_count < 10:
            return
            
        error_rate = self.error_count / self.request_count
        avg_response_time = self.total_response_time / self.request_count
        
        old_status = self.health_status
        
        if error_rate >= 0.1 or avg_response_time > 10.0:
            self.health_status = "unhealthy"
        elif error_rate >= 0.05 or avg_response_time > 5.0:
            self.health_status = "degraded"
        else:
            self.health_status = "healthy"
        
        if old_status != self.health_status:
            await self._log_and_alert("STATUS_CHANGE", f"Health: {old_status}→{self.health_status}")
    
    async def _log_and_alert(self, alert_type: str, message: str):
        """统一的日志和告警"""
        self.log_service.log_message(self.name, "WARNING", f"[{alert_type}] {message}")
        
        if self.alert_callback:
            try:
                await self.alert_callback(alert_type, message, self.get_stats())
            except Exception as e:
                self.log_service.log_message(self.name, "ERROR", f"Alert callback failed: {e}")
    
    def set_alert_callback(self, callback: Callable):
        """设置告警回调"""
        self.alert_callback = callback
    
    def get_stats(self) -> Dict:
        """获取当前统计信息"""
        error_rate = self.error_count / max(self.request_count, 1)
        avg_response_time = self.total_response_time / max(self.request_count, 1)
        
        return {
            "name": self.name,
            "current_rate": self.current_rate,
            "status": self.status.value,
            "health_status": self.health_status,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": error_rate,
            "consecutive_errors": self.consecutive_errors,
            "avg_response_time": avg_response_time,
            "tokens": self.tokens
        }
    
    def get_recent_metrics(self, minutes: int = 5) -> list:
        """获取最近N分钟的指标"""
        cutoff = time.time() - (minutes * 60)
        return [m for m in self.metrics_history if m.timestamp > cutoff]
    
    async def reset_stats(self):
        """重置统计信息"""
        async with self._lock:
            self.request_count = 0
            self.error_count = 0
            self.consecutive_errors = 0
            self.total_response_time = 0.0
            self.metrics_history.clear()


class GlobalRateLimiterManager:
    """全局限流器管理器 - 单例模式"""
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls, default_config: RateLimitConfig = None, log_service=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, default_config: RateLimitConfig = None, log_service=None):
        if hasattr(self, "_initialized"):
            return
            
        self.default_config = default_config or RateLimitConfig()
        self.log_service = log_service or get_log_utils()
        self.limiters: Dict[str, RateLimiter] = {}
        self._initialized = True
    
    async def get_limiter(self, endpoint: str) -> RateLimiter:
        """获取指定端点的限流器"""
        async with self._lock:
            if endpoint not in self.limiters:
                self.limiters[endpoint] = RateLimiter(
                    name=f"limiter_{endpoint}",
                    config=self.default_config,
                    log_service=self.log_service
                )
            return self.limiters[endpoint]
    
    async def acquire(self, endpoint: str, timeout: float = 30.0) -> bool:
        """获取指定端点的请求令牌"""
        limiter = await self.get_limiter(endpoint)
        return await limiter.acquire(timeout)
    
    async def report_response(self, endpoint: str, response_time: float, success: bool,
                            status_code: Optional[int] = None, error_message: Optional[str] = None):
        """报告指定端点的响应结果"""
        limiter = await self.get_limiter(endpoint)
        await limiter.report_response(response_time, success, status_code, error_message)
    
    def get_all_stats(self) -> Dict[str, Dict]:
        """获取所有端点的统计信息 - 供前端调用"""
        return {endpoint: limiter.get_stats() for endpoint, limiter in self.limiters.items()}
    
    def get_health_summary(self) -> Dict[str, list]:
        """获取健康状态摘要 - 供前端调用"""
        summary = {"healthy": [], "degraded": [], "unhealthy": []}
        
        for endpoint, limiter in self.limiters.items():
            status = limiter.health_status
            summary[status].append(endpoint)
        
        return summary
    
    def get_endpoint_stats(self, endpoint: str) -> Optional[Dict]:
        """获取指定端点统计 - 供前端调用"""
        if endpoint in self.limiters:
            return self.limiters[endpoint].get_stats()
        return None
    
    def get_system_overview(self) -> Dict:
        """获取系统概览 - 供前端仪表板使用"""
        total_requests = sum(limiter.request_count for limiter in self.limiters.values())
        total_errors = sum(limiter.error_count for limiter in self.limiters.values())
        avg_error_rate = total_errors / max(total_requests, 1)
        
        health_summary = self.get_health_summary()
        
        return {
            "total_endpoints": len(self.limiters),
            "total_requests": total_requests,
            "total_errors": total_errors,
            "system_error_rate": avg_error_rate,
            "healthy_endpoints": len(health_summary["healthy"]),
            "degraded_endpoints": len(health_summary["degraded"]),
            "unhealthy_endpoints": len(health_summary["unhealthy"]),
            "timestamp": time.time()
        }


# 全局单例访问函数
_global_rate_limiter_manager = None

def get_rate_limiter_manager(config: RateLimitConfig = None) -> GlobalRateLimiterManager:
    """获取全局限流器管理器单例"""
    global _global_rate_limiter_manager
    if _global_rate_limiter_manager is None:
        _global_rate_limiter_manager = GlobalRateLimiterManager(config)
    return _global_rate_limiter_manager