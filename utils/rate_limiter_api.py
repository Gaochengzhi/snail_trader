"""
限流器API接口 - 供前端和其他组件获取限流器状态信息
"""

from typing import Dict, Optional, Any
from utils.rate_limiter import get_rate_limiter_manager


class RateLimiterAPI:
    """限流器API接口类 - 提供简洁的访问方法"""
    
    def __init__(self):
        """初始化时获取管理器实例，避免重复获取"""
        self.manager = get_rate_limiter_manager()

    def get_all_endpoints_stats(self) -> Dict[str, Dict]:
        """获取所有端点的统计信息"""
        return self.manager.get_all_stats()

    def get_endpoint_stats(self, endpoint: str) -> Optional[Dict]:
        """获取指定端点的统计信息"""
        return self.manager.get_endpoint_stats(endpoint)

    def get_health_summary(self) -> Dict[str, list]:
        """获取健康状态摘要"""
        return self.manager.get_health_summary()

    def get_system_overview(self) -> Dict[str, Any]:
        """获取系统概览信息"""
        return self.manager.get_system_overview()

    def is_endpoint_healthy(self, endpoint: str) -> bool:
        """检查指定端点是否健康"""
        stats = self.get_endpoint_stats(endpoint)
        if not stats:
            return False
        return stats.get("health_status") == "healthy"

    def get_system_health_score(self) -> float:
        """获取系统整体健康评分 (0.0-1.0)"""
        overview = self.get_system_overview()
        total_endpoints = overview.get("total_endpoints", 0)

        if total_endpoints == 0:
            return 1.0

        healthy_count = overview.get("healthy_endpoints", 0)
        degraded_count = overview.get("degraded_endpoints", 0)
        unhealthy_count = overview.get("unhealthy_endpoints", 0)

        # 健康=1分，降级=0.5分，不健康=0分
        score = (healthy_count * 1.0 + degraded_count * 0.5) / total_endpoints
        return min(1.0, max(0.0, score))


# 全局API实例 - 单例模式
_api_instance = None

def get_rate_limiter_api() -> RateLimiterAPI:
    """获取限流器API实例 - 单例模式"""
    global _api_instance
    if _api_instance is None:
        _api_instance = RateLimiterAPI()
    return _api_instance


# 简化的函数接口，供其他模块直接调用
def get_limiter_stats(endpoint: str = None) -> Dict:
    """获取限流器统计信息
    
    Args:
        endpoint: 端点名称，为None时返回所有端点信息
        
    Returns:
        统计信息字典
    """
    api = get_rate_limiter_api()
    if endpoint:
        return api.get_endpoint_stats(endpoint) or {}
    else:
        return api.get_all_endpoints_stats()


def get_system_health() -> Dict[str, Any]:
    """获取系统健康状态
    
    Returns:
        包含健康摘要和评分的字典
    """
    api = get_rate_limiter_api()
    return {
        "overview": api.get_system_overview(),
        "health_summary": api.get_health_summary(),
        "health_score": api.get_system_health_score()
    }


def is_system_healthy() -> bool:
    """检查系统是否整体健康"""
    api = get_rate_limiter_api()
    health_score = api.get_system_health_score()
    return health_score >= 0.8  # 80%以上认为健康