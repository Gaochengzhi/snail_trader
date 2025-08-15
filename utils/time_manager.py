"""
时间管理器 - 支持回测和实时模式的虚拟时间系统
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from enum import Enum


class TimeMode(Enum):
    BACKTEST = "backtest"  # 回测模式，快速向前跑
    LIVE = "live"         # 实时模式，按实际时间运行


class TimeManager:
    """时间管理器 - Linus式简洁设计"""
    
    def __init__(self, run_from_datetime: Optional[datetime] = None, mode: TimeMode = TimeMode.LIVE, backtest_end_time: Optional[datetime] = None):
        self.mode = mode
        self.virtual_time = run_from_datetime or datetime.now()
        self.real_start_time = datetime.now()
        self.is_running = False
        
        # 记录回测开始时间（用于进度计算）
        self._start_time = self.virtual_time
        
        # 回测终止时间处理
        if mode == TimeMode.BACKTEST:
            if backtest_end_time is not None:
                # 用户明确指定了结束时间
                self.backtest_end_time = backtest_end_time
                print(f"📅 回测时间范围: {self.virtual_time} → {self.backtest_end_time}")
            else:
                # 没有指定结束时间：默认运行到当前时间
                self.backtest_end_time = datetime.now()
                print(f"⚠️  未指定回测结束时间，默认运行到当前时间: {self.backtest_end_time}")
        else:
            self.backtest_end_time = None
        
    def now(self) -> datetime:
        """获取当前虚拟时间"""
        if self.mode == TimeMode.LIVE:
            return datetime.now()
        return self.virtual_time
    
    def advance_time(self, interval_hours: float = 4.0):
        """推进虚拟时间（仅回测模式有效）"""
        if self.mode == TimeMode.BACKTEST:
            self.virtual_time += timedelta(hours=interval_hours)
            
    def is_backtest(self) -> bool:
        return self.mode == TimeMode.BACKTEST
        
    def is_live(self) -> bool:
        return self.mode == TimeMode.LIVE
        
    def time_until_now(self) -> timedelta:
        """距离真实现在还有多久（回测用）"""
        if self.mode == TimeMode.LIVE:
            return timedelta(0)
        return datetime.now() - self.virtual_time
        
    def should_continue(self) -> bool:
        """是否应该继续运行"""
        if self.mode == TimeMode.LIVE:
            return True
        
        # 回测模式：必须有明确的结束条件
        if self.mode == TimeMode.BACKTEST:
            if self.backtest_end_time is None:
                print(f"❌ 回测模式必须设定结束时间!")
                return False
                
            # 检查是否到达用户指定的结束时间
            if self.virtual_time >= self.backtest_end_time:
                print(f"⏹️  回测正常结束:")
                print(f"   虚拟时间: {self.virtual_time}")
                print(f"   结束时间: {self.backtest_end_time}")
                return False
                
            # 显示进度
            if self.virtual_time.hour % 24 == 0 and self.virtual_time.minute == 0:  # 每天显示一次进度
                progress = (self.virtual_time - self.get_start_time()).total_seconds() / (self.backtest_end_time - self.get_start_time()).total_seconds()
                print(f"📊 回测进度: {progress*100:.1f}% | 当前: {self.virtual_time} | 目标: {self.backtest_end_time}")
                
        return True
    
    def get_start_time(self) -> datetime:
        """获取回测开始时间（用于进度计算）"""
        # 这里需要记住初始的virtual_time，简单实现就用类属性
        if not hasattr(self, '_start_time'):
            self._start_time = self.virtual_time
        return self._start_time


# 全局时间管理器
_global_time_manager: Optional[TimeManager] = None

def get_time_manager() -> TimeManager:
    """获取全局时间管理器"""
    global _global_time_manager
    if _global_time_manager is None:
        _global_time_manager = TimeManager()
    return _global_time_manager

def init_time_manager(run_from_datetime: Optional[datetime] = None, mode: TimeMode = TimeMode.LIVE, backtest_end_time: Optional[datetime] = None):
    """初始化全局时间管理器"""
    global _global_time_manager
    _global_time_manager = TimeManager(run_from_datetime, mode, backtest_end_time)
    return _global_time_manager