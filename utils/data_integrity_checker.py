"""
数据完整性检测器

用于检测K线数据的完整性，包括：
1. 时间序列缺口检测
2. 每日记录数完整性检测
3. 异常时间戳检测
4. 数据污染检测
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from utils.data_query import TimeRange


@dataclass
class DataGap:
    """数据缺口信息"""

    start_time: datetime
    end_time: datetime
    gap_type: str  # "missing_records", "time_gap", "incomplete_day"
    expected_count: int
    actual_count: int
    severity: str  # "low", "medium", "high"


@dataclass
class IntegrityReport:
    """数据完整性报告"""

    symbol: str
    interval: str
    time_range: TimeRange
    total_records: int
    expected_records: int
    completeness_ratio: float
    gaps: List[DataGap]
    is_healthy: bool
    needs_repair: bool
    repair_ranges: List[TimeRange]


class DataIntegrityChecker:
    """数据完整性检测器"""

    # 各种间隔每天应有的记录数
    RECORDS_PER_DAY = {
        "1m": 1440,  # 24 * 60
        "5m": 288,  # 24 * 12
        "15m": 96,  # 24 * 4
        "1h": 24,  # 24 * 1
        "4h": 6,  # 24 / 4
        "1d": 1,  # 1
    }

    def __init__(self):
        pass

    def get_interval_minutes(self, interval: str) -> int:
        """获取间隔的分钟数"""
        interval_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
        }
        return interval_map.get(interval, 15)  # 默认15分钟

    def get_expected_records_per_day(self, interval: str) -> int:
        """获取每天预期的记录数"""
        return self.RECORDS_PER_DAY.get(interval, 96)  # 默认15分钟间隔

    def analyze_data_completeness(
        self,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        actual_data: List[Dict[str, Any]],
        current_time: Optional[datetime] = None,
    ) -> IntegrityReport:
        """
        分析数据完整性

        Args:
            symbol: 交易对
            interval: K线间隔
            time_range: 查询时间范围
            actual_data: 实际获取的数据
            current_time: 当前时间 (用于判断哪些数据应该存在)

        Returns:
            完整性报告
        """
        # 如果没有提供当前时间，使用系统当前时间
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        # 调整时间范围，不应该超过当前时间
        effective_end_time = min(time_range.end, current_time)

        # 如果查询范围完全在未来，则没有预期数据
        if time_range.start >= current_time:
            return IntegrityReport(
                symbol=symbol,
                interval=interval,
                time_range=time_range,
                total_records=0,
                expected_records=0,
                completeness_ratio=1.0,  # 未来时间，完整性为100%
                gaps=[],
                is_healthy=True,
                needs_repair=False,
                repair_ranges=[],
            )

        effective_time_range = TimeRange(time_range.start, effective_end_time)

        # 计算预期记录数（基于有效时间范围）
        expected_records = self._calculate_expected_records(
            effective_time_range, interval
        )
        actual_records = len(actual_data)
        completeness_ratio = actual_records / max(expected_records, 1)

        # 检测数据缺口（传入当前时间和有效时间范围）
        gaps = self._detect_data_gaps(
            symbol, interval, effective_time_range, actual_data, current_time
        )

        # 生成修复范围
        repair_ranges = self._generate_repair_ranges(gaps)

        # 判断数据健康状态
        is_healthy = completeness_ratio >= 0.999 and len(gaps) == 0
        needs_repair = completeness_ratio < 0.99 or any(
            gap.severity == "high" for gap in gaps
        )

        return IntegrityReport(
            symbol=symbol,
            interval=interval,
            time_range=effective_time_range,  # 使用有效时间范围
            total_records=actual_records,
            expected_records=expected_records,
            completeness_ratio=completeness_ratio,
            gaps=gaps,
            is_healthy=is_healthy,
            needs_repair=needs_repair,
            repair_ranges=repair_ranges,
        )

    def _calculate_expected_records(self, time_range: TimeRange, interval: str) -> int:
        """计算预期的记录数"""
        interval_minutes = self.get_interval_minutes(interval)
        total_minutes = int((time_range.end - time_range.start).total_seconds() / 60)
        return total_minutes // interval_minutes

    def _detect_data_gaps(
        self,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        actual_data: List[Dict[str, Any]],
        current_time: datetime,
    ) -> List[DataGap]:
        """检测数据缺口 - 基于实际时间的精确检测"""
        if not actual_data:
            # 完全没有数据，但只计算到当前时间
            expected_count = self._calculate_expected_records(time_range, interval)
            if expected_count > 0:
                return [
                    DataGap(
                        start_time=time_range.start,
                        end_time=time_range.end,
                        gap_type="missing_records",
                        expected_count=expected_count,
                        actual_count=0,
                        severity="high",
                    )
                ]
            return []

        # 核心算法：生成完整的预期时间序列，然后与实际数据对比
        gaps = self._detect_precise_missing_records(
            time_range, actual_data, interval, current_time
        )

        return gaps

    def _detect_precise_missing_records(
        self,
        time_range: TimeRange,
        actual_data: List[Dict[str, Any]],
        interval: str,
        current_time: datetime,
    ) -> List[DataGap]:
        """精确检测缺失的记录 - 生成完整时间序列进行对比"""
        gaps = []
        interval_minutes = self.get_interval_minutes(interval)

        # 1. 生成预期的完整时间序列（只到当前时间）
        expected_timestamps = self._generate_expected_timestamps(
            time_range.start, time_range.end, interval_minutes, current_time
        )

        if not expected_timestamps:
            return gaps

        # 2. 提取实际数据的时间戳
        actual_timestamps = set()
        for record in actual_data:
            # 使用open_time作为记录的标识时间戳
            timestamp = datetime.fromtimestamp(
                record["open_time"] / 1000, tz=timezone.utc
            )
            # 对齐到间隔边界（避免时间戳微小差异导致的误判）
            aligned_timestamp = self._align_to_interval_boundary(
                timestamp, interval_minutes
            )
            actual_timestamps.add(aligned_timestamp)

        # 3. 找出缺失的时间戳
        missing_timestamps = []
        for expected_ts in expected_timestamps:
            if expected_ts not in actual_timestamps:
                missing_timestamps.append(expected_ts)

        # 4. 将连续的缺失时间戳合并为缺口
        if missing_timestamps:
            gaps.extend(
                self._group_missing_timestamps_to_gaps(
                    missing_timestamps, interval_minutes
                )
            )

        return gaps

    def _generate_expected_timestamps(
        self,
        start_time: datetime,
        end_time: datetime,
        interval_minutes: int,
        current_time: datetime,
    ) -> List[datetime]:
        """生成预期的时间戳序列（只到当前时间）"""
        timestamps = []

        # 将开始时间对齐到间隔边界
        current = self._align_to_interval_boundary(start_time, interval_minutes)

        # 确保不超过当前时间
        effective_end = min(end_time, current_time)

        # 生成所有应该存在的时间戳
        while current < effective_end:
            timestamps.append(current)
            current += timedelta(minutes=interval_minutes)

        return timestamps

    def _align_to_interval_boundary(
        self, timestamp: datetime, interval_minutes: int
    ) -> datetime:
        """将时间戳对齐到间隔边界 - 支持分钟级别检测"""
        if interval_minutes >= 60:
            # 对于小时及以上的间隔，对齐到整点
            if interval_minutes == 60:  # 1小时
                return timestamp.replace(minute=0, second=0, microsecond=0)
            elif interval_minutes == 240:  # 4小时
                aligned_hour = (timestamp.hour // 4) * 4
                return timestamp.replace(
                    hour=aligned_hour, minute=0, second=0, microsecond=0
                )
            elif interval_minutes == 1440:  # 1天
                return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # 对于分钟级别的间隔，对齐到间隔边界
            aligned_minute = (timestamp.minute // interval_minutes) * interval_minutes
            return timestamp.replace(minute=aligned_minute, second=0, microsecond=0)

        # 默认情况：清除秒和微秒
        return timestamp.replace(second=0, microsecond=0)

    def _group_missing_timestamps_to_gaps(
        self, missing_timestamps: List[datetime], interval_minutes: int
    ) -> List[DataGap]:
        """将缺失的时间戳分组为连续的缺口"""
        if not missing_timestamps:
            return []

        gaps = []
        missing_timestamps.sort()

        # 分组连续的时间戳
        current_group_start = missing_timestamps[0]
        current_group_timestamps = [missing_timestamps[0]]

        for i in range(1, len(missing_timestamps)):
            expected_next = current_group_timestamps[-1] + timedelta(
                minutes=interval_minutes
            )

            if missing_timestamps[i] == expected_next:
                # 连续缺失
                current_group_timestamps.append(missing_timestamps[i])
            else:
                # 不连续，结束当前组，开始新组
                gaps.append(
                    self._create_gap_from_timestamps(
                        current_group_start, current_group_timestamps, interval_minutes
                    )
                )
                current_group_start = missing_timestamps[i]
                current_group_timestamps = [missing_timestamps[i]]

        # 处理最后一组
        if current_group_timestamps:
            gaps.append(
                self._create_gap_from_timestamps(
                    current_group_start, current_group_timestamps, interval_minutes
                )
            )

        return gaps

    def _create_gap_from_timestamps(
        self,
        start_timestamp: datetime,
        missing_timestamps: List[datetime],
        interval_minutes: int,
    ) -> DataGap:
        """从缺失时间戳列表创建数据缺口"""
        end_timestamp = (
            missing_timestamps[-1]
            + timedelta(minutes=interval_minutes)
            - timedelta(microseconds=1)
        )
        missing_count = len(missing_timestamps)

        # 根据缺失数量确定严重性
        if missing_count >= 10:
            severity = "high"
        elif missing_count >= 3:
            severity = "medium"
        else:
            severity = "low"

        return DataGap(
            start_time=start_timestamp,
            end_time=end_timestamp,
            gap_type="precise_missing_records",
            expected_count=missing_count,
            actual_count=0,
            severity=severity,
        )

    def _detect_time_series_gaps(
        self, sorted_data: List[Dict], interval_minutes: int
    ) -> List[DataGap]:
        """检测时间序列中的缺口"""
        gaps = []
        interval_ms = interval_minutes * 60 * 1000

        for i in range(len(sorted_data) - 1):
            current_close = sorted_data[i]["close_time"]
            next_open = sorted_data[i + 1]["open_time"]

            # 计算时间差
            time_diff = next_open - current_close

            # 如果时间差超过一个间隔（考虑1ms的误差），则存在缺口
            if time_diff > interval_ms + 1000:  # 允许1秒误差
                missing_periods = int(time_diff / interval_ms) - 1

                if missing_periods > 0:
                    gap_start = datetime.fromtimestamp(
                        (current_close + 1) / 1000, tz=timezone.utc
                    )
                    gap_end = datetime.fromtimestamp(
                        (next_open - 1) / 1000, tz=timezone.utc
                    )

                    severity = (
                        "high"
                        if missing_periods > 10
                        else ("medium" if missing_periods > 3 else "low")
                    )

                    gaps.append(
                        DataGap(
                            start_time=gap_start,
                            end_time=gap_end,
                            gap_type="time_gap",
                            expected_count=missing_periods,
                            actual_count=0,
                            severity=severity,
                        )
                    )

        return gaps

    def _detect_incomplete_days(
        self, sorted_data: List[Dict], expected_per_day: int
    ) -> List[DataGap]:
        """检测每日记录数不完整的情况"""
        gaps = []

        # 按日期分组数据
        daily_counts = {}
        for record in sorted_data:
            date_key = datetime.fromtimestamp(
                record["open_time"] / 1000, tz=timezone.utc
            ).date()
            daily_counts[date_key] = daily_counts.get(date_key, 0) + 1

        # 检查每一天的记录数
        for date, count in daily_counts.items():
            if count < expected_per_day * 0.9:  # 如果少于预期的90%
                missing_count = expected_per_day - count
                severity = (
                    "high" if missing_count > expected_per_day * 0.3 else "medium"
                )

                day_start = datetime.combine(date, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )
                day_end = day_start + timedelta(days=1) - timedelta(microseconds=1)

                gaps.append(
                    DataGap(
                        start_time=day_start,
                        end_time=day_end,
                        gap_type="incomplete_day",
                        expected_count=expected_per_day,
                        actual_count=count,
                        severity=severity,
                    )
                )

        return gaps

    def _detect_boundary_gaps(
        self, time_range: TimeRange, sorted_data: List[Dict], interval_minutes: int
    ) -> List[DataGap]:
        """检测查询范围边界的数据缺失"""
        gaps = []

        if not sorted_data:
            return gaps

        first_record_time = datetime.fromtimestamp(
            sorted_data[0]["open_time"] / 1000, tz=timezone.utc
        )
        last_record_time = datetime.fromtimestamp(
            sorted_data[-1]["close_time"] / 1000, tz=timezone.utc
        )

        # 检测开始边界缺失
        if first_record_time > time_range.start:
            missing_duration = (
                first_record_time - time_range.start
            ).total_seconds() / 60
            missing_count = int(missing_duration / interval_minutes)

            if missing_count > 0:
                gaps.append(
                    DataGap(
                        start_time=time_range.start,
                        end_time=first_record_time - timedelta(microseconds=1),
                        gap_type="missing_records",
                        expected_count=missing_count,
                        actual_count=0,
                        severity="medium",
                    )
                )

        # 检测结束边界缺失
        if last_record_time < time_range.end:
            missing_duration = (time_range.end - last_record_time).total_seconds() / 60
            missing_count = int(missing_duration / interval_minutes)

            if missing_count > 0:
                gaps.append(
                    DataGap(
                        start_time=last_record_time + timedelta(microseconds=1),
                        end_time=time_range.end,
                        gap_type="missing_records",
                        expected_count=missing_count,
                        actual_count=0,
                        severity="medium",
                    )
                )

        return gaps

    def _generate_repair_ranges(self, gaps: List[DataGap]) -> List[TimeRange]:
        """基于检测到的缺口生成需要修复的时间范围"""
        if not gaps:
            return []

        # 将相邻的缺口合并为连续的修复范围
        repair_ranges = []
        sorted_gaps = sorted(gaps, key=lambda g: g.start_time)

        current_start = sorted_gaps[0].start_time
        current_end = sorted_gaps[0].end_time

        for gap in sorted_gaps[1:]:
            # 如果缺口之间的间隔小于1小时，则合并
            if gap.start_time - current_end <= timedelta(hours=1):
                current_end = max(current_end, gap.end_time)
            else:
                # 保存当前范围，开始新范围
                repair_ranges.append(TimeRange(current_start, current_end))
                current_start = gap.start_time
                current_end = gap.end_time

        # 添加最后一个范围
        repair_ranges.append(TimeRange(current_start, current_end))

        return repair_ranges


# 全局实例
integrity_checker = DataIntegrityChecker()
