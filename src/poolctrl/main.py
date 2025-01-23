import atexit
import hashlib
import json
import os
import random
import time
from collections import deque
from pathlib import Path
from threading import Condition, Lock
from typing import Any, Dict, List, Optional


class MultiSingletonMeta(type):
    """
    多例单例(多实例单例)的元类：给定一个 key，不同的 key 拥有不同的单例实例。
    """

    _instances = {}  # (cls, key) -> instance

    def __call__(cls, *args, **kwargs):
        task_id = kwargs.get("task_id", None)
        if task_id is None:
            task_id = "default"

        dict_key = (cls, task_id)
        if dict_key not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[dict_key] = instance

        return cls._instances[dict_key]


class RateLimitRule:
    """
    表示一个限速规则：在 timeframe_s 秒的滚动窗口内，最多允许 max_requests 次请求。
    用户可以通过传入 (interval, time_unit) 的方式，在内部自动转换为秒。
    例如：
        - RateLimitRule(5, interval=1, time_unit="second")  -> 1秒内最多5次
        - RateLimitRule(15, interval=1, time_unit="minute") -> 1分钟内最多15次
        - RateLimitRule(100, interval=1, time_unit="hour")  -> 1小时内最多100次
        - RateLimitRule(5000, interval=1, time_unit="day")  -> 1天内最多5000次
        - RateLimitRule(60000, interval=1, time_unit="month")-> 约1个月(30天)内最多60000次
    """

    _TIME_UNIT_MAP = {
        "second": 1,
        "seconds": 1,
        "sec": 1,
        "minute": 60,
        "minutes": 60,
        "min": 60,
        "hour": 3600,
        "hours": 3600,
        "day": 86400,
        "days": 86400,
        "d": 86400,
        "month": 2592000,  # 30天近似值
        "months": 2592000,
        "mth": 2592000,
        "year": 31536000,  # 365天近似值
        "years": 31536000,
        "y": 31536000,
    }

    def __init__(
        self,
        max_requests: int,
        interval: int | float = 1,
        time_unit: str = "second",
    ):
        """
        :param max_requests: 在给定( interval, time_unit )时间内允许的最大请求次数
        :param interval: 时间跨度的数量 (默认=1)
        :param time_unit: 时间单位，可选：second、minute、hour、day、month、year 等
        """
        time_unit_lower = time_unit.lower().strip()
        if time_unit_lower not in self._TIME_UNIT_MAP:
            raise ValueError(f"不支持的 time_unit: {time_unit}")

        self.timeframe_s = int(interval * self._TIME_UNIT_MAP[time_unit_lower])
        self.max_requests = max_requests

    def __repr__(self):
        return f"<RateLimitRule {self.timeframe_s}s/{self.max_requests}req>"


class Pool(metaclass=MultiSingletonMeta):
    def __init__(
        self,
        limits: List[RateLimitRule] = [],
        allow_concurrent: bool = False,
        cooldown_time: int = 60,
        task_id: str = None,
        persist: bool = False,
    ):
        """
        :param limits: 一组限速规则，例如：
               [RateLimitRule(5, 1, "second"), RateLimitRule(15, 1, "minute")]
        :param allow_concurrent: 是否允许同一密钥在同一时间段内被并发使用
        :param cooldown_time: 普通冷却时长（秒）
        :param usage_path: 若需持久化记录（尤其是大时间窗口），可指定保存使用记录的文件路径
        :param persist: 是否启用持久化
        """
        if not task_id:
            raise ValueError("task_id is required")

        self.task_id = task_id
        self.limits = limits
        self.allow_concurrent = allow_concurrent
        self.cooldown_time = cooldown_time
        self.persist = persist

        # 每个密钥对应的【时间戳队列】，保存最近的请求时间（秒级）
        self.request_timestamps: Dict[str, deque] = {}

        # 冷却中的密钥，值为冷却结束的时间戳
        self.cooldown_keys: Dict[str, float] = {}
        # 连续冷却计数
        self.consecutive_cooldown_counts: Dict[str, int] = {}

        # 当前被占用的密钥（如果不允许并发，则同一时间只允许一个线程占用某个特定密钥）
        self.occupied_keys: set = set()

        self._lock = Lock()
        self._condition = Condition(self._lock)

        # 用于持久化 request_timestamps
        self.usage_path: Optional[Path] = Path(f"persist/{self.task_id}.usage.json")
        if self.persist and self.usage_path is not None:
            self.load_usage()
            atexit.register(self.save_usage)

    def _hash_key(self, key: Any) -> str:
        """生成密钥的哈希表示，用于内部管理"""
        return hashlib.sha256(str(key).encode("utf-8")).hexdigest()

    def load_usage(self):
        """从磁盘加载历史请求的时间戳队列（仅当 persist=True 且 usage_path 存在时）"""
        if self.usage_path and self.usage_path.exists():
            with open(self.usage_path, "r") as f:
                data: Dict = json.load(f)
            self.request_timestamps.clear()
            for k, ts_list in data.items():
                dq = deque(ts_list)
                self.request_timestamps[k] = dq

    def save_usage(self):
        """将当前所有密钥的请求时间戳写入文件"""
        if not self.usage_path:
            return
        os.makedirs(self.usage_path.parent, exist_ok=True)
        data = {}
        for k, dq in self.request_timestamps.items():
            data[k] = list(dq)
        with open(self.usage_path, "w") as f:
            json.dump(data, f, indent=2)

    def _clean_old_requests(self, internal_key: str, current_time: float):
        """
        根据“最大时间窗口”来统一清理过期的请求。
        """
        if internal_key not in self.request_timestamps:
            self.request_timestamps[internal_key] = deque()

        max_timeframe = max(rule.timeframe_s for rule in self.limits)
        dq = self.request_timestamps[internal_key]

        cutoff = current_time - max_timeframe
        while dq and dq[0] < cutoff:
            dq.popleft()

    def _is_key_available(self, internal_key: str, current_time: float) -> bool:
        """
        检查key在当前时刻是否可用：
        1. 如果仍在 cooldown 中，则不可用
        2. 如果 cooldown 过期则恢复可用
        3. 检查所有限速规则：只要有一个规则超出上限，就不可用
        4. 如果不允许并发且该 key 已被占用，则不可用
        """
        # 先判断 cooldown
        if internal_key in self.cooldown_keys:
            if current_time < self.cooldown_keys[internal_key]:
                return False
            else:
                del self.cooldown_keys[internal_key]

        # 并发限制
        if not self.allow_concurrent and internal_key in self.occupied_keys:
            return False

        # 清理超时的旧请求
        self._clean_old_requests(internal_key, current_time)
        dq = self.request_timestamps[internal_key]

        # 检查所有限速规则
        for rule in self.limits:
            cutoff = current_time - rule.timeframe_s
            # recent_count = 在[cutoff, current_time]窗口内的请求数量
            recent_count = 0
            for t in reversed(dq):
                if t >= cutoff:
                    recent_count += 1
                else:
                    break
            if recent_count >= rule.max_requests:
                return False

        return True

    def _get_wait_time_for_key(self, internal_key: str, current_time: float) -> float:
        """
        计算下一个该密钥可能变为可用状态的等待时间（秒）。
        如果无法可用（比如一直在cooldown），则返回一个较大值。
        """
        wait_time = 0.0

        # 如果在冷却中
        if internal_key in self.cooldown_keys:
            cd_end = self.cooldown_keys[internal_key]
            if current_time < cd_end:
                wait_time = max(wait_time, cd_end - current_time)

        # 对所有限速规则计算等待时间，取最大值
        dq = self.request_timestamps.get(internal_key, deque())
        for rule in self.limits:
            cutoff = current_time - rule.timeframe_s
            recent_requests = [t for t in dq if t >= cutoff]
            if len(recent_requests) >= rule.max_requests:
                # 需要等到最早那次请求 + rule.timeframe_s
                earliest_in_window = recent_requests[0]
                rule_end = earliest_in_window + rule.timeframe_s
                rule_wait = rule_end - current_time
                wait_time = max(wait_time, rule_wait)

        # 并发限制下，如果被占用，也许只能等它被释放（无法准确计算，只能等 notify）
        if not self.allow_concurrent and internal_key in self.occupied_keys:
            wait_time = max(wait_time, 0)

        return wait_time

    def mark_key_used(self, key: Any):
        """标记密钥被使用一次，并添加请求时间戳"""
        internal_key = self._hash_key(key)
        with self._lock:
            current_time = time.time()
            self._clean_old_requests(internal_key, current_time)
            self.request_timestamps[internal_key].append(current_time)

            if not self.allow_concurrent:
                self.occupied_keys.add(internal_key)

            self._condition.notify_all()

    def release_key(self, key: Any):
        """释放密钥占用"""
        internal_key = self._hash_key(key)
        with self._lock:
            if internal_key in self.occupied_keys:
                self.occupied_keys.remove(internal_key)
            self._condition.notify_all()

    def mark_key_cooldown(self, key: Any):
        """将密钥标记为进入冷却状态，如果连续3次进入长时冷却"""
        internal_key = self._hash_key(key)
        with self._lock:
            current_time = time.time()
            self.consecutive_cooldown_counts[internal_key] = (
                self.consecutive_cooldown_counts.get(internal_key, 0) + 1
            )

            if self.consecutive_cooldown_counts[internal_key] >= 3:
                self.cooldown_keys[internal_key] = current_time + 3600  # 1小时冷却
            else:
                self.cooldown_keys[internal_key] = current_time + self.cooldown_time

            if internal_key in self.occupied_keys:
                self.occupied_keys.remove(internal_key)
            self._condition.notify_all()

    def get_available_key(self, keys: List[Any]) -> Any:
        """获取一个可用的密钥，如果没有可用则阻塞等待"""
        if not keys:
            raise ValueError("未提供任何 API 密钥")

        with self._lock:
            while True:
                current_time = time.time()
                shuffled_keys = keys.copy()
                random.shuffle(shuffled_keys)

                available_keys = []
                for key in shuffled_keys:
                    ik = self._hash_key(key)
                    if self._is_key_available(ik, current_time):
                        available_keys.append(key)

                if available_keys:
                    for k in available_keys:
                        ik = self._hash_key(k)
                        if self.allow_concurrent or ik not in self.occupied_keys:
                            self._clean_old_requests(ik, current_time)
                            if not self.allow_concurrent:
                                self.occupied_keys.add(ik)
                            return k

                # 如果没有可用的 key，计算最小等待时间
                min_wait_time = float("inf")
                for key in keys:
                    ik = self._hash_key(key)
                    wait_time = self._get_wait_time_for_key(ik, current_time)
                    if wait_time < min_wait_time:
                        min_wait_time = wait_time

                if min_wait_time == float("inf"):
                    raise RuntimeError("无法确定密钥的等待时间，可能没有可用密钥。")

                wait_time = min_wait_time if min_wait_time > 0 else None
                self._condition.wait(timeout=wait_time)

    def context(self, keys: List[Any]):
        """
        上下文管理器，用于自动释放密钥。例如：
            with pool.context(keys) as key:
                # 使用key进行请求
        """

        class KeyContext:
            def __init__(self, manager: Pool, key: Any):
                self.manager = manager
                self.key = key
                self.entered = False

            def __enter__(self):
                self.manager.mark_key_used(self.key)
                self.entered = True
                return self.key

            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.entered:
                    # 如果没有异常，说明key使用正常，重置连续冷却计数
                    if exc_type is None:
                        ik = self.manager._hash_key(self.key)
                        self.manager.consecutive_cooldown_counts[ik] = 0
                    self.manager.release_key(self.key)

        key = self.get_available_key(keys)
        return KeyContext(self, key)
