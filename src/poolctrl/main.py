import atexit
import hashlib
import json
import os
import random
import time
from collections import deque
from datetime import date, datetime, timezone
from datetime import time as dtime
from pathlib import Path
from threading import Condition, Lock
from typing import Any, Dict, List, Optional


class MultiSingletonMeta(type):
    """
    多例单例(多实例单例)的元类：给定一个 key，不同的 key 拥有不同的单例实例。
    """

    _instances = {}  # (cls, key) -> instance

    def __call__(cls, *args, **kwargs):
        # 例如从 kwargs 中取一个 'task_id' 作为区分
        task_id = kwargs.get("task_id", None)
        if task_id is None:
            # 如果没有传入 task_id，可以决定返回一个默认key的单例
            task_id = "default"

        # 确定字典的键
        dict_key = (cls, task_id)

        # 如果没有对应实例，则创建并存储
        if dict_key not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[dict_key] = instance

        # 返回已经存在的实例
        return cls._instances[dict_key]


# ========== KeyManager Class ==========
class Pool(metaclass=MultiSingletonMeta):
    def __init__(
        self,
        rpm: int = 15,
        rpd: Optional[int] = None,
        allow_concurrent: bool = False,
        cooldown_time: int = 60,
        task_id: str = None,
    ):
        """
        :param rpm: 每分钟每个密钥可用的请求次数上限
        :param allow_concurrent: 是否允许同一密钥在同一时间段内被并发使用
        :param cooldown_time: 密钥进入冷却状态的持续时间（秒）
        """
        if not task_id:
            raise ValueError("task_id is required")
        self.task_id = task_id
        self.rpm = rpm
        self.rpd = rpd
        self.allow_concurrent = allow_concurrent
        self.cooldown_time = cooldown_time

        # 每个密钥对应的请求时间戳队列，用于计算 RPM 限制
        self.request_counts: Dict[str, deque] = {}

        # 冷却中的密钥，值为冷却结束的时间戳
        self.cooldown_keys: Dict[str, float] = {}
        # 连续冷却计数
        self.consecutive_cooldown_counts: Dict[str, int] = {}

        # 当前被占用的密钥（如果不允许并发，则同一时间只允许一个线程占用某个特定密钥）
        self.occupied_keys: set = set()

        self._lock = Lock()
        self._condition = Condition(self._lock)

        # 每个 key 的当日使用计数 key_hash -> int
        self.daily_usage_counts: Dict[str, int] = {"which_day": str(date.today())}
        self.usage_path: Path = Path(f"cache/{self.task_id}_daily_usage_counts.json")
        # 用来判断是否到了新的一天，需要重置计数
        self._day_tag: Optional[date] = None

        # 从缓存中加载当日使用计数
        if rpd:
            self.load_usage()

            atexit.register(self.save_usage)

    def _reset_daily_usage_if_needed(self):
        """
        如果今天已变更（跨日），就重置所有 key 的当日使用计数
        """
        today = datetime.now(timezone.utc).date().isoformat()
        if self._day_tag != today:
            # 新的一天，重置计数
            self.daily_usage_counts = {"which_day": today}
            self._day_tag = today
            self.save_usage()

    def save_usage(self):
        """
        只在程序退出（atexit 回调）或者需要强制保存时调用，
        将 self.daily_usage_counts 写入文件。
        """
        os.makedirs(self.usage_path.parent, exist_ok=True)
        with open(self.usage_path, "w") as f:
            json.dump(self.daily_usage_counts, f, indent=2)

    def load_usage(self):
        if self.usage_path.exists():
            with open(self.usage_path, "r") as f:
                self.daily_usage_counts = json.load(f)
                self._day_tag = self.daily_usage_counts.get("which_day", "")
            self._reset_daily_usage_if_needed()

    def _hash_key(self, key: Any) -> str:
        """生成密钥的哈希表示，用于内部管理"""
        return hashlib.sha256(str(key).encode("utf-8")).hexdigest()

    def _clean_old_requests(self, internal_key: str, current_time: float):
        """移除超过60秒之前的请求记录以适应 RPM 限制"""
        if internal_key not in self.request_counts:
            self.request_counts[internal_key] = deque()
        rq = self.request_counts[internal_key]
        while rq and rq[0] <= current_time - 60:
            rq.popleft()

    def _is_key_available(self, internal_key: str, current_time: float) -> bool:
        """
        检查key在当前时刻是否可用：
        - 若仍在cooldown或ban中则不可用
        - 若cooldown/ban过期则恢复可用
          * 若是ban过期，则重置计数器为0
          * 若只是普通cooldown过期，不重置计数器（继续累积）
        """
        if internal_key in self.cooldown_keys:
            if current_time < self.cooldown_keys[internal_key]:
                # 仍在cooldown/ban中
                return False
            else:
                del self.cooldown_keys[internal_key]
        daily_used = self.daily_usage_counts.get(internal_key, 0)
        if self.rpd and daily_used >= self.rpd:
            # 当日使用数已达上限
            return False
        # 检查RPM限制
        self._clean_old_requests(internal_key, current_time)
        if len(self.request_counts[internal_key]) >= self.rpm:
            return False

        # 检查并发占用
        if not self.allow_concurrent and internal_key in self.occupied_keys:
            return False

        return True

    def _get_wait_time_for_key(self, internal_key: str, current_time: float) -> float:
        """
        计算下一个该密钥可能变为可用状态的等待时间（秒）。
        如不可计算或需要等待很久，则返回相对时间。可能为0表示无需等待。
        """
        wait_time = 0.0

        # 如果在冷却中
        if (
            internal_key in self.cooldown_keys
            and current_time < self.cooldown_keys[internal_key]
        ):
            wait_time = max(wait_time, self.cooldown_keys[internal_key] - current_time)

        # 如果达到了 RPM 限制，需要等待最早一次请求时间戳满60秒后再重试
        if (
            internal_key in self.request_counts
            and len(self.request_counts[internal_key]) >= self.rpm
        ):
            oldest_request = self.request_counts[internal_key][0]
            rpm_wait = (oldest_request + 60) - current_time
            wait_time = max(wait_time, rpm_wait)
        daily_used = self.daily_usage_counts.get(internal_key, 0)
        if self.rpd and daily_used >= self.rpd:
            # 计算到今日结束的剩余秒数
            now = datetime.now(timezone.utc)
            end_of_day = datetime.combine(now.date(), dtime.max)
            wait_time = max(wait_time, (end_of_day - now).total_seconds() + 60)

        # 并发限制下，如果密钥被占用，也许只能等它被释放
        if not self.allow_concurrent and internal_key in self.occupied_keys:
            # 这里无法精确计算等待时间，只能设置为0，等待条件变量通知
            wait_time = max(wait_time, 0)

        return wait_time

    def mark_key_used(self, key: Any):
        """标记密钥被使用一次，并添加请求时间戳"""
        internal_key = self._hash_key(key)
        with self._lock:
            current_time = time.time()
            self._clean_old_requests(internal_key, current_time)
            self.request_counts[internal_key].append(current_time)

            current_count = self.daily_usage_counts.get(internal_key, 0)
            self.daily_usage_counts[internal_key] = current_count + 1
            self.save_usage() if self.rpd else None

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
        """获取一个可用的密钥，如果没有可用的则阻塞等待"""
        if not keys:
            raise ValueError("未提供任何 API 密钥")

        with self._lock:
            self._reset_daily_usage_if_needed()
            while True:
                current_time = time.time()
                shuffled_keys = keys.copy()
                random.shuffle(shuffled_keys)

                available_keys = []
                for key in shuffled_keys:
                    internal_key = self._hash_key(key)
                    if self._is_key_available(internal_key, current_time):
                        available_keys.append(key)

                if available_keys:
                    # 优先选择未被占用的密钥
                    for key in available_keys:
                        internal_key = self._hash_key(key)
                        if (
                            self.allow_concurrent
                            or internal_key not in self.occupied_keys
                        ):
                            self._clean_old_requests(internal_key, current_time)
                            if not self.allow_concurrent:
                                self.occupied_keys.add(internal_key)
                            return key

                # 判断是否有密钥仅被占用但未冷却
                occupied_only = [
                    key
                    for key in keys
                    if self._hash_key(key) in self.occupied_keys
                    and self._hash_key(key) not in self.cooldown_keys
                ]

                if occupied_only:
                    # 等待被占用的密钥被释放
                    self._condition.wait()
                    continue  # 重新检查密钥状态

                # 如果没有仅被占用的密钥，计算最小等待时间
                min_wait_time = float("inf")
                for key in keys:
                    internal_key = self._hash_key(key)
                    wait_time = self._get_wait_time_for_key(internal_key, current_time)
                    if wait_time < min_wait_time:
                        min_wait_time = wait_time

                if min_wait_time == float("inf"):
                    raise RuntimeError("无法确定密钥的等待时间，可能没有可用密钥。")

                if min_wait_time >= 4 * 3600:
                    total_keys = len(keys)
                    cooling_keys = len(self.cooldown_keys)
                    cooldown_info = ", ".join(
                        f"{k}: {(v - current_time) / 3600:.1f}h"
                        for k, v in self.cooldown_keys.items()
                    )
                    print(
                        f"All keys are unavailable, most likely due to daily API limits. "
                        f"Will retry in {min_wait_time / 3600:.1f} hours, or interrupt the program to add new keys. "
                        f"(Total keys: {total_keys}, Cooling down: {cooling_keys}, "
                        f"Cooldown times: {cooldown_info})"
                    )

                # 等待最小的等待时间，或者在等待被释放时唤醒
                wait_time = min_wait_time if min_wait_time > 0 else None
                self._condition.wait(timeout=wait_time)

    def context(self, keys: List[Any]):
        """
        上下文管理器，用于自动释放密钥。例如：
            with key_manager.context(keys) as key:
                # 使用 key 进行请求
        """

        class KeyContext:
            def __init__(self, manager: Pool, key: Any):
                self.manager = manager
                self.key = key
                self.entered = False

            def __enter__(self):
                # 在进入上下文时标记使用请求数+1
                self.manager.mark_key_used(self.key)
                self.entered = True
                return self.key

            def __exit__(self, exc_type, exc_val, exc_tb):
                # 无论成功或失败，都释放密钥占用
                if self.entered:
                    # 如果没有异常发生，说明key使用成功，重置连续冷却计数
                    if exc_type is None:
                        internal_key = self.manager._hash_key(self.key)
                        self.manager.consecutive_cooldown_counts[internal_key] = 0
                    self.manager.release_key(self.key)

        key = self.get_available_key(keys)
        return KeyContext(self, key)
