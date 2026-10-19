"""轻量 TTL 缓存 + stale-while-revalidate 支持

背景：ARM 小主机（4 核 + SD 卡）上 Docker 的 stats(stream=False) 必须采集两帧，
单次就要 1~2 秒。单纯设 TTL 对"隔十几秒手动刷新"毫无帮助（每次都过期 → 每次都重算）。

因此本模块额外提供 get_stale()：即使数据已过期，也把上一次的快照交出来，
由调用方立刻响应，同时后台线程慢慢重算。用户侧体感即为"瞬时"。
"""

import threading
import time
from typing import Any, Callable, Optional, Tuple

DEFAULT_TTL = 30.0

# key -> (set_at, value, ttl)
_store: dict[str, tuple[float, Any, float]] = {}
_lock = threading.Lock()
_hits = 0
_stale_hits = 0
_misses = 0


def get(key: str) -> Optional[Any]:
    """只返回仍在新鲜期内的值。"""
    global _hits, _misses
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        if item is None:
            _misses += 1
            return None
        set_at, value, ttl = item
        if now - set_at >= ttl:
            _misses += 1
            return None
        _hits += 1
        return value


def get_stale(key: str, max_age: Optional[float] = None) -> Tuple[Optional[Any], float]:
    """返回 (值, 年龄秒)。即使已过期也返回旧值，供立即响应用。

    max_age 用于兜底：年龄超过该秒数则视为不可用（防止返回几小时前的数据）。
    """
    global _stale_hits
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        if item is None:
            return None, 0.0
        set_at, value, ttl = item
        age = now - set_at
        if max_age is not None and age > max_age:
            return None, age
        if age >= ttl:
            _stale_hits += 1
        return value, age


def set(key: str, value: Any, ttl: float = DEFAULT_TTL) -> None:
    """写入缓存。value 为 None 时不写入，避免把失败结果缓存成空。"""
    if value is None:
        return
    with _lock:
        _store[key] = (time.monotonic(), value, ttl)


def is_fresh(key: str) -> bool:
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        return bool(item and now - item[0] < item[2])


def age(key: str) -> Optional[float]:
    """缓存年龄（秒）；无该键返回 None。"""
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        return (now - item[0]) if item else None


def invalidate(*keys: str) -> None:
    with _lock:
        for k in keys:
            _store.pop(k, None)


def invalidate_all() -> None:
    with _lock:
        _store.clear()


def stats() -> dict:
    now = time.monotonic()
    with _lock:
        entries = {
            k: {"age_s": round(now - v[0], 2), "ttl_s": v[2]}
            for k, v in _store.items()
        }
    return {
        "entries": len(entries),
        "hits": _hits,
        "stale_hits": _stale_hits,
        "misses": _misses,
        "default_ttl_s": DEFAULT_TTL,
        "per_entry": entries,
    }


def get_or_set(key: str, producer: Callable[[], Any], ttl: float = DEFAULT_TTL) -> Any:
    cached = get(key)
    if cached is not None:
        return cached
    value = producer()
    if value is not None:
        set(key, value, ttl)
    return value
