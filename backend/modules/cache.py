"""轻量 TTL 缓存模块

用途：MyPanel 的热点只读接口。ARM 小主机（4 核 + SD 卡）上 Docker API 的
全量 inspect / list 比较慢，30 秒内重复请求直接命中内存缓存，避免每次刷新
都全量重算，把首屏从 6 秒级压到 1~2 秒级。

设计要点：
- 进程内字典 + 单调时钟（time.monotonic），无第三方依赖、无需 Redis
- 线程安全：Docker 调用跑在 asyncio.to_thread 的线程池里
- 写操作（启停容器 / 改设置 / 删镜像 / 改定时任务等）调用 invalidate_all()
  立即整体失效，保证"操作后立刻能看到新状态"
"""

import threading
import time
from typing import Any, Callable, Optional

DEFAULT_TTL = 30.0

_store: dict[str, tuple[float, Any]] = {}
_lock = threading.Lock()
_hits = 0
_misses = 0


def get(key: str) -> Optional[Any]:
    """返回未过期的缓存值；无缓存或已过期返回 None。"""
    global _hits, _misses
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        if item is None:
            _misses += 1
            return None
        expire_at, value = item
        if now >= expire_at:
            _store.pop(key, None)
            _misses += 1
            return None
        _hits += 1
        return value


def set(key: str, value: Any, ttl: float = DEFAULT_TTL) -> None:
    """写入缓存。value 为 None 时不写入，避免把失败结果缓存成"空"。"""
    if value is None:
        return
    with _lock:
        _store[key] = (time.monotonic() + ttl, value)


def is_fresh(key: str) -> bool:
    """缓存是否仍然有效（不改变命中统计）。"""
    now = time.monotonic()
    with _lock:
        item = _store.get(key)
        return bool(item and now < item[0])


def invalidate(*keys: str) -> None:
    """让指定的若干 key 失效。"""
    with _lock:
        for k in keys:
            _store.pop(k, None)


def invalidate_all() -> None:
    """让全部缓存失效（写操作后调用）。"""
    with _lock:
        _store.clear()


def stats() -> dict:
    """缓存运行状态，便于排查。"""
    with _lock:
        return {
            "entries": len(_store),
            "hits": _hits,
            "misses": _misses,
            "ttl_seconds": DEFAULT_TTL,
        }


def get_or_set(key: str, producer: Callable[[], Any], ttl: float = DEFAULT_TTL) -> Any:
    """同步场景：命中直接返回，否则执行 producer 并写入缓存。"""
    cached = get(key)
    if cached is not None:
        return cached
    value = producer()
    if value is not None:
        set(key, value, ttl)
    return value
