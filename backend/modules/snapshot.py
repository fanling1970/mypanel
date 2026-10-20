"""后台常驻采集器（Snapshot collector）

思路：把「每次请求现算」换成「后台持续算、请求只读内存」。

为什么需要它
------------
Docker 的 `stats(stream=False)` 必须等两帧 cgroup 采样才能算出结果，
单次就要 1~2 秒；容器越多、Compose 项目里越分散，现算耗时越长。
只靠 stale-while-revalidate 只能让「命中缓存」的请求变快，
一旦快照超过刷新阈值，后台还会再付一次 1~2 秒——冷启动、切页、
手动刷新时依然能感觉到停顿。

采集器用一个后台线程按固定节奏（默认 2 秒 / 5 秒）把结果算好写进内存，
所有读接口直接返回内存里的快照，请求路径不再触碰任何 Docker 采样。

设计要点
--------
- 单飞：同一采集项上一轮没跑完，这一轮直接跳过，不会堆积。
- 线程池：多个采集项并行，互不阻塞；慢的（stats）不会拖住快的（列表）。
- 失败隔离：单个采集项抛异常只记录错误计数，旧快照继续可用。
- 可观测：每个项的 age/interval/runs/errors/last_ms 都能查询，
  便于在 /api/debug/perf 里判断采集器是否健康。
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class SnapshotCollector:
    """按固定间隔在后台重建快照，供请求线程直接读取。"""

    def __init__(self, tick: float = 0.5, max_workers: int = 4):
        self._tick = float(tick)
        self._data = {}      # key -> (value, monotonic_ts)
        self._jobs = {}      # key -> job dict
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._pool = ThreadPoolExecutor(max_workers=max_workers,
                                        thread_name_prefix="snapshot")

    # ---------- 注册 / 读取 ----------

    def register(self, key: str, fn, interval: float = 3.0,
                 initial_delay: float = 0.0) -> None:
        """登记一个采集项。interval 为两次采集的最小间隔（秒）。"""
        with self._lock:
            self._jobs[key] = {
                "fn": fn,
                "interval": float(interval),
                "next_due": time.monotonic() + float(initial_delay),
                "inflight": False,
                "runs": 0,
                "errors": 0,
                "last_ms": None,
            }

    def covers(self, key: str) -> bool:
        """该 key 是否由采集器负责（是则调用方不应再走即时计算）。"""
        return key in self._jobs

    def get(self, key: str):
        """返回 (value, age_s)；无快照时返回 (None, None)。"""
        with self._lock:
            item = self._data.get(key)
        if item is None:
            return None, None
        return item[0], time.monotonic() - item[1]

    def put(self, key: str, value) -> None:
        """外部主动写入快照（例如 force 刷新后回填）。"""
        with self._lock:
            self._data[key] = (value, time.monotonic())

    def invalidate(self, *keys: str) -> None:
        """丢弃快照并让采集项立即到期（变更操作后调用）。

        丢弃后第一次读取会回落到即时计算，同时下一个 tick 立刻重采，
        因此不会长时间没有数据。
        """
        now = time.monotonic()
        with self._lock:
            for k in keys:
                self._data.pop(k, None)
                j = self._jobs.get(k)
                if j is not None:
                    j["next_due"] = now

    # ---------- 观测 ----------

    def snapshot_stats(self) -> dict:
        now = time.monotonic()
        with self._lock:
            out = {}
            for k, j in self._jobs.items():
                item = self._data.get(k)
                out[k] = {
                    "interval": j["interval"],
                    "runs": j["runs"],
                    "errors": j["errors"],
                    "last_ms": j["last_ms"],
                    "age_s": round(now - item[1], 2) if item else None,
                    "has_data": item is not None,
                }
            return out

    # ---------- 调度 ----------

    def _submit_due(self) -> None:
        now = time.monotonic()
        due = []
        with self._lock:
            for k, j in self._jobs.items():
                if j["inflight"] or now < j["next_due"]:
                    continue
                j["inflight"] = True
                j["next_due"] = now + j["interval"]
                due.append((k, j["fn"]))
        for k, fn in due:
            self._pool.submit(self._run_job, k, fn)

    def _run_job(self, key: str, fn) -> None:
        t0 = time.monotonic()
        ok = True
        value = None
        try:
            value = fn()
        except Exception as e:
            ok = False
            logger.debug(f"[snapshot] {key} 采集失败: {e}")
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        with self._lock:
            if ok:
                self._data[key] = (value, time.monotonic())
            j = self._jobs.get(key)
            if j:
                j["inflight"] = False
                j["runs"] += 1
                if not ok:
                    j["errors"] += 1
                j["last_ms"] = round(elapsed_ms, 1)

    def _loop(self) -> None:
        while not self._stop.wait(self._tick):
            try:
                self._submit_due()
            except Exception as e:
                logger.debug(f"[snapshot] 调度异常: {e}")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop,
                                        name="snapshot-collector",
                                        daemon=True)
        self._thread.start()
        logger.info("[snapshot] 后台常驻采集器已启动，采集项: "
                    + ", ".join(sorted(self._jobs.keys())))

    def stop(self) -> None:
        self._stop.set()
        self._thread = None
