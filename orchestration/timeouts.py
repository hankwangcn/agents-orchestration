"""框架侧 wall-clock 超时原语（任务执行与信息采集共用）。

任务执行对单次调用设墙钟上限；信息采集同样设
墙钟上限（决策点校验 / TTL 刷新都在调用方路径上，采集无响应不能无上限地
阻塞调用方）。两者共用同一原语，避免各写一份。

异步路径直接用 asyncio.wait_for（各调用点自行使用），此模块只提供同步原语。
"""
from __future__ import annotations

import threading
from typing import Callable, TypeVar

T = TypeVar("T")


def call_with_timeout(fn: Callable[[], T], timeout_seconds: float) -> T:
    """同步调用上限：超过 timeout_seconds 未返回则抛 TimeoutError。

    用 daemon 线程执行——超时后调用方立即返回（不阻塞解释器退出），无响应的
    调用不再被等待。这正是"为调用占用设置上限"的目的：调用方资源随超时释放，
    被调方即便不中断也不再阻塞整条链路。调用方自身的异常原样向上抛出。
    """
    box: dict = {}

    def _target() -> None:
        try:
            box["result"] = fn()
        except BaseException as e:  # 原样向上抛出给调用方（含适配器自身异常）
            box["error"] = e

    th = threading.Thread(target=_target, daemon=True)
    th.start()
    th.join(timeout_seconds)
    if th.is_alive():
        raise TimeoutError(f"调用超过 {timeout_seconds}s 未返回")
    if "error" in box:
        raise box["error"]
    return box["result"]
