"""框架侧 wall-clock 超时原语（执行层 #34 与规划层 info 采集共用）。

执行层对单次任务调用设墙钟上限（#34）；规划层对 info_request 采集同样设
墙钟上限（决策点校验 / TTL 刷新都在调用方路径上，采集卡死不能无封顶地
拖住调用方）。两者共用同一原语，避免各写一份。

异步路径直接用 asyncio.wait_for（各调用点自行使用），此模块只提供同步原语。
"""
from __future__ import annotations

import threading
from typing import Callable, TypeVar

T = TypeVar("T")


def call_with_timeout(fn: Callable[[], T], timeout_seconds: float) -> T:
    """同步调用封顶：超过 timeout_seconds 未返回则抛 TimeoutError。

    用 daemon 线程执行——超时后调用方立即返回（不阻塞解释器退出），挂死的
    调用不再被等待。这正是"给调用占用封顶"的目的：调用方资源随超时释放，
    被调方即便不中断也不再拖住整条链路。原调用方异常原样回抛。
    """
    box: dict = {}

    def _target() -> None:
        try:
            box["result"] = fn()
        except BaseException as e:  # 原样回抛给调用方（含适配器自身异常）
            box["error"] = e

    th = threading.Thread(target=_target, daemon=True)
    th.start()
    th.join(timeout_seconds)
    if th.is_alive():
        raise TimeoutError(f"调用超过 {timeout_seconds}s 未返回")
    if "error" in box:
        raise box["error"]
    return box["result"]
