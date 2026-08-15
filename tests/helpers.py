"""测试共享工具：异步脚本适配器 + 快速构造 helper（阶段四测试用）。"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from orchestration.adapters.base import AgentAdapter
from orchestration.models import DAG, Result, Task


class AsyncScriptedAdapter(AgentAdapter):
    """script: task_id -> [Result|dict 序列]，每次调用消费一个。

    delay：所有任务的固定延迟；script_delay：按任务覆盖（模拟慢任务）。
    延迟用 asyncio.sleep——I/O 等待不阻塞事件循环，可真实并发。
    """

    def __init__(self, script: dict[str, list[dict]], model: str = "deepseek-chat",
                 delay: float = 0.0):
        super().__init__(model=model)
        self.script = {k: list(v) for k, v in script.items()}
        self.calls: list[tuple[str, str, dict, float]] = []
        self.cancelled: list[str] = []
        self.delay = delay
        self.script_delay: dict[str, float] = {}

    async def arun_task(self, task: Task, request_id: str,
                        inputs: Optional[dict] = None) -> Result:
        self.calls.append((task.id, request_id, dict(inputs or {}), time.monotonic()))
        d = self.script_delay.get(task.id, self.delay)
        if d:
            await asyncio.sleep(d)
        payload = self.script.get(task.id, [{}]).pop(0)
        if isinstance(payload, Result):
            return payload
        merged = dict(payload)
        merged.setdefault("task_id", task.id)
        return Result(**merged)

    async def acancel(self, task_id: str, request_id: str) -> Result:
        self.cancelled.append(task_id)
        return Result(task_id=task_id, success=True, output=None)

    def _call_llm(self, messages: list[dict]) -> str:
        raise NotImplementedError


class ConcurrencyProbeAdapter(AsyncScriptedAdapter):
    """记录活跃调用数峰值（验证 per-agent 并发上限）。"""

    def __init__(self, script: dict, model: str = "deepseek-chat"):
        super().__init__(script, model=model)
        self.active = 0
        self.peak = 0

    async def arun_task(self, task: Task, request_id: str,
                        inputs: Optional[dict] = None) -> Result:
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.05)
        self.active -= 1
        payload = self.script.get(task.id, [{}]).pop(0)
        merged = dict(payload)
        merged.setdefault("task_id", task.id)
        return Result(**merged)


def ok(task_id: str, output=None, cost: float = 0.01) -> dict:
    return {"task_id": task_id, "success": True, "output": output or {},
            "usage": {"tokens_in": 10, "tokens_out": 10, "cost": cost}}


def fail(task_id: str, code: str = "model_error") -> dict:
    return {"task_id": task_id, "success": False,
            "error": {"code": code, "message": "failed"}}


def dag_of(*specs: tuple[str, list[str]]) -> DAG:
    return DAG(tasks={tid: Task(id=tid, desc=tid, deps=list(deps)) for tid, deps in specs})
