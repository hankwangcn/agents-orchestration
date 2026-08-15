"""API 网关（阶段四：框架从"库"到"系统"的入口）。

形态：FastAPI REST。职责：提交 DAG、查询进度、获取结果/审计、取消 run、
查看 agent 注册表快照。协议层保持"框架永远主动"——网关只受理框架自己的
请求，agent 侧的协议通信仍在适配器层，不暴露到 HTTP。

RunManager 管理 run 生命周期（run_id → 后台 asyncio.Task + 状态快照），
同一 AsyncScheduler 实例可并发运行多个 run（_RunCtx 状态隔离）。
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..metrics import MetricsCollector, configure_logging, log_event
from ..models import DAG
from ..registry import AgentRegistry
from ..scheduler_async import AsyncScheduler, ScheduleReport


# ---------------------------------------------------------------------------
# Run 生命周期管理
# ---------------------------------------------------------------------------

@dataclass
class RunHandle:
    """一次已提交 run 的句柄。"""
    run_id: str
    dag: DAG
    status: str = "running"  # running | done | failed
    report: Optional[ScheduleReport] = None
    error: str = ""
    cancel_event: Optional[asyncio.Event] = None
    started_at: str = ""
    finished_at: str = ""
    task: Optional[asyncio.Task] = None


class RunManager:
    """DAG 提交与 run 生命周期管理（单进程内运行）。"""

    def __init__(
        self,
        registry: AgentRegistry,
        retries: int = 2,
        metrics: Optional[MetricsCollector] = None,
    ):
        self._registry = registry
        self._metrics = metrics or MetricsCollector()
        self._scheduler = AsyncScheduler(
            registry=registry, retries=retries, metrics=self._metrics
        )
        self._runs: dict[str, RunHandle] = {}

    async def submit(self, dag: DAG, run_id: Optional[str] = None) -> str:
        """提交 DAG 并立即返回 run_id（后台执行）。"""
        rid = run_id or uuid.uuid4().hex[:12]
        if rid in self._runs:
            raise ValueError(f"run_id 已存在：{rid}")
        handle = RunHandle(run_id=rid, dag=dag, cancel_event=asyncio.Event())
        handle.started_at = _now()
        self._runs[rid] = handle
        handle.task = asyncio.create_task(self._run_background(handle))
        log_event("run_submitted", run_id=rid, dag_size=len(dag.tasks))
        return rid

    async def _run_background(self, handle: RunHandle) -> None:
        try:
            handle.report = await self._scheduler.run(
                handle.dag, run_id=handle.run_id,
                cancel_event=handle.cancel_event,
            )
            handle.status = "done"
        except Exception as e:  # 调度器异常兜底（不应发生，防御）
            handle.error = str(e)
            handle.status = "failed"
            log_event("run_error", run_id=handle.run_id, error=str(e))
        finally:
            handle.finished_at = _now()

    # ---------- 查询 ----------

    def snapshot(self, run_id: str) -> dict:
        """进度快照：各任务状态表（API 轮询用）。"""
        handle = self._get(run_id)
        return {
            "run_id": run_id,
            "status": handle.status,
            "started_at": handle.started_at,
            "finished_at": handle.finished_at,
            "tasks": [
                {
                    "id": t.id,
                    "status": t.status.value,
                    "agent": self._task_agent(handle, t.id),
                }
                for t in handle.dag.tasks.values()
            ],
        }

    def report(self, run_id: str) -> dict:
        """收尾报告（含审计数据源：assignments/prune/results/cost）。"""
        handle = self._get(run_id)
        if handle.status == "running":
            raise HTTPException(status_code=409, detail="run 尚未结束")
        if handle.status == "failed":
            raise HTTPException(status_code=500, detail=handle.error)
        report = handle.report
        assert report is not None
        return {
            "run_id": run_id,
            "final_status": report.final_status,
            "total_cost": report.total_cost,
            "task_results": {
                tid: _result_summary(r) for tid, r in report.results.items()
            },
            "prune_reports": [p.model_dump() for p in report.prune_reports],
            "assignments": [a.model_dump() for a in report.assignments],
        }

    async def cancel(self, run_id: str) -> dict:
        """请求取消一个 run（best-effort，整棵取消）。"""
        handle = self._get(run_id)
        if handle.status != "running":
            raise HTTPException(status_code=409, detail="run 不在运行中")
        assert handle.cancel_event is not None
        handle.cancel_event.set()
        return {"run_id": run_id, "cancel_requested": True}

    async def wait(self, run_id: str, timeout: Optional[float] = None) -> ScheduleReport:
        """等待 run 结束并返回报告（编程式调用用）。"""
        handle = self._get(run_id)
        assert handle.task is not None
        await asyncio.wait_for(asyncio.shield(handle.task), timeout=timeout)
        assert handle.report is not None
        return handle.report

    def agents_snapshot(self) -> dict:
        """注册表快照：agent 档案（能力/资源/状态）。"""
        return {
            aid: {
                "agent_id": a.agent_id,
                "model": a.model,
                "status": a.status,
                "capabilities": a.capabilities,
                "max_concurrency": a.max_concurrency,
                "rate_limit_per_min": a.rate_limit_per_min,
                "budget_limit_usd": a.budget_limit_usd,
                "languages": a.languages,
            }
            for aid, a in self._registry.agents.items()
        }

    def metrics_snapshot(self, run_id: str) -> dict:
        """run 的运行指标（可观测性出口）。"""
        m = self._metrics.run(run_id)
        if m is None:
            raise HTTPException(status_code=404, detail="metrics 不存在")
        return {
            "run_id": run_id,
            "duration_ms": m.duration_ms,
            "tasks": {
                "total": m.tasks_total,
                "success": m.tasks_success,
                "failed": m.tasks_failed,
                "cancelled": m.tasks_cancelled,
                "skipped": m.tasks_skipped,
            },
            "pruned_count": m.pruned_count,
            "total_cost": m.total_cost,
            "agents": {
                aid: {
                    "tasks": a.tasks,
                    "success_rate": a.success_rate,
                    "avg_duration_ms": a.avg_duration_ms,
                    "peak_concurrency": a.peak_concurrency,
                    "cost": a.total_cost,
                    "degraded": a.degraded,
                }
                for aid, a in m.agents.items()
            },
        }

    # ---------- 内部 ----------

    def _get(self, run_id: str) -> RunHandle:
        handle = self._runs.get(run_id)
        if handle is None:
            raise HTTPException(status_code=404, detail=f"run 不存在：{run_id}")
        return handle

    @staticmethod
    def _task_agent(handle: RunHandle, tid: str) -> str:
        if handle.report is None:
            return ""
        for a in handle.report.assignments:
            if a.task_id == tid:
                return a.agent_id
        return ""


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

class DagSubmit(BaseModel):
    dag: dict = Field(description="DAG JSON（tasks: {id: {desc, deps, ...}}）")
    run_id: Optional[str] = None


def create_app(
    registry: AgentRegistry,
    retries: int = 2,
    metrics: Optional[MetricsCollector] = None,
) -> tuple[FastAPI, RunManager]:
    """构造 (app, manager)。registry 需已注册 agent（可先 collect 能力声明）。"""
    configure_logging()
    manager = RunManager(registry, retries=retries, metrics=metrics)
    app = FastAPI(
        title="Agents Orchestration Gateway",
        description="结果导向 Agent 编排框架——API 入口",
        version="0.1.0",
    )

    @app.post("/api/runs", status_code=201)
    async def submit_run(payload: DagSubmit) -> dict:
        """提交 DAG，返回 run_id（后台执行）。"""
        dag = DAG.model_validate(payload.dag)
        run_id = await manager.submit(dag, run_id=payload.run_id)
        return {"run_id": run_id, "status": "submitted"}

    @app.get("/api/runs/{run_id}")
    async def run_status(run_id: str) -> dict:
        """进度快照（任务状态表）。"""
        return manager.snapshot(run_id)

    @app.get("/api/runs/{run_id}/report")
    async def run_report(run_id: str) -> dict:
        """收尾报告（结果/剪枝/分配/成本）。"""
        return manager.report(run_id)

    @app.get("/api/runs/{run_id}/metrics")
    async def run_metrics(run_id: str) -> dict:
        """运行指标（可观测性）。"""
        return manager.metrics_snapshot(run_id)

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> dict:
        """请求取消 run（best-effort）。"""
        return await manager.cancel(run_id)

    @app.get("/api/agents")
    async def agents_list() -> dict:
        """agent 注册表快照。"""
        return manager.agents_snapshot()

    return app, manager


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _result_summary(r: object) -> dict:
    """Result 契约的对外摘要（不泄露内部字段）。"""
    if r is None:
        return {}
    return {
        "success": r.success,
        "output": r.output,
        "error": r.error.model_dump() if r.error else None,
        "cost": r.usage.cost,
        "duration_ms": r.duration_ms,
        "retries": r.retries,
    }
