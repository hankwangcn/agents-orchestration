"""API 网关（阶段四：框架从"库"到"系统"的入口）。

形态：FastAPI REST。职责：提交 DAG、查询进度、获取结果/审计、取消 run、
查看 agent 注册表快照。协议面保持"框架永远主动"——网关只受理框架自己的
请求，agent 侧的协议通信仍在适配器层，不暴露到 HTTP。

RunManager 管理 run 生命周期（run_id → 后台 asyncio.Task + 状态快照），
同一 AsyncScheduler 实例可并发运行多个 run（_RunCtx 状态隔离）。
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ..metrics import MetricsCollector, configure_logging, log_event
from ..models import DAG, Result, TaskStatus
from ..registry import AgentRegistry
from ..scheduler_async import AsyncScheduler, ScheduleReport
from ..state_store import StateStore


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
    """DAG 提交与 run 生命周期管理（单进程内运行）。

    state_store：启用断点持久化（可选）。启用后每次调度状态变更落盘，
    进程崩溃后可通过 resume 恢复；副作用任务恢复时置 INTERRUPTED，
    经 resolve（complete/cancel/retry）人工确认后继续。
    """

    def __init__(
        self,
        registry: AgentRegistry,
        retries: int = 2,
        metrics: Optional[MetricsCollector] = None,
        state_store: Optional[StateStore] = None,
    ):
        self._registry = registry
        self._metrics = metrics or MetricsCollector()
        self._store = state_store
        self._scheduler = AsyncScheduler(
            registry=registry,
            retries=retries,
            metrics=self._metrics,
            state_store=state_store,
        )
        self._runs: dict[str, RunHandle] = {}

    async def submit(self, dag: DAG, run_id: Optional[str] = None) -> str:
        """提交 DAG 并立即返回 run_id（后台执行）。"""
        rid = run_id or uuid.uuid4().hex[:12]
        if rid in self._runs:
            raise ValueError(f"run_id 已存在：{rid}")
        if self._store is not None and self._store.has_run(rid):
            raise ValueError(f"run_id 已存在于存储中：{rid}")
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

    async def resume(self, run_id: str) -> str:
        """崩溃恢复：从 StateStore 加载 run 并继续调度（需启用断点持久化）。

        RUNNING 任务按 A+B 策略处理：无副作用重派，有副作用置 INTERRUPTED。
        """
        if self._store is None:
            raise HTTPException(
                status_code=400, detail="未启用断点持久化（state_store），无法恢复"
            )
        if not self._store.has_run(run_id):
            raise HTTPException(status_code=404, detail=f"run 不存在：{run_id}")
        if run_id in self._runs and self._runs[run_id].status == "running":
            raise HTTPException(status_code=409, detail="run 已在运行中")
        dag = self._store.load_run(run_id)["dag"]
        handle = RunHandle(run_id=run_id, dag=dag, cancel_event=asyncio.Event())
        handle.started_at = _now()
        self._runs[run_id] = handle
        handle.task = asyncio.create_task(self._run_background_resume(handle))
        log_event("run_resume_requested", run_id=run_id)
        return run_id

    async def _run_background_resume(self, handle: RunHandle) -> None:
        try:
            handle.report = await self._scheduler.resume_run(
                handle.run_id, cancel_event=handle.cancel_event
            )
            handle.dag = handle.report.dag  # 同步为调度器实际推进的对象
            handle.status = "done"
        except Exception as e:  # 调度器异常兜底（防御）
            handle.error = str(e)
            handle.status = "failed"
            log_event("run_error", run_id=handle.run_id, error=str(e))
        finally:
            handle.finished_at = _now()

    async def resolve_task(
        self,
        run_id: str,
        task_id: str,
        action: str,
        result: Optional[dict] = None,
    ) -> dict:
        """人工确认中断任务（仅 INTERRUPTED 可 resolve；需启用断点持久化）。

        - complete：附带人工核实的结果契约 → 任务置 SUCCESS
        - cancel：任务置 CANCELLED（副作用未执行或已核实放弃）
        - retry：任务置 PENDING（等待下一次 resume 重新派发）
        """
        if self._store is None:
            raise HTTPException(
                status_code=400, detail="resolve 需要启用断点持久化（state_store）"
            )
        data = self._store.load_run(run_id)
        dag: DAG = data["dag"]
        if task_id not in dag.tasks:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        task = dag.tasks[task_id]
        if task.status != TaskStatus.INTERRUPTED:
            raise HTTPException(
                status_code=409,
                detail=f"任务 {task_id} 状态 {task.status.value}，仅 INTERRUPTED 可 resolve",
            )
        if action == "complete":
            if not result:
                raise HTTPException(status_code=400, detail="complete 需要 result 契约")
            task.result = Result.model_validate(result)
            task.status = TaskStatus.SUCCESS
        elif action == "cancel":
            task.status = TaskStatus.CANCELLED
        elif action == "retry":
            task.status = TaskStatus.PENDING
            task.result = None
        else:
            raise HTTPException(
                status_code=400,
                detail=f"未知 action：{action}（可选 complete | cancel | retry）",
            )
        # 保留原 assignments/prune，局部状态变更落盘
        self._store.save_run(
            run_id,
            dag,
            assignments=list(data["assignments"].values()),
            prune_reports=data["prune_reports"],
        )
        log_event(
            "task_resolved", run_id=run_id, task_id=task_id,
            action=action, new_status=task.status.value,
        )
        return {
            "run_id": run_id,
            "task_id": task_id,
            "action": action,
            "status": task.status.value,
            "hint": (
                "任务已置 PENDING，调用 POST /api/runs/{run_id}/resume 继续调度"
                if action == "retry" else ""
            ),
        }

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

    def _task_agent(self, handle: RunHandle, tid: str) -> str:
        if handle.report is not None:
            for a in handle.report.assignments:
                if a.task_id == tid:
                    return a.agent_id
            return ""
        # 运行中（report 仅收尾后生成）：从调度器 live 分配映射读，
        # 避免运行中快照 tasks[].agent 恒为空
        return self._scheduler.task_agents(handle.run_id).get(tid, "")


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

class DagSubmit(BaseModel):
    dag: dict = Field(description="DAG JSON（tasks: {id: {desc, deps, ...}}）")
    run_id: Optional[str] = None


class TaskResolve(BaseModel):
    action: str = Field(description="complete | cancel | retry")
    result: Optional[dict] = Field(default=None, description="complete 时的人工核实结果契约")


def create_app(
    registry: AgentRegistry,
    retries: int = 2,
    metrics: Optional[MetricsCollector] = None,
    state_store: Optional[StateStore] = None,
) -> tuple[FastAPI, RunManager]:
    """构造 (app, manager)。registry 需已注册 agent（可先 collect 能力声明）。

    state_store：启用断点持久化（SQLite 等），提供 resume / resolve 端点。
    """
    configure_logging()
    manager = RunManager(
        registry, retries=retries, metrics=metrics, state_store=state_store
    )
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

    @app.post("/api/runs/{run_id}/resume")
    async def resume_run(run_id: str) -> dict:
        """崩溃恢复：从 StateStore 加载 run 并继续调度。"""
        rid = await manager.resume(run_id)
        return {"run_id": rid, "status": "resumed"}

    @app.post("/api/runs/{run_id}/tasks/{task_id}/resolve")
    async def resolve_task(run_id: str, task_id: str, payload: TaskResolve) -> dict:
        """人工确认中断任务（仅 INTERRUPTED 可 resolve）。"""
        return await manager.resolve_task(
            run_id, task_id, payload.action, payload.result
        )

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
