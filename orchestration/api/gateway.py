"""API 网关（阶段四：框架从"库"到"系统"的入口）。

形态：FastAPI REST。职责：目标拆解（规划层 → DAG）、提交 DAG（可选带原始
目标）、查询进度、获取结果/审计、取消 run、查看 agent 注册表快照。协议面
保持"框架永远主动"——网关只受理框架自己的请求，agent 侧的协议通信仍在
适配器层，不暴露到 HTTP。

RunManager 管理 run 生命周期（run_id → 后台 asyncio.Task + 状态快照），
同一 AsyncScheduler 实例可并发运行多个 run（_RunCtx 状态隔离）。run 收尾后，
若提交时给了原始目标且注入了 Reflector，则做一次目标达成度判定（治理层
反思/判定，advisory——只写报告，不改状态不阻断）。

**学习层闭环**：run 收尾时对同一份报告做确定性复盘——审计对账 →
成本归集 → 规则提取（含判定结论），产物挂回报告；启用存储时规则落盘成
**跨 run 经验库**，并经 `lessons.PromptAdvisor` 回馈拆解提示词
（见 `decomposer.Decomposer(guidance_provider=...)`）。

**运行存档 + 接入层 Web**：启用存储时，run 的**过程事件流**（状态变更逐条
落盘）与**终态报告**即运行存档（持久化底座、唯一真源）；网关另行提供
**运行枚举**（`/api/runs`）、**人读投影**（`/api/runs/{id}/view`，确定性、
按需渲染）、**叙述摘要**（`/api/runs/{id}/narrative`，LLM 非确定、显式触发）
与**服务端渲染的 Web 页面**（`/` 列表 + `/runs/{id}` 详情，同源、零构建、
零 CORS）。人读版是存档的读时投影，不落盘成第二份真相。
"""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ..audit import Auditor
from ..cost import CostAccountant
from ..decomposer import DecomposeError, Decomposer
from ..dependency import DependencyGraph
from ..learning import LearningEngine
from ..lessons import build_digest
from ..metrics import MetricsCollector, configure_logging, log_event
from ..models import DAG, Result, TaskStatus
from ..narrative import NarrativeError, Narrator
from ..reflection import ReflectionReport, Reflector
from ..registry import AgentRegistry
from ..report_view import build_run_view, render_index_html, render_run_html
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
    goal: str = ""  # 用户原始目标（判定基准）；直接提交 DAG 且未传时为空
    status: str = "running"  # running | done | failed
    report: Optional[ScheduleReport] = None
    error: str = ""
    cancel_event: Optional[asyncio.Event] = None
    started_at: str = ""
    finished_at: str = ""
    task: Optional[asyncio.Task] = None
    reflection_obj: Optional[ReflectionReport] = None
    """判定结论对象（喂学习层 JUD-* 规则；报告里是 dict 形态）。"""


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
        decomposer: Optional[Decomposer] = None,
        reflector: Optional[Reflector] = None,
        learning: Optional[LearningEngine] = None,
        narrator: Optional[Narrator] = None,
    ):
        self._registry = registry
        self._metrics = metrics or MetricsCollector()
        self._store = state_store
        self._decomposer = decomposer
        self._reflector = reflector
        self._learning = learning or LearningEngine()
        self._narrator = narrator
        self._scheduler = AsyncScheduler(
            registry=registry,
            retries=retries,
            metrics=self._metrics,
            state_store=state_store,
        )
        self._runs: dict[str, RunHandle] = {}

    async def submit(
        self,
        dag: DAG,
        run_id: Optional[str] = None,
        goal: Optional[str] = None,
    ) -> str:
        """提交 DAG 并立即返回 run_id（后台执行）。

        goal：用户原始目标（可选）——提交时给定后随 run 持久化，收尾时作为
        反思/判定的基准。直接提交现成 DAG（未走拆解）且不传时无基准，判定跳过。
        """
        rid = run_id or uuid.uuid4().hex[:12]
        if rid in self._runs:
            raise ValueError(f"run_id 已存在：{rid}")
        if self._store is not None and self._store.has_run(rid):
            raise ValueError(f"run_id 已存在于存储中：{rid}")
        handle = RunHandle(
            run_id=rid, dag=dag, goal=goal or "", cancel_event=asyncio.Event()
        )
        handle.started_at = _now()
        self._runs[rid] = handle
        if self._store is not None and handle.goal:
            # 先落盘目标：调度器的事件驱动落盘（goal=None）随后保留该值
            self._store.save_run(rid, dag, goal=handle.goal)
        handle.task = asyncio.create_task(self._run_background(handle))
        log_event("run_submitted", run_id=rid, dag_size=len(dag.tasks),
                  has_goal=bool(handle.goal))
        return rid

    async def _run_background(self, handle: RunHandle) -> None:
        try:
            handle.report = await self._scheduler.run(
                handle.dag, run_id=handle.run_id,
                cancel_event=handle.cancel_event,
            )
            await self._attach_reflection(handle)
            self._attach_learning(handle)
            handle.status = "done"
        except Exception as e:  # 调度器异常兜底（不应发生，防御）
            handle.error = str(e)
            handle.status = "failed"
            log_event("run_error", run_id=handle.run_id, error=str(e))
        finally:
            handle.finished_at = _now()

    async def _attach_reflection(self, handle: RunHandle) -> None:
        """治理层反思/判定：run 收尾后按原始目标判定最终交付（advisory）。

        无目标 / 无判定 agent 时判定自身会跳过（不视为错误）；判定是 advisory，
        任何异常都不得影响 run 的收尾状态。
        """
        if self._reflector is None or handle.report is None:
            return
        try:
            rr = await self._reflector.areflect(
                handle.goal, handle.report, run_id=handle.run_id
            )
        except Exception as e:  # 防御：判定永不阻断 run 收尾
            log_event("reflection_error", run_id=handle.run_id, error=str(e))
            return
        handle.reflection_obj = rr
        handle.report.reflection = rr.model_dump()
        log_event(
            "run_reflected", run_id=handle.run_id, judged=rr.judged,
            enabled=rr.enabled, achieved=rr.achieved,
            independent=rr.independent, judge_agent=rr.judge_agent,
            cost=rr.cost,
        )
        if self._store is not None:
            self._store.save_report(handle.run_id, handle.report)

    def _attach_learning(self, handle: RunHandle) -> None:
        """学习层闭环：对同一份报告做确定性复盘 → 落盘经验库 → 回馈拆解。

        ① 审计对账（只读、可复跑）→ ② 成本归集 → ③ 规则提取（含判定结论
        JUD-*）→ ④ 挂回报告 + 落盘为**跨 run 经验库**（回馈拆解提示词在
        拆解侧经 PromptAdvisor 读取）。

        学习层是复盘、不是主链路：任何异常都不得影响 run 收尾（防御）。
        """
        if handle.report is None:
            return
        try:
            audit = Auditor(self._registry).audit(handle.report)
            cost = CostAccountant(self._registry).account(handle.report)
            report = self._learning.learn(
                audit, cost, handle.reflection_obj
            )
        except Exception as e:  # 防御：学习层故障不阻断 run 收尾
            log_event("learning_error", run_id=handle.run_id, error=str(e))
            return
        handle.report.audit = audit.model_dump()
        handle.report.cost = cost.model_dump()
        handle.report.learning = report.model_dump()
        stored = False
        if self._store is not None:
            try:
                self._store.save_lessons(handle.run_id, report)
                stored = True
            except Exception as e:
                log_event("lesson_store_error", run_id=handle.run_id, error=str(e))
        log_event(
            "run_learned", run_id=handle.run_id, audit_verdict=audit.verdict,
            rules=report.rule_count, stored=stored,
        )
        if self._store is not None:
            self._store.save_report(handle.run_id, handle.report)

    # ---------- 规划层接入：目标 → DAG ----------

    async def decompose(
        self,
        goal: str,
        submit: bool = False,
        run_id: Optional[str] = None,
    ) -> dict:
        """把自然语言目标拆解为 DAG（规划层头部接入）。

        submit=True 时拆解完直接提交，返回的 run_id 可用于后续
        status/report/metrics——"目标 → 结果"一条链走完。
        """
        if self._decomposer is None:
            raise HTTPException(
                status_code=400,
                detail="未配置拆解引擎（规划层）——请设置环境变量 "
                       "DEEPSEEK_API_KEY 后重启 serve，或注入自定义 decomposer",
            )
        try:
            # 拆解是同步 LLM 调用（框架内部组件，不走协议），丢线程池避免阻塞事件循环
            dag = await asyncio.to_thread(self._decomposer.decompose, goal)
        except DecomposeError as e:
            raise HTTPException(status_code=422, detail=f"拆解失败：{e}") from None
        out = {
            "goal": goal,
            "status": "decomposed",
            "dag": dag.model_dump(mode="json"),
            # 规划层「依赖分析」产物摘要：拓扑分层即并行前沿（同层可并行），
            # 最大并行宽度供资源协调参考
            "analysis": _dependency_analysis(dag),
            "run_id": None,
        }
        log_event("goal_decomposed", dag_size=len(dag.tasks), submit=submit)
        if submit:
            out["run_id"] = await self.submit(dag, run_id=run_id, goal=goal)
            out["status"] = "submitted"
        return out

    # ---------- 查询 ----------

    def snapshot(self, run_id: str) -> dict:
        """进度快照：各任务状态表（API 轮询用）。"""
        handle = self._get(run_id)
        return {
            "run_id": run_id,
            "goal": handle.goal,
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
        """收尾报告（含审计/成本/学习产物——治理 + 学习层的确定性复盘）。

        内存 handle 优先（运行中的最新）；进程重启后 handle 不在 → 从运行存档
        读回（原先只写不读，重启后已完成 run 的报告返回 404）。
        """
        handle = self._runs.get(run_id)
        if handle is None:
            return self._report_from_archive(run_id)
        if handle.status == "running":
            raise HTTPException(status_code=409, detail="run 尚未结束")
        if handle.status == "failed":
            raise HTTPException(status_code=500, detail=handle.error)
        report = handle.report
        assert report is not None
        return {
            "run_id": run_id,
            "goal": handle.goal,
            "final_status": report.final_status,
            "total_cost": report.total_cost,
            "task_results": {
                tid: _result_summary(r) for tid, r in report.results.items()
            },
            "prune_reports": [p.model_dump() for p in report.prune_reports],
            "assignments": [a.model_dump() for a in report.assignments],
            "reflection": report.reflection,
            "audit": report.audit,
            "cost": report.cost,
            "learning": report.learning,
        }

    def _report_from_archive(self, run_id: str) -> dict:
        """从运行存档读回收尾报告（进程重启后的读回路径）。"""
        arch = self._load_archive(run_id)
        if arch is None:
            raise HTTPException(status_code=404, detail=f"run 不存在：{run_id}")
        rep = arch["report"]
        if rep is None:
            raise HTTPException(status_code=409, detail="run 尚未收尾（无终态报告）")
        return {
            "run_id": run_id,
            "goal": arch["goal"],
            "final_status": rep.get("final_status"),
            "total_cost": rep.get("total_cost"),
            "task_results": {
                tid: _result_summary_dict(r)
                for tid, r in (rep.get("results") or {}).items()
            },
            "prune_reports": rep.get("prune_reports") or [],
            "assignments": rep.get("assignments") or [],
            "reflection": rep.get("reflection"),
            "audit": rep.get("audit"),
            "cost": rep.get("cost"),
            "learning": rep.get("learning"),
        }

    def lessons_snapshot(self, limit: int = 20) -> dict:
        """跨 run 经验库视图（学习层闭环的"记忆"出口）。

        聚合口径：同 rule_id 跨 run 命中次数 / 贡献 run 数 / 最高 severity /
        最近一次的证据——复现次数即"客观支撑"的强度。
        """
        if self._store is None:
            raise HTTPException(
                status_code=400,
                detail="未启用状态存储（state_store）——经验库无落盘载体",
            )
        digest = build_digest(self._store.load_lessons())
        return {
            "runs_considered": digest.runs_considered,
            "lesson_count": len(digest.lessons),
            "generated_at": digest.generated_at,
            "lessons": [ls.model_dump() for ls in digest.lessons[: max(limit, 0)]],
        }

    # ---------- 运行存档：读回 / 枚举 / 人读投影 / 叙述摘要 ----------

    def list_runs(self, limit: int = 50) -> list[dict]:
        """运行枚举（最近在前）。持久化底座优先，内存中的运行中 run 兜底纳入。"""
        out: list[dict] = []
        seen: set[str] = set()
        if self._store is not None:
            for r in self._store.list_runs(limit=limit):
                out.append(r)
                seen.add(r["run_id"])
        for rid, h in self._runs.items():
            if rid in seen:
                continue
            out.append({
                "run_id": rid,
                "goal": h.goal,
                "run_status": h.status,
                "updated_at": h.finished_at or h.started_at,
                "has_report": h.report is not None,
            })
        out.sort(key=lambda r: r.get("updated_at") or "", reverse=True)
        return out[: max(0, limit)]

    def _load_archive(self, run_id: str) -> Optional[dict]:
        """读运行存档（过程事件流 + 终态报告 + 目标）。

        持久化底座优先（进程重启后仍可读回）；底座未落盘时回退到内存 handle
        （run 刚提交、尚未首次落盘的窗口）。都没有返回 None。
        """
        if self._store is not None and self._store.has_run(run_id):
            data = self._store.load_run(run_id)
            return {
                "goal": data.get("goal", ""),
                "run_status": data.get("run_status", ""),
                "dag": data["dag"].model_dump(mode="json"),
                "events": self._store.load_events(run_id),
                "report": self._store.load_report(run_id),
                "narrative": self._store.load_narrative(run_id),
            }
        handle = self._runs.get(run_id)
        if handle is None:
            return None
        dag = (handle.report.dag if handle.report else handle.dag)
        return {
            "goal": handle.goal,
            "run_status": handle.status,
            "dag": dag.model_dump(mode="json"),
            "events": (self._store.load_events(run_id)
                       if self._store is not None else []),
            "report": (handle.report.model_dump(mode="json")
                       if handle.report is not None else None),
            "narrative": (self._store.load_narrative(run_id)
                          if self._store is not None else None),
        }

    def archive_view(self, run_id: str) -> dict:
        """运行存档的人读投影（确定性、按需渲染，不落盘成第二份真相）。"""
        arch = self._load_archive(run_id)
        if arch is None:
            raise HTTPException(status_code=404, detail=f"run 不存在：{run_id}")
        return build_run_view(
            run_id=run_id, goal=arch["goal"], run_status=arch["run_status"],
            dag=arch["dag"], events=arch["events"], report=arch["report"],
            narrative=arch["narrative"],
        )

    async def narrate(self, run_id: str) -> dict:
        """显式生成叙述摘要（LLM 产出、非确定、单独留痕）——不默认生成。"""
        if self._narrator is None:
            raise HTTPException(
                status_code=400,
                detail="未配置叙述引擎（LLM）——请设置 $DEEPSEEK_API_KEY 后重启 "
                       "serve，或注入自定义 narrator",
            )
        view = self.archive_view(run_id)
        if not view["archive"]["report_available"]:
            raise HTTPException(
                status_code=409, detail="run 尚未收尾——叙述摘要需终态报告"
            )
        try:
            narrative = await self._narrator.anarrate(view)
        except NarrativeError as e:
            raise HTTPException(status_code=502, detail=f"叙述生成失败：{e}") from None
        if self._store is not None:
            try:
                self._store.save_narrative(run_id, narrative)
            except Exception as e:  # 归档写失败不阻断返回（叙述仍可呈现）
                log_event("narrative_store_error", run_id=run_id, error=str(e))
        log_event("run_narrated", run_id=run_id, model=narrative.get("model"))
        return {
            "run_id": run_id,
            "source": "llm",
            "deterministic": False,
            **narrative,
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
        data = self._store.load_run(run_id)
        handle = RunHandle(
            run_id=run_id, dag=data["dag"], goal=data.get("goal", ""),
            cancel_event=asyncio.Event(),
        )
        handle.started_at = _now()
        self._runs[run_id] = handle
        handle.task = asyncio.create_task(self._run_background_resume(handle))
        log_event("run_resume_requested", run_id=run_id, has_goal=bool(handle.goal))
        return run_id

    async def _run_background_resume(self, handle: RunHandle) -> None:
        try:
            handle.report = await self._scheduler.resume_run(
                handle.run_id, cancel_event=handle.cancel_event
            )
            handle.dag = handle.report.dag  # 同步为调度器实际推进的对象
            await self._attach_reflection(handle)
            self._attach_learning(handle)
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
    goal: Optional[str] = Field(
        default=None,
        description="用户原始目标（可选）——收尾时作为反思/判定的基准；"
                    "直接提交现成 DAG 时建议一并给出",
    )


class DecomposeRequest(BaseModel):
    goal: str = Field(description="自然语言目标——由规划层拆解为任务 DAG")
    submit: bool = Field(default=False, description="拆解后直接提交执行")
    run_id: Optional[str] = Field(default=None, description="submit 时的自定义 run_id")


class TaskResolve(BaseModel):
    action: str = Field(description="complete | cancel | retry")
    result: Optional[dict] = Field(default=None, description="complete 时的人工核实结果契约")


def create_app(
    registry: AgentRegistry,
    retries: int = 2,
    metrics: Optional[MetricsCollector] = None,
    state_store: Optional[StateStore] = None,
    decomposer: Optional[Decomposer] = None,
    reflector: Optional[Reflector] = None,
    learning: Optional[LearningEngine] = None,
    narrator: Optional[Narrator] = None,
) -> tuple[FastAPI, RunManager]:
    """构造 (app, manager)。registry 需已注册 agent（可先 collect 能力声明）。

    state_store：启用断点持久化（SQLite 等），提供 resume / resolve / lessons 端点，
    并作为学习层跨 run 经验库与**运行存档**（过程事件流 + 终态报告）的落盘载体。
    decomposer：启用规划层拆解（POST /api/decompose）——未注入则端点返回 400。
    reflector：启用治理层反思/判定（run 收尾后按 goal 判定交付，advisory）——
    未注入则不做判定。
    learning：替换学习层阈值配置（默认 LearningEngine()；审计/成本/学习是
    确定性复盘，始终启用，不依赖注入）。
    narrator：启用叙述摘要（POST /api/runs/{id}/narrative，LLM 产出、非确定、
    显式触发）——未注入则端点返回 400。人读**结构化**投影（Web/CLI）始终可用，
    不依赖 narrator。
    """
    configure_logging()
    manager = RunManager(
        registry, retries=retries, metrics=metrics, state_store=state_store,
        decomposer=decomposer, reflector=reflector, learning=learning,
        narrator=narrator,
    )
    app = FastAPI(
        title="Agents Orchestration Gateway",
        description="结果导向 Agent 编排框架——API 入口",
        version="0.1.0",
    )

    # ---------- 接入层 Web 页面（服务端渲染；同源、零构建、零 CORS）----------

    @app.get("/", response_class=HTMLResponse)
    async def index_page() -> HTMLResponse:
        """运行列表页（run 枚举 → 跳转详情）。"""
        try:
            runs = manager.list_runs(limit=200)
        except Exception:  # 存档读失败不阻塞首页
            runs = []
        return HTMLResponse(render_index_html(runs))

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    async def run_page(run_id: str) -> HTMLResponse:
        """运行详情页（整条业务完成过程：时间线 + 任务 + 治理 + 学习）。"""
        try:
            view = manager.archive_view(run_id)
        except HTTPException as e:
            return HTMLResponse(
                _error_page(run_id, str(e.detail)), status_code=e.status_code
            )
        return HTMLResponse(render_run_html(view))

    @app.post("/api/runs", status_code=201)
    async def submit_run(payload: DagSubmit) -> dict:
        """提交 DAG，返回 run_id（后台执行）。

        可选给 goal：提交后随 run 持久化，收尾时作为反思/判定的基准。
        """
        dag = DAG.model_validate(payload.dag)
        run_id = await manager.submit(dag, run_id=payload.run_id, goal=payload.goal)
        return {"run_id": run_id, "status": "submitted"}

    @app.get("/api/runs")
    async def list_runs(limit: int = 50) -> dict:
        """运行枚举（最近在前）——崩溃后/换进程后仍可发现已有 run。"""
        return {"runs": manager.list_runs(limit=limit)}


    @app.post("/api/decompose")
    async def decompose_goal(payload: DecomposeRequest) -> dict:
        """目标 → DAG（规划层）；submit=true 时拆解后直接提交。"""
        return await manager.decompose(
            payload.goal, submit=payload.submit, run_id=payload.run_id
        )

    @app.get("/api/runs/{run_id}")
    async def run_status(run_id: str) -> dict:
        """进度快照（任务状态表）。"""
        return manager.snapshot(run_id)

    @app.get("/api/runs/{run_id}/report")
    async def run_report(run_id: str) -> dict:
        """收尾报告（结果/剪枝/分配/成本）。"""
        return manager.report(run_id)

    @app.get("/api/runs/{run_id}/view")
    async def run_view(run_id: str) -> dict:
        """运行存档的人读投影（确定性、按需渲染）：过程时间线 + 任务 + 治理 + 学习。"""
        return manager.archive_view(run_id)

    @app.post("/api/runs/{run_id}/narrative")
    async def run_narrative(run_id: str) -> dict:
        """显式生成叙述摘要（LLM 产出、非确定、单独留痕）——不默认生成。"""
        return await manager.narrate(run_id)

    @app.get("/api/runs/{run_id}/metrics")
    async def run_metrics(run_id: str) -> dict:
        """运行指标（可观测性）。"""
        return manager.metrics_snapshot(run_id)

    @app.get("/api/lessons")
    async def lessons(limit: int = 20) -> dict:
        """跨 run 经验库（学习层闭环记忆）：规则聚合 + 复现证据强度。"""
        return manager.lessons_snapshot(limit=limit)

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


def _dependency_analysis(dag: DAG) -> dict:
    """依赖分析摘要（规划层 DependencyGraph 的结构视图）。

    只暴露结构信息，不含运行时状态——运行中进度看 snapshot。
    """
    graph = DependencyGraph(dag)
    levels = graph.levels()
    return {
        "task_count": len(graph.tasks),
        "levels": levels,                       # 拓扑分层：同层可并行
        "depth": len(levels),
        "max_parallel_width": graph.max_parallel_width(),
        "roots": sorted(graph.roots()),
        "final_tasks": sorted(graph.final_tasks()),
    }


def _error_page(run_id: str, detail: str) -> str:
    """Web 详情页的错误呈现（不返回裸 JSON）。"""
    import html as _h
    return (
        '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/>'
        '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
        f'<title>run {_h.escape(run_id)}</title></head><body '
        'style="font-family:system-ui,-apple-system,sans-serif;background:#f1f5f9;'
        'margin:0;padding:60px 20px"><div style="max-width:640px;margin:0 auto;'
        'background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:28px 32px">'
        f'<h1 style="font-size:18px;margin:0 0 10px;color:#0f172a">无法打开 run '
        f'{_h.escape(run_id)}</h1>'
        f'<p style="color:#64748b;font-size:13.5px;margin:0 0 18px">'
        f'{_h.escape(detail)}</p>'
        '<a href="/" style="color:#0284c7;font-size:13.5px">← 返回运行列表</a>'
        '</div></body></html>'
    )



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


def _result_summary_dict(r: dict) -> dict:
    """Result 转储（dict）的对外摘要——与 _result_summary 同形（存档读回用）。"""
    if not r:
        return {}
    return {
        "success": r.get("success"),
        "output": r.get("output"),
        "error": r.get("error"),
        "cost": (r.get("usage") or {}).get("cost", 0.0),
        "duration_ms": r.get("duration_ms", 0),
        "retries": r.get("retries", 0),
    }
