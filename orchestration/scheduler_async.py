"""异步并发调度器（架构 §3.2；阶段四核心）。

相对同步 Scheduler 的差异：
- **并发派发**：每轮把全部 ready 任务并行派发（事件驱动：FIRST_COMPLETED
  持续推进，完成一个处理一个），不再逐任务串行
- **资源限制执行**（阶段三从统计走向执行）：
  - 并发上限：per-agent asyncio.Semaphore（注册表 max_concurrency）
  - 速率配额：per-agent 滑动窗口限速（注册表 rate_limit_per_min）
- **竞态处理**（架构 §5.3）：任务失败 → 冻结新派发 → 剪枝 →
  对 RUNNING 任务逐级下发取消（best-effort）→ 等待全部 in-flight 收尾
  → 统一解冻；被剪任务的晚到结果直接丢弃（不写 task.result、不计健康度）
- **外部取消**（API 网关）：cancel_event 置位 → 整棵取消（PENDING 置
  CANCELLED、RUNNING 下发取消、等待收尾），final_status=cancelled
- **可观测性**：结构化日志 + MetricsCollector（按 run_id 隔离，
  同一实例可并发运行多个 run——RunManager 场景，状态全部在 _RunCtx 内）

取消契约（D5）：cancel 为 best-effort——晚到结果一律丢弃，不依赖 agent 履约。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional


from .adapters.base import AgentAdapter
from .metrics import MetricsCollector, log_event
from .models import (
    DAG,
    ErrorInfo,
    PruneReport,
    Result,
    ScheduleReport,
    Task,
    TaskStatus,
)
from .registry import AgentRegistry
from .models import Assignment


@dataclass
class _RunCtx:
    """单次 run 的全部运行状态（多 run 并发隔离的关键）。

    实例字段只放只读配置与 per-agent 资源（sem/限速器），
    运行中可变状态全部收在这里。
    """
    dag: DAG
    run_id: str
    assignments: dict[str, Assignment] = field(default_factory=dict)
    task_agent: dict[str, str] = field(default_factory=dict)
    results: dict[str, Result] = field(default_factory=dict)
    prune_reports: list[PruneReport] = field(default_factory=list)
    cancelled: bool = False
    started_at: float = field(default_factory=time.monotonic)

    @property
    def duration_ms(self) -> int:
        return int((time.monotonic() - self.started_at) * 1000)


class _RateLimiter:
    """per-agent 固定窗口限速（rate_limit_per_min）。limit<=0 表示不限。"""

    def __init__(self, limit_per_min: int, window_seconds: float = 60.0):
        self.limit = limit_per_min
        self.window = window_seconds
        self._window_start = 0.0
        self._count = 0

    async def acquire(self) -> None:
        if self.limit <= 0:
            return
        now = time.monotonic()
        if now - self._window_start >= self.window:
            self._window_start = now
            self._count = 0
        if self._count >= self.limit:
            wait = self.window - (now - self._window_start)
            if wait > 0:
                await asyncio.sleep(wait)
            self._window_start = time.monotonic()
            self._count = 0
        self._count += 1


class AsyncScheduler:
    """异步并发调度器：事件驱动 + 竞态处理 + 资源限制执行。

    与同步 Scheduler 同构（复用 DAG 剪枝算法、Assignment 留痕），
    但派发/等待/取消全部异步化。
    """

    def __init__(
        self,
        registry: AgentRegistry,
        retries: int = 2,
        backoff_base: float = 0.1,
        metrics: Optional[MetricsCollector] = None,
        rate_window_seconds: float = 60.0,
    ):
        self._registry = registry
        self.retries = retries
        self.backoff_base = backoff_base
        self._metrics = metrics
        self._rate_window = rate_window_seconds
        # per-agent 资源（跨 run 共享：agent 池是全局的）
        self._sems: dict[str, asyncio.Semaphore] = {}
        self._ratelim: dict[str, _RateLimiter] = {}

    # ------------------------------------------------------------------

    async def run(
        self,
        dag: DAG,
        run_id: str = "run_1",
        cancel_event: Optional[asyncio.Event] = None,
    ) -> ScheduleReport:
        """调度一个 DAG 至全部终态（原地推进状态），返回收尾报告。"""
        ctx = _RunCtx(dag=dag, run_id=run_id)
        pending: dict[str, asyncio.Task] = {}
        frozen = False

        if self._metrics:
            self._metrics.begin_run(run_id, len(dag.tasks))
        log_event("run_started", run_id=run_id, dag_size=len(dag.tasks))

        while not dag.all_terminal():
            # 外部取消请求（API 网关）→ 整棵取消
            if cancel_event is not None and cancel_event.is_set():
                await self._cancel_all(ctx, pending)
                break

            # 派发阶段：全部 ready 并行派发（未冻结时）
            if not frozen:
                for tid in dag.ready_tasks():
                    if tid in pending:
                        continue
                    assignment, adapter = self._registry.assign(dag.tasks[tid])
                    ctx.assignments[tid] = assignment
                    ctx.task_agent[tid] = assignment.agent_id
                    if not self._has_quota(assignment):  # 并发上限（非阻塞）
                        continue
                    pending[tid] = asyncio.create_task(
                        self._execute_task(ctx, tid, assignment, adapter)
                    )
                    if self._metrics:
                        self._metrics.task_launched(
                            run_id, assignment.agent_id, assignment.match_type
                        )
                    log_event(
                        "task_launched", run_id=run_id, task_id=tid,
                        agent_id=assignment.agent_id,
                        match_type=assignment.match_type,
                    )

            if not pending:
                # 无 in-flight：依赖失败/取消导致不可达 → 防御性跳过
                skipped = self._mark_skipped(dag)
                if self._metrics and skipped:
                    self._metrics.skipped(run_id, skipped)
                break

            # 等待阶段：任一完成即处理（事件驱动持续推进）
            done, _ = await asyncio.wait(
                pending.values(), return_when=asyncio.FIRST_COMPLETED
            )
            for tid in [t for t in pending if pending[t] in done]:
                coro = pending.pop(tid)
                result = coro.result()  # 异常已在执行内部兜底为失败 Result
                task = dag.tasks[tid]
                if task.status in (TaskStatus.CANCELLED, TaskStatus.SKIPPED):
                    # 晚到结果直接丢弃（§5.3）：不写 result、不计健康度
                    log_event(
                        "result_dropped", run_id=run_id, task_id=tid,
                        reason="task_cancelled",
                    )
                    continue
                ctx.results[tid] = result
                assignment = ctx.assignments[tid]
                self._finish_task(ctx, tid, result, assignment)
                if not result.success:
                    # 失败传播 + 死任务剪枝 → 冻结 → 逐级取消 → 收尾
                    frozen = True
                    report = dag.prune_after_failure(tid)
                    ctx.prune_reports.append(report)
                    if self._metrics:
                        self._metrics.pruned(run_id, len(report.pruned))
                    log_event(
                        "prune", run_id=run_id, root_failure=tid,
                        pruned_count=len(report.pruned),
                        pruned_final=report.pruned_final,
                    )
                    await self._dispatch_cancels(ctx, report, pending)

            if frozen and not pending:
                frozen = False  # 收尾完成，解冻继续下一轮

        total_cost = sum(
            r.usage.cost for r in ctx.results.values() if r.usage
        )
        final_status = self._final_status(dag, ctx.prune_reports, ctx.cancelled)
        if self._metrics:
            self._metrics.finish_run(run_id, final_status)
        log_event(
            "run_finished", run_id=run_id, final_status=final_status,
            total_cost=round(total_cost, 4), duration_ms=ctx.duration_ms,
        )
        return ScheduleReport(
            dag=dag,
            results=ctx.results,
            prune_reports=ctx.prune_reports,
            assignments=list(ctx.assignments.values()),
            total_cost=round(total_cost, 4),
            final_status=final_status,
        )

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------

    def _has_quota(self, assignment: Assignment) -> bool:
        """并发上限非阻塞检查：无空闲配额则本轮跳过（下一轮再试）。"""
        agent = self._registry.get(assignment.agent_id)
        sem = self._sem_for(agent.agent_id, agent.max_concurrency)
        return not sem.locked()

    def _sem_for(self, agent_id: str, max_concurrency: int) -> asyncio.Semaphore:
        if agent_id not in self._sems:
            self._sems[agent_id] = asyncio.Semaphore(max(1, max_concurrency))
        return self._sems[agent_id]

    def _ratelim_for(self, agent_id: str, rate_limit_per_min: int) -> _RateLimiter:
        if agent_id not in self._ratelim:
            self._ratelim[agent_id] = _RateLimiter(
                rate_limit_per_min, window_seconds=self._rate_window
            )
        return self._ratelim[agent_id]

    async def _execute_task(
        self,
        ctx: _RunCtx,
        tid: str,
        assignment: Assignment,
        adapter: AgentAdapter,
    ) -> Result:
        """执行单个任务：限速 → 并发闸门 → 重试执行。"""
        task = ctx.dag.tasks[tid]
        agent = self._registry.get(assignment.agent_id)
        ratelim = self._ratelim_for(agent.agent_id, agent.rate_limit_per_min)
        sem = self._sem_for(agent.agent_id, agent.max_concurrency)

        await ratelim.acquire()
        async with sem:
            if task.status != TaskStatus.PENDING:
                # 排队期间已被外部取消 → 不再执行
                return Result(
                    task_id=tid, success=False,
                    error=ErrorInfo(code="cancelled_before_run"),
                )
            # 进入执行区：先置 RUNNING 再统计并发水位（瞬时任务也能记到峰值）
            task.status = TaskStatus.RUNNING
            if self._metrics:
                cur = sum(
                    1 for t in ctx.dag.tasks.values()
                    if t.status == TaskStatus.RUNNING
                )
                self._metrics.task_concurrency(
                    ctx.run_id, agent.agent_id, cur
                )
            result = await self._execute_with_retry(ctx, task, adapter)
            return result

    async def _execute_with_retry(
        self,
        ctx: _RunCtx,
        task: Task,
        adapter: AgentAdapter,
    ) -> Result:
        """派发任务，失败自动重试 N 次 + 指数退避（架构 §5.1）。

        执行期间被剪枝/外部取消（task.status 变为 CANCELLED）→ 立即停止重试，
        晚到结果由外层丢弃。
        """
        task.status = TaskStatus.RUNNING
        inputs = self._collect_inputs(ctx, task)

        last: Result | None = None
        for attempt in range(self.retries + 1):
            if task.status in (TaskStatus.CANCELLED, TaskStatus.SKIPPED):
                break  # 外部已判定取消 → 停止（不再烧钱）
            request_id = f"{task.id}:run:{attempt}"
            try:
                result = await adapter.arun_task(
                    task, request_id=request_id, inputs=inputs
                )
            except Exception as e:  # 适配器层异常（网络等）→ 视为失败
                result = Result(
                    task_id=task.id,
                    success=False,
                    error=ErrorInfo(code="adapter_error", message=str(e)),
                    retries=attempt,
                )
            result.retries = attempt
            last = result
            if result.success:
                break
            if attempt < self.retries and self.backoff_base > 0:
                await asyncio.sleep(self.backoff_base * (2**attempt))

        if last is None:
            last = Result(
                task_id=task.id, success=False,
                error=ErrorInfo(code="cancelled"),
            )
        if task.status not in (TaskStatus.CANCELLED, TaskStatus.SKIPPED):
            task.result = last
            task.status = (
                TaskStatus.SUCCESS if last.success else TaskStatus.FAILED
            )
        return last

    def _collect_inputs(self, ctx: _RunCtx, task: Task) -> dict:
        """组装上游结果作为任务输入（数据流依赖，架构 §4.3）。"""
        inputs: dict = {}
        for dep in task.deps:
            dep_task = ctx.dag.tasks[dep]
            inputs[dep] = dep_task.result.output if dep_task.result else None
        return inputs

    def _finish_task(
        self,
        ctx: _RunCtx,
        tid: str,
        result: Result,
        assignment: Assignment,
    ) -> None:
        """任务结果反馈注册表 + 指标（健康度只对真实失败任务计）。"""
        if result.success:
            self._registry.record_success(assignment.agent_id)
        else:
            self._registry.record_failure(assignment.agent_id)
        if self._metrics:
            self._metrics.task_done(
                ctx.run_id,
                assignment.agent_id,
                success=result.success,
                duration_ms=result.duration_ms,
                cost=result.usage.cost,
                tokens_in=result.usage.tokens_in,
                tokens_out=result.usage.tokens_out,
            )
        log_event(
            "task_done", run_id=ctx.run_id, task_id=tid,
            success=result.success,
            error_code=result.error.code if result.error else "",
            cost=result.usage.cost, duration_ms=result.duration_ms,
        )

    # ------------------------------------------------------------------
    # 取消（竞态处理，架构 §5.3）
    # ------------------------------------------------------------------

    async def _dispatch_cancels(
        self,
        ctx: _RunCtx,
        report: PruneReport,
        pending: dict[str, asyncio.Task],
    ) -> None:
        """对剪枝时仍在运行的任务逐级下发取消（best-effort）。

        流程：先冻结新派发（frozen=True 已置）→ 这里逐级下发 →
        外层等待 pending 全部收尾 → 统一解冻。
        无论 agent 是否履约，晚到结果都会被丢弃（状态已是 CANCELLED）。
        """
        for p in report.pruned:
            if p["state_at_cancel"] != TaskStatus.RUNNING.value:
                continue
            tid = p["task_id"]
            agent_id = ctx.task_agent.get(tid)
            if agent_id is None:
                continue
            try:
                await self._registry.get_adapter(agent_id).acancel(
                    task_id=tid, request_id=f"{tid}:cancel"
                )
                log_event("cancel_sent", run_id=ctx.run_id, task_id=tid, agent_id=agent_id)
            except Exception as e:
                log_event(
                    "cancel_failed", run_id=ctx.run_id, task_id=tid,
                    error=str(e), best_effort=True,
                )

    async def _cancel_all(
        self,
        ctx: _RunCtx,
        pending: dict[str, asyncio.Task],
    ) -> None:
        """外部取消整棵 DAG：PENDING 直接置 CANCELLED，RUNNING 下发取消并等待收尾。"""
        ctx.cancelled = True
        running_ids: list[str] = []
        for tid, t in ctx.dag.tasks.items():
            if t.status == TaskStatus.PENDING:
                t.status = TaskStatus.CANCELLED
            elif t.status == TaskStatus.RUNNING:
                # 置终态：晚到结果由 _execute_with_retry 检测到 CANCELLED 丢弃
                t.status = TaskStatus.CANCELLED
                running_ids.append(tid)
        for tid in running_ids:
            agent_id = ctx.task_agent.get(tid)
            if agent_id is None:
                continue
            try:
                await self._registry.get_adapter(agent_id).acancel(
                    task_id=tid, request_id=f"{tid}:cancel"
                )
            except Exception:
                pass  # best-effort
        if pending:
            await asyncio.gather(*pending.values(), return_exceptions=True)
        log_event("run_cancelled", run_id=ctx.run_id, cancelled_tasks=len(ctx.dag.tasks))

    # ------------------------------------------------------------------

    @staticmethod
    def _mark_skipped(dag: DAG) -> int:
        count = 0
        for t in dag.tasks.values():
            if t.status == TaskStatus.PENDING:
                t.status = TaskStatus.SKIPPED
                count += 1
        return count

    @staticmethod
    def _final_status(
        dag: DAG,
        prune_reports: list[PruneReport],
        cancelled: bool,
    ) -> str:
        if cancelled:
            return "cancelled"
        if not prune_reports:
            return "success"
        finals = dag.final_tasks()
        any_final_success = any(
            dag.tasks[f].status == TaskStatus.SUCCESS for f in finals
        )
        return "partial" if any_final_success else "failed"
