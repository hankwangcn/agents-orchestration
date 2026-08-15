"""DAG 调度器（架构 §3.2）：拓扑派发 + 失败传播 + 死任务剪枝。

- 只认拓扑序：in-degree=0（依赖全部 success）的任务可派发
- 结果导向：派发后只等结果，不监控过程（D3）
- 失败处理：自动重试 N 次 → 耗尽后 prune_after_failure（反向可达剪枝）
  → 对剪枝时仍在运行的任务下发取消（best-effort，架构 §5.3）
- 竞态（§5.3）：剪枝后重新计算可派发集合，天然冻结被剪任务的派发
"""
from __future__ import annotations

from typing import Optional


from .models import (
    DAG,
    ErrorInfo,
    PruneReport,
    Result,
    ScheduleReport,
    Task,
    TaskStatus,
)
from .adapters.base import AgentAdapter
from .registry import AgentRegistry
from .models import Assignment


class Scheduler:
    """同步顺序调度器（阶段一骨架 + 阶段三任务分配）。

    registry: AgentRegistry——任务分配（精确 model / 能力匹配 / 降级默认）
    与多 agent 资源协调（多实例轮询、连续失败摘除）都由注册表负责。
    """

    def __init__(
        self,
        registry: AgentRegistry,
        retries: int = 2,
        backoff_base: float = 1.0,
    ):
        self._registry = registry
        self.retries = retries
        self.backoff_base = backoff_base

    # ------------------------------------------------------------------

    def run(self, dag: DAG) -> ScheduleReport:
        """调度一个 DAG 至全部终态（原地推进状态），返回收尾报告。"""
        self._current_dag = dag
        self._task_agent: dict[str, str] = {}  # task_id → agent_id（取消用）
        results: dict[str, Result] = {}
        prune_reports: list[PruneReport] = []
        assignments: list[Assignment] = []

        while not dag.all_terminal():
            ready = dag.ready_tasks()
            if not ready:
                # 有非终态但无 ready：依赖失败/取消导致不可达，防御性跳过
                for t in dag.tasks.values():
                    if t.status == TaskStatus.PENDING:
                        t.status = TaskStatus.SKIPPED
                break

            for tid in ready:
                task = dag.tasks[tid]
                assignment, adapter = self._registry.assign(task)
                assignments.append(assignment)
                self._task_agent[tid] = assignment.agent_id

                result = self._execute_with_retry(task, adapter)
                results[tid] = result

                # 结果反馈注册表：连续失败摘除（best-effort）
                if result.success:
                    self._registry.record_success(assignment.agent_id)
                else:
                    self._registry.record_failure(assignment.agent_id)

                if result.success:
                    continue

                # 重试耗尽 → 失败传播 + 死任务剪枝（架构 §5.2）
                report = dag.prune_after_failure(tid)
                prune_reports.append(report)
                self._dispatch_cancels(dag, report)
                # 剪枝改变了状态集合 → 重新进入外层循环
                break

        total_cost = sum(
            r.usage.cost for r in results.values() if r.usage
        )
        final_status = self._final_status(dag, prune_reports)
        return ScheduleReport(
            dag=dag,
            results=results,
            prune_reports=prune_reports,
            assignments=assignments,
            total_cost=round(total_cost, 4),
            final_status=final_status,
        )

    # ------------------------------------------------------------------

    def _execute_with_retry(self, task: Task, adapter: AgentAdapter) -> Result:
        """派发任务，失败自动重试 N 次（架构 §5.1）。adapter 由分配层给出。"""
        task.status = TaskStatus.RUNNING
        inputs = self._collect_inputs(task)

        last: Result | None = None
        for attempt in range(self.retries + 1):
            request_id = f"{task.id}:run:{attempt}"
            try:
                result = adapter.run_task(task, request_id=request_id, inputs=inputs)
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

        assert last is not None
        task.result = last
        task.status = (
            TaskStatus.SUCCESS if last.success else TaskStatus.FAILED
        )
        return last

    def _collect_inputs(self, task: Task) -> dict:
        """组装上游结果作为任务输入（数据流依赖，架构 §4.3）。

        每个依赖的输入 = 上游任务的结果 output（结果导向：只传结果，不传状态）。
        """
        dag = self._current_dag
        inputs: dict = {}
        for dep in task.deps:
            dep_task = dag.tasks[dep]
            inputs[dep] = dep_task.result.output if dep_task.result else None
        return inputs

    def _dispatch_cancels(self, dag: DAG, report: PruneReport) -> None:
        """对剪枝时仍在运行的任务下发取消（best-effort，架构 §5.3）。

        同步模型下剪枝时无 running 任务，此逻辑为并发模型（阶段四）预留。
        取消走分配时的 agent（_task_agent 记录），不重新分配。
        """
        for p in report.pruned:
            if p["state_at_cancel"] != TaskStatus.RUNNING.value:
                continue
            agent_id = self._task_agent.get(p["task_id"])
            if agent_id is None:
                continue
            self._registry.get_adapter(agent_id).cancel(
                task_id=p["task_id"],
                request_id=f"{p['task_id']}:cancel",
            )

    @staticmethod
    def _final_status(dag: DAG, prune_reports: list[PruneReport]) -> str:
        if not prune_reports:
            return "success"
        finals = dag.final_tasks()
        any_final_success = any(
            dag.tasks[f].status == TaskStatus.SUCCESS for f in finals
        )
        return "partial" if any_final_success else "failed"
