"""结果审计器（架构 §3.2 治理层；阶段二）。

输入：ScheduleReport（调度收尾报告）+ AgentRegistry（agent 档案，预算/能力对比用）。
输出：AuditReport——正确性对账 / 分配审计 / 剪枝审计 / 语义交叉校验 / 错误模式归集。

审计是"汇聚验证"环节：只对账事实，不修改任何状态（纯函数式，可安全复跑）。
同时产出喂给自我学习（learning.py）的原始素材。
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .models import Result, Task, TaskStatus
from .registry import AgentRegistry
from .models import Assignment
from .scheduler import ScheduleReport


class AuditReport(BaseModel):
    """一次调度的审计报告。"""

    # -- 总体 --
    total_tasks: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    success_rate: float = 0.0

    # -- 正确性对账（结果契约 vs 任务状态）--
    status_result_mismatches: list[dict] = Field(default_factory=list)
    """状态与 result.success 矛盾（如 status=success 但 result.success=false）。"""
    missing_results: list[str] = Field(default_factory=list)
    """SUCCESS/FAILED 终态但缺 result（硬性断链，协议 §4.2：无契约框架断链）。"""
    late_results: list[str] = Field(default_factory=list)
    """已取消但带 result（晚到结果，调度器应丢弃——此处标记暴露）。"""

    # -- 分配审计（阶段三留痕消费）--
    assignments_by_type: dict[str, int] = Field(default_factory=dict)
    degraded_tasks: list[dict] = Field(default_factory=list)
    """降级明细：task/model → 降级到谁，为什么。"""
    risky_tasks: list[dict] = Field(default_factory=list)
    """能力声明未覆盖需求的任务（risk=True），合并执行结果供审计盯防。"""

    # -- 剪枝审计（架构 §5.4 取消报告）--
    prune_events: int = 0
    pruned_task_count: int = 0
    pruned_final_events: int = 0
    root_failures: list[dict] = Field(default_factory=list)
    """失败根因列表（喂自我学习）。"""

    # -- 语义交叉校验（消息协议 §7.5）--
    side_effect_mismatches: list[dict] = Field(default_factory=list)
    """声明 side_effects vs 实际 side_effect_report 不一致。"""
    capability_risks: list[dict] = Field(default_factory=list)

    # -- 错误模式归集（喂自我学习）--
    error_patterns: list[dict] = Field(default_factory=list)
    """[{code, count, task_ids}] 按频率降序。"""

    # -- 汇总 --
    verdict: str = "ok"  # ok | warning | critical
    issues: list[str] = Field(default_factory=list)


class Auditor:
    """审计器：对 ScheduleReport 做只读对账。"""

    def __init__(self, registry: Optional[AgentRegistry] = None):
        self._registry = registry

    # ------------------------------------------------------------------

    def audit(self, report: ScheduleReport) -> AuditReport:
        dag = report.dag
        out = AuditReport()

        # ---- 总体 ----
        out.total_tasks = len(dag.tasks)
        for t in dag.tasks.values():
            out.by_status[t.status.value] = out.by_status.get(t.status.value, 0) + 1
        success_n = out.by_status.get(TaskStatus.SUCCESS.value, 0)
        out.success_rate = round(success_n / out.total_tasks, 4) if out.total_tasks else 0.0

        # ---- 正确性对账 ----
        for tid, task in dag.tasks.items():
            self._reconcile_task(out, tid, task)

        # ---- 分配审计 ----
        self._audit_assignments(out, report.assignments, dag)

        # ---- 剪枝审计 ----
        self._audit_prunes(out, report.prune_reports)

        # ---- 语义交叉校验 ----
        self._audit_side_effects(out, dag)

        # ---- 错误模式归集 ----
        self._collect_error_patterns(out, dag)

        # ---- 汇总 ----
        self._finalize(out)
        return out

    # ------------------------------------------------------------------

    def _reconcile_task(self, out: AuditReport, tid: str, task: Task) -> None:
        """单个任务的状态 vs 结果契约对账。"""
        res: Optional[Result] = task.result

        if task.status == TaskStatus.SUCCESS:
            if res is None:
                out.missing_results.append(tid)
            elif not res.success:
                out.status_result_mismatches.append({
                    "task_id": tid, "status": "success", "result_success": False,
                    "detail": "状态 success 但结果契约 success=false",
                })
        elif task.status == TaskStatus.FAILED:
            if res is None:
                out.missing_results.append(tid)
            elif res.success:
                out.status_result_mismatches.append({
                    "task_id": tid, "status": "failed", "result_success": True,
                    "detail": "状态 failed 但结果契约 success=true",
                })
        elif task.status == TaskStatus.CANCELLED:
            if res is not None:
                out.late_results.append(tid)  # 晚到结果：应被调度器丢弃

    def _audit_assignments(
        self,
        out: AuditReport,
        assignments: list[Assignment],
        dag,
    ) -> None:
        """分配留痕汇总：降级 / 风险明细（风险项合并执行结果）。"""
        by_type: dict[str, int] = {}
        for a in assignments:
            by_type[a.match_type] = by_type.get(a.match_type, 0) + 1
            if a.match_type == "degraded":
                out.degraded_tasks.append({
                    "task_id": a.task_id, "agent_id": a.agent_id,
                    "reason": a.reason,
                })
            if a.risk:
                task = dag.tasks.get(a.task_id)
                out.risky_tasks.append({
                    "task_id": a.task_id, "agent_id": a.agent_id,
                    "reason": a.reason,
                    "task_status": task.status.value if task else "unknown",
                })
        out.assignments_by_type = by_type

    def _audit_prunes(self, out: AuditReport, prune_reports) -> None:
        """剪枝取消报告汇总（架构 §5.4）。"""
        out.prune_events = len(prune_reports)
        for pr in prune_reports:
            out.pruned_task_count += len(pr.pruned)
            if pr.pruned_final:
                out.pruned_final_events += 1
            out.root_failures.append(pr.root_failure)

    def _audit_side_effects(self, out: AuditReport, dag) -> None:
        """语义交叉校验（协议 §7.5）：side_effects 声明 vs side_effect_report。

        - 声明 none 但报告了副作用 → 隐藏副作用，硬问题（剪枝/审计可能误判）
        - 声明有副作用但未报告 → 无法对账，也标记（审计看不到副作用落地）
        """
        for tid, task in dag.tasks.items():
            res = task.result
            if res is None:
                continue
            declared = task.side_effects.value
            reported = (res.side_effect_report or "").strip()
            if declared == "none" and reported:
                out.side_effect_mismatches.append({
                    "task_id": tid, "declared": declared, "reported": reported,
                    "detail": "声明无副作用但报告了副作用（隐藏副作用风险）",
                })
            elif declared != "none" and not reported:
                out.side_effect_mismatches.append({
                    "task_id": tid, "declared": declared, "reported": "",
                    "detail": "声明有副作用但未报告（无法对账）",
                })

    def _collect_error_patterns(self, out: AuditReport, dag) -> None:
        """失败任务错误码归集（喂自我学习：什么错最常发生）。"""
        counter: dict[str, dict] = {}
        for tid, task in dag.tasks.items():
            if task.status != TaskStatus.FAILED or task.result is None:
                continue
            err = task.result.error
            code = err.code if err else "unknown"
            entry = counter.setdefault(code, {"code": code, "count": 0, "task_ids": []})
            entry["count"] += 1
            entry["task_ids"].append(tid)
        out.error_patterns = sorted(
            counter.values(), key=lambda e: e["count"], reverse=True
        )

    def _finalize(self, out: AuditReport) -> None:
        """汇总判定：硬性问题（断链/矛盾）→ critical；软性问题 → warning。"""
        if out.missing_results or out.status_result_mismatches:
            out.verdict = "critical"
            out.issues.append(
                f"{len(out.missing_results)} 个任务缺结果契约，"
                f"{len(out.status_result_mismatches)} 处状态与结果矛盾"
            )
        elif (out.prune_events or out.degraded_tasks or out.risky_tasks
              or out.side_effect_mismatches or out.late_results):
            out.verdict = "warning"
            parts = []
            if out.prune_events:
                parts.append(f"{out.prune_events} 次剪枝（{out.pruned_task_count} 任务被剪）")
            if out.degraded_tasks:
                parts.append(f"{len(out.degraded_tasks)} 次降级分配")
            if out.risky_tasks:
                parts.append(f"{len(out.risky_tasks)} 个风险分配")
            if out.side_effect_mismatches:
                parts.append(f"{len(out.side_effect_mismatches)} 处副作用声明不一致")
            if out.late_results:
                parts.append(f"{len(out.late_results)} 个晚到结果")
            out.issues.append("；".join(parts))
