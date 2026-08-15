"""结果审计器测试（阶段二，治理层）。

覆盖：正确性对账（状态 vs 结果契约）、缺失/晚到结果、分配审计（降级/风险）、
剪枝审计、副作用交叉校验（协议 §7.5）、错误模式归集、verdict 判定。
"""
import pytest

from orchestration.audit import Auditor
from orchestration.models import (
    DAG,
    ErrorInfo,
    ResourceRequirement,
    Result,
    SideEffects,
    Task,
    TaskStatus,
)
from orchestration.registry import AgentRegistry, Assignment
from orchestration.scheduler import Scheduler, ScheduleReport

from test_scheduler import ScriptedAdapter, dag_of, fail, ok, make_scheduler


# ---------------------------------------------------------------------------
# 构造 helper
# ---------------------------------------------------------------------------

def run_report(dag: DAG, script: dict, retries=2, **reg_kwargs) -> ScheduleReport:
    """跑一次调度拿报告（复用 test_scheduler 的适配器）。"""
    adapter = ScriptedAdapter(script)
    reg = AgentRegistry(**reg_kwargs)
    reg.register(adapter)
    sched = Scheduler(registry=reg, retries=retries)
    return sched.run(dag)


def audit_of(report: ScheduleReport):
    return Auditor().audit(report)


# ---------------------------------------------------------------------------
# 正确性对账
# ---------------------------------------------------------------------------

class TestReconciliation:
    def test_clean_success(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        report = run_report(dag, {"a": [ok("a")], "b": [ok("b")]})
        audit = audit_of(report)

        assert audit.verdict == "ok"
        assert audit.total_tasks == 2
        assert audit.by_status["success"] == 2
        assert audit.success_rate == 1.0
        assert audit.missing_results == []
        assert audit.status_result_mismatches == []
        assert audit.late_results == []

    def test_failed_task_has_result(self):
        """FAILED 任务带 result 属正常对账，不报 mismatch。"""
        dag = dag_of(("a", []))
        report = run_report(dag, {"a": [fail("a", "model_timeout")] * 3})
        audit = audit_of(report)

        assert audit.verdict == "warning"  # 有失败但契约一致 → warning
        assert audit.by_status["failed"] == 1
        assert audit.error_patterns[0]["code"] == "model_timeout"
        assert audit.status_result_mismatches == []

    def test_status_result_contradiction(self):
        """状态 success 但 result.success=false → 矛盾（critical）。"""
        dag = dag_of(("a", []))
        dag.tasks["a"].status = TaskStatus.SUCCESS
        dag.tasks["a"].result = Result(
            task_id="a", success=False,
            error=ErrorInfo(code="x"),
        )
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert audit.verdict == "critical"
        assert len(audit.status_result_mismatches) == 1
        assert audit.status_result_mismatches[0]["task_id"] == "a"

    def test_missing_result_is_critical(self):
        """SUCCESS 终态但缺 result → 断链，critical。"""
        dag = dag_of(("a", []))
        dag.tasks["a"].status = TaskStatus.SUCCESS
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert audit.verdict == "critical"
        assert audit.missing_results == ["a"]

    def test_late_result_on_cancelled(self):
        """已取消任务带 result → 晚到结果标记暴露。"""
        dag = dag_of(("a", []), ("b", ["a"]))
        dag.tasks["b"].status = TaskStatus.CANCELLED
        dag.tasks["b"].result = Result(task_id="b", success=True, output={})
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert audit.late_results == ["b"]
        assert audit.verdict == "warning"


# ---------------------------------------------------------------------------
# 分配审计（阶段三留痕消费）
# ---------------------------------------------------------------------------

class TestAssignmentAudit:
    def test_assignments_by_type(self):
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(
                id="b", desc="b",
                required_resources=ResourceRequirement(model="llama-3"),
            ),
        })
        report = run_report(dag, {"a": [ok("a")], "b": [ok("b")]})
        audit = audit_of(report)

        assert audit.assignments_by_type == {"exact": 1, "degraded": 1}
        assert len(audit.degraded_tasks) == 1
        assert audit.degraded_tasks[0]["task_id"] == "b"

    def test_risky_assignment_tracked_with_result(self):
        """精确匹配但能力未覆盖 → risk，审计合并执行结果。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", required_capabilities=["code_review"]),
        })
        report = run_report(dag, {"a": [ok("a")]})
        audit = audit_of(report)

        assert len(audit.risky_tasks) == 1
        assert audit.risky_tasks[0]["task_id"] == "a"
        assert audit.risky_tasks[0]["task_status"] == "success"


# ---------------------------------------------------------------------------
# 剪枝审计
# ---------------------------------------------------------------------------

class TestPruneAudit:
    def test_prune_summary(self):
        """A→B→C：A 失败重试耗尽 → 剪枝整棵，root_failure 归集。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        report = run_report(dag, {"a": [fail("a", "model_timeout")] * 3})
        audit = audit_of(report)

        assert audit.prune_events == 1
        assert audit.pruned_task_count == 2
        assert audit.pruned_final_events == 1
        assert audit.root_failures[0]["task_id"] == "a"
        assert audit.root_failures[0]["reason"] == "model_timeout"
        assert audit.root_failures[0]["retries"] == 2


# ---------------------------------------------------------------------------
# 副作用交叉校验（协议 §7.5）
# ---------------------------------------------------------------------------

class TestSideEffectAudit:
    def test_declared_none_but_reported(self):
        """声明 none 但报告了副作用 → 隐藏副作用风险。"""
        dag = dag_of(("a", []))
        dag.tasks["a"].result = Result(
            task_id="a", success=True, output={},
            side_effect_report="已提交外部订单 #A88",
        )
        dag.tasks["a"].status = TaskStatus.SUCCESS
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert len(audit.side_effect_mismatches) == 1
        m = audit.side_effect_mismatches[0]
        assert m["task_id"] == "a"
        assert m["declared"] == "none"
        assert "隐藏副作用" in m["detail"]

    def test_declared_side_effect_but_not_reported(self):
        """声明有副作用但未报告 → 无法对账。"""
        dag = dag_of(("a", []))
        dag.tasks["a"].side_effects = SideEffects.EXTERNAL_API
        dag.tasks["a"].result = Result(task_id="a", success=True, output={})
        dag.tasks["a"].status = TaskStatus.SUCCESS
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert len(audit.side_effect_mismatches) == 1
        assert audit.side_effect_mismatches[0]["declared"] == "external_api"

    def test_matching_side_effects_clean(self):
        """声明与报告一致 → 不报。"""
        dag = dag_of(("a", []))
        dag.tasks["a"].side_effects = SideEffects.FILE_WRITE
        dag.tasks["a"].result = Result(
            task_id="a", success=True, output={},
            side_effect_report="已写入 /tmp/x.json",
        )
        dag.tasks["a"].status = TaskStatus.SUCCESS
        report = ScheduleReport(dag=dag)
        audit = audit_of(report)

        assert audit.side_effect_mismatches == []


# ---------------------------------------------------------------------------
# 错误模式归集
# ---------------------------------------------------------------------------

class TestErrorPatterns:
    def test_pattern_counts_and_order(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        # a 失败两次不同错误会怎样？同一任务最终只留一个 result；
        # 用三个独立失败任务测归集
        dag2 = dag_of(("x", []), ("y", []), ("z", []))
        dag2.tasks["x"].status = TaskStatus.FAILED
        dag2.tasks["x"].result = Result(task_id="x", success=False,
                                        error=ErrorInfo(code="timeout"))
        dag2.tasks["y"].status = TaskStatus.FAILED
        dag2.tasks["y"].result = Result(task_id="y", success=False,
                                        error=ErrorInfo(code="timeout"))
        dag2.tasks["z"].status = TaskStatus.FAILED
        dag2.tasks["z"].result = Result(task_id="z", success=False,
                                        error=ErrorInfo(code="parse_error"))
        report = ScheduleReport(dag=dag2)
        audit = audit_of(report)

        assert audit.error_patterns[0]["code"] == "timeout"
        assert audit.error_patterns[0]["count"] == 2
        assert audit.error_patterns[0]["task_ids"] == ["x", "y"]
        assert audit.error_patterns[1]["code"] == "parse_error"

    def test_no_failures_no_patterns(self):
        dag = dag_of(("a", []))
        report = run_report(dag, {"a": [ok("a")]})
        audit = audit_of(report)
        assert audit.error_patterns == []


# ---------------------------------------------------------------------------
# verdict 判定
# ---------------------------------------------------------------------------

class TestVerdict:
    def test_warning_when_issues_but_contract_clean(self):
        """有剪枝/降级/风险但契约一致 → warning 而非 critical。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        report = run_report(dag, {"a": [fail("a")] * 3})
        audit = audit_of(report)

        assert audit.verdict == "warning"
        assert any("剪枝" in i for i in audit.issues)

    def test_ok_when_pristine(self):
        dag = dag_of(("a", []))
        report = run_report(dag, {"a": [ok("a")]})
        audit = audit_of(report)
        assert audit.verdict == "ok"
        assert audit.issues == []
