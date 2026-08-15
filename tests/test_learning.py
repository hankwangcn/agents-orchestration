"""自我学习规则提取测试（阶段二，学习层）。

覆盖：失败模式 / 降级占比 / 风险分配失败率 / 预算超支 / 剪枝质量 / 正确性断链
六类规则触发与不触发边界（阈值驱动）。
"""
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine
from orchestration.models import (
    DAG,
    ErrorInfo,
    ResourceRequirement,
    Result,
    SideEffects,
    Task,
    TaskStatus,
)
from orchestration.registry import AgentRegistry
from orchestration.scheduler import Scheduler, ScheduleReport

from test_scheduler import ScriptedAdapter, dag_of, fail, ok


def run_audit_cost(dag: DAG, script: dict, retries=2, budget: dict | None = None):
    """跑调度 → 审计 + 成本核算。返回 (AuditReport, CostReport)。"""
    adapter = ScriptedAdapter(script)
    reg = AgentRegistry()
    reg.register(adapter)
    if budget:
        for aid, b in budget.items():
            reg.get(aid).budget_limit_usd = b
    sched = Scheduler(registry=reg, retries=retries)
    report = sched.run(dag)
    return Auditor().audit(report), CostAccountant(reg).account(report)


def learn(audit, cost) -> list:
    return LearningEngine().learn(audit, cost).rules


def rule_ids(rules) -> set:
    return {r.rule_id for r in rules}


# ---------------------------------------------------------------------------
# 正确性断链（最高优先级）
# ---------------------------------------------------------------------------

class TestReconciliationRule:
    def test_missing_result_triggers_rec1(self):
        dag = dag_of(("a", []))
        dag.tasks["a"].status = TaskStatus.SUCCESS  # 无 result
        report = ScheduleReport(dag=dag)
        audit = Auditor().audit(report)
        cost = CostAccountant().account(report)
        rules = learn(audit, cost)
        assert "REC-1" in rule_ids(rules)
        assert rules[0].severity == "high"

    def test_clean_no_rec_rule(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        audit, cost = run_audit_cost(dag, {"a": [ok("a")], "b": [ok("b")]})
        assert "REC-1" not in rule_ids(learn(audit, cost))


# ---------------------------------------------------------------------------
# 失败模式
# ---------------------------------------------------------------------------

class TestFailurePatternRule:
    def test_frequent_code_triggers_rule(self):
        dag = dag_of(("x", []), ("y", []), ("z", []))
        dag.tasks["x"].status = TaskStatus.FAILED
        dag.tasks["x"].result = Result(task_id="x", success=False,
                                       error=ErrorInfo(code="timeout"))
        dag.tasks["y"].status = TaskStatus.FAILED
        dag.tasks["y"].result = Result(task_id="y", success=False,
                                       error=ErrorInfo(code="timeout"))
        dag.tasks["z"].status = TaskStatus.FAILED
        dag.tasks["z"].result = Result(task_id="z", success=False,
                                       error=ErrorInfo(code="timeout"))
        report = ScheduleReport(dag=dag)
        audit = Auditor().audit(report)
        cost = CostAccountant().account(report)
        rules = learn(audit, cost)

        fp = [r for r in rules if r.category == "failure_pattern"]
        assert len(fp) == 1
        assert fp[0].severity == "high"  # count=3 ≥ 3
        assert "timeout" in fp[0].message
        assert fp[0].evidence["task_ids"] == ["x", "y", "z"]

    def test_below_min_not_triggered(self):
        """出现 1 次（< min=2）→ 不触发。"""
        dag = dag_of(("x", []))
        dag.tasks["x"].status = TaskStatus.FAILED
        dag.tasks["x"].result = Result(task_id="x", success=False,
                                       error=ErrorInfo(code="timeout"))
        report = ScheduleReport(dag=dag)
        audit = Auditor().audit(report)
        cost = CostAccountant().account(report)
        rules = learn(audit, cost)
        assert not any(r.category == "failure_pattern" for r in rules)


# ---------------------------------------------------------------------------
# 降级占比
# ---------------------------------------------------------------------------

class TestDegradedRule:
    def test_high_degraded_ratio_triggers(self):
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b",
                      required_resources=ResourceRequirement(model="llama-3")),
            "c": Task(id="c", desc="c",
                      required_resources=ResourceRequirement(model="llama-3")),
        })
        audit, cost = run_audit_cost(dag, {"a": [ok("a")], "b": [ok("b")], "c": [ok("c")]})
        rules = learn(audit, cost)

        deg = [r for r in rules if r.category == "degraded_assignment"]
        assert len(deg) == 1  # 2/3 = 67% ≥ 20%
        assert "降级" in deg[0].message

    def test_low_degraded_ratio_not_triggered(self):
        """6 任务 1 降级（16.7% < 20%）→ 不触发。"""
        dag = DAG(tasks={
            **{f"t{i}": Task(id=f"t{i}", desc=f"t{i}") for i in range(5)},
            "deg": Task(id="deg", desc="deg",
                        required_resources=ResourceRequirement(model="llama-3")),
        })
        script = {f"t{i}": [ok(f"t{i}")] for i in range(5)}
        script["deg"] = [ok("deg")]
        audit, cost = run_audit_cost(dag, script)
        rules = learn(audit, cost)
        assert not any(r.category == "degraded_assignment" for r in rules)


# ---------------------------------------------------------------------------
# 风险分配失败率
# ---------------------------------------------------------------------------

class TestCapabilityRiskRule:
    def test_risky_failure_triggers(self):
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", required_capabilities=["code_review"]),
        })
        # 精确匹配但能力未覆盖 → risk；任务失败 → CAP-1
        audit, cost = run_audit_cost(dag, {"a": [fail("a"), fail("a"), fail("a")]})
        rules = learn(audit, cost)

        cap = [r for r in rules if r.category == "capability_risk"]
        assert len(cap) == 1
        assert cap[0].severity == "high"
        assert cap[0].evidence["failed"] == 1

    def test_risky_success_not_triggered(self):
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", required_capabilities=["code_review"]),
        })
        audit, cost = run_audit_cost(dag, {"a": [ok("a")]})
        rules = learn(audit, cost)
        assert not any(r.category == "capability_risk" for r in rules)


# ---------------------------------------------------------------------------
# 预算超支
# ---------------------------------------------------------------------------

class TestBudgetRule:
    def test_over_budget_triggers(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        audit, cost = run_audit_cost(
            dag, {"a": [ok("a", cost=0.06)], "b": [ok("b", cost=0.03)]},
            budget={"agent_001": 0.05},
        )
        rules = learn(audit, cost)

        bug = [r for r in rules if r.category == "budget_overrun"]
        assert len(bug) == 1
        assert "agent_001" in bug[0].rule_id
        assert "超支" in bug[0].message

    def test_within_budget_no_rule(self):
        dag = dag_of(("a", []))
        audit, cost = run_audit_cost(
            dag, {"a": [ok("a", cost=0.03)]}, budget={"agent_001": 0.1},
        )
        assert not any(r.category == "budget_overrun" for r in learn(audit, cost))


# ---------------------------------------------------------------------------
# 剪枝质量
# ---------------------------------------------------------------------------

class TestPruningRule:
    def test_prune_triggers_rule(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        audit, cost = run_audit_cost(dag, {"a": [fail("a")] * 3})
        rules = learn(audit, cost)

        pru = [r for r in rules if r.category == "pruning_quality"]
        assert len(pru) == 1
        assert pru[0].severity == "high"  # pruned_final=True
        assert pru[0].evidence["pruned"] == 2

    def test_partial_prune_medium_severity(self):
        """final 未被波及 → medium。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b"]), ("e", ["c"]))
        audit, cost = run_audit_cost(dag, {"a": [ok("a")], "b": [fail("b")] * 3,
                                           "c": [ok("c")], "e": [ok("e")]})
        rules = learn(audit, cost)

        pru = [r for r in rules if r.category == "pruning_quality"]
        assert len(pru) == 1
        assert pru[0].severity == "medium"

    def test_no_prune_no_rule(self):
        dag = dag_of(("a", []))
        audit, cost = run_audit_cost(dag, {"a": [ok("a")]})
        assert not any(r.category == "pruning_quality" for r in learn(audit, cost))


# ---------------------------------------------------------------------------
# 完整链路（阶段二闭环演示）
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_governance_pipeline_end_to_end(self):
        """一次失败调度 → 审计 warning → 学习出 3 类规则。"""
        dag = dag_of(("a", []), ("c", ["a"]), ("b", ["a"]), ("d", ["b", "c"]))
        script = {
            "a": [ok("a")],
            "b": [fail("b", "model_timeout"), fail("b", "model_timeout"),
                  fail("b", "model_timeout")],
            "c": [ok("c")],
            "d": [ok("d")],
        }
        audit, cost = run_audit_cost(dag, script)
        rules = learn(audit, cost)
        ids = rule_ids(rules)

        assert audit.verdict == "warning"
        assert cost.pruned_cost == 0.02  # a、c 均已成功但整棵被剪
        # 剪枝质量规则必然触发（失败模式需 ≥2 失败任务，此处仅 b 一个）
        assert any(r.category == "pruning_quality" for r in rules)


# ---------------------------------------------------------------------------
# 中断任务规则（断点恢复）
# ---------------------------------------------------------------------------

class TestInterruptedRule:
    def test_int1_rule_when_interrupted(self):
        """存在中断任务 → INT-1 规则（待人工确认，检查崩溃原因）。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.INTERRUPTED,
                      side_effects=SideEffects.EXTERNAL_API),
        })
        report = ScheduleReport(dag=dag)
        audit = Auditor().audit(report)
        cost = CostAccountant().account(report)
        rules = learn(audit, cost)
        ids = rule_ids(rules)

        assert "INT-1" in ids
        rule = next(r for r in rules if r.rule_id == "INT-1")
        assert rule.evidence["tasks"][0]["task_id"] == "a"

    def test_no_int1_when_clean(self):
        dag = dag_of(("a", []))
        report = run_audit_cost(dag, {"a": [ok("a")]})
        audit, cost = report
        assert "INT-1" not in rule_ids(learn(audit, cost))
