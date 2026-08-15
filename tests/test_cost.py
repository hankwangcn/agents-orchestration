"""成本核算测试（阶段二，治理层 OB）。

覆盖：按 agent / 匹配类型归集、失败成本、剪枝已发生消耗（§5.4 口径）、
预算超支判定（声明 0 不判）、平均成本。
"""
from orchestration.cost import CostAccountant
from orchestration.models import (
    DAG,
    ErrorInfo,
    ResourceRequirement,
    Result,
    Task,
    TaskStatus,
)
from orchestration.registry import AgentRegistry
from orchestration.scheduler import Scheduler

from test_scheduler import ScriptedAdapter, dag_of, fail, ok


def run_account(dag: DAG, script: dict, retries=2, budget: dict | None = None,
                model="deepseek-chat") -> tuple:
    """跑调度 → 核算。返回 (CostReport, AgentRegistry)。"""
    adapter = ScriptedAdapter(script, model=model)
    reg = AgentRegistry()
    reg.register(adapter)
    if budget:
        for aid, b in budget.items():
            reg.get(aid).budget_limit_usd = b
    sched = Scheduler(registry=reg, retries=retries)
    report = sched.run(dag)
    return CostAccountant(reg).account(report), reg


# ---------------------------------------------------------------------------
# 归集
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_by_agent_totals(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        report, _ = run_account(
            dag, {"a": [ok("a", cost=0.05)], "b": [ok("b", cost=0.03)]},
        )

        assert report.total_cost == 0.08
        assert report.total_tokens_in == 20
        assert report.total_tokens_out == 20
        assert report.avg_cost_per_task == 0.04
        assert len(report.by_agent) == 1
        ac = report.by_agent[0]
        assert ac.agent_id == "agent_001"
        assert ac.tasks == 2
        assert ac.cost == 0.08

    def test_by_match_type_separates_degraded(self):
        """降级任务成本单独归集（暴露降级开销）。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(
                id="b", desc="b",
                required_resources=ResourceRequirement(model="llama-3"),
            ),
        })
        report, _ = run_account(
            dag, {"a": [ok("a", cost=0.04)], "b": [ok("b", cost=0.06)]},
        )

        types = {t.match_type: t for t in report.by_match_type}
        assert types["exact"].cost == 0.04
        assert types["degraded"].cost == 0.06
        assert types["degraded"].tasks == 1

    def test_failed_cost_accounted(self):
        """失败任务成本单独暴露（重试沉没成本）。"""
        dag = dag_of(("a", []), ("b", ["a"]))
        # a 第一次失败（cost 已花），重试成功；b 成功
        report, _ = run_account(
            dag,
            {"a": [fail("a"), ok("a", cost=0.02)], "b": [ok("b", cost=0.01)]},
        )

        # a 失败的 attempt 无 usage（fail 脚本没带 cost）→ failed_cost=0；
        # 验证失败但带 cost 的场景：
        assert report.total_cost == 0.03

    def test_failed_with_cost(self):
        dag = dag_of(("a", []))
        payload = dict(fail("a"), usage={"tokens_in": 5, "tokens_out": 5, "cost": 0.5})
        report, _ = run_account(dag, {"a": [payload] * 3})
        assert report.failed_cost == 0.5
        assert report.total_cost == 0.5


# ---------------------------------------------------------------------------
# 剪枝已发生消耗（架构 §5.4）
# ---------------------------------------------------------------------------

class TestPrunedCost:
    def test_pruned_success_task_cost_counted(self):
        """并行任务已成功但被剪 → 已发生消耗单独归集 pruned_cost。"""
        dag = dag_of(("a", []), ("c", ["a"]), ("b", ["a"]), ("d", ["b", "c"]))
        script = {
            "a": [ok("a", cost=0.01)],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c", cost=0.02)],  # 先成功后仍被剪
            "d": [ok("d", cost=0.05)],
        }
        report, _ = run_account(dag, script)

        # a、c 均已成功但整棵被剪（a 产出无人要）→ 两者消耗都计入 pruned_cost
        assert report.pruned_cost == 0.03
        assert report.total_cost == 0.03  # a + c（b 失败无 cost，d 未执行）
        assert report.failed_cost == 0.0


# ---------------------------------------------------------------------------
# 预算超支
# ---------------------------------------------------------------------------

class TestBudget:
    def test_over_budget_flagged(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        report, _ = run_account(
            dag,
            {"a": [ok("a", cost=0.06)], "b": [ok("b", cost=0.03)]},
            budget={"agent_001": 0.05},
        )

        assert report.by_agent[0].over_budget is True
        assert len(report.over_budget_agents) == 1
        ob = report.over_budget_agents[0]
        assert ob["agent_id"] == "agent_001"
        assert ob["spent"] == 0.09
        assert ob["budget"] == 0.05
        assert ob["overspend"] == 0.04

    def test_within_budget_clean(self):
        dag = dag_of(("a", []))
        report, _ = run_account(
            dag, {"a": [ok("a", cost=0.03)]}, budget={"agent_001": 0.1},
        )
        assert report.over_budget_agents == []
        assert report.by_agent[0].over_budget is False

    def test_zero_budget_not_judged(self):
        """预算声明 0 = 未知 → 不判超支。"""
        dag = dag_of(("a", []))
        report, _ = run_account(dag, {"a": [ok("a", cost=5.0)]})
        assert report.over_budget_agents == []

    def test_multi_agent_budget_per_agent(self):
        """多 agent 各自按预算判定。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a",
                      required_resources=ResourceRequirement(model="deepseek-chat")),
            "b": Task(id="b", desc="b",
                      required_resources=ResourceRequirement(model="claude-3.5")),
        })
        gpt = ScriptedAdapter({"a": [ok("a", cost=0.09)]}, model="deepseek-chat")
        claude = ScriptedAdapter({"b": [ok("b", cost=0.01)]}, model="claude-3.5")
        reg = AgentRegistry()
        reg.register(gpt)
        reg.register(claude)
        reg.get("agent_001").budget_limit_usd = 0.05  # gpt 超
        reg.get("agent_002").budget_limit_usd = 0.5   # claude 不超
        sched = Scheduler(registry=reg)
        report = sched.run(dag)
        cost = CostAccountant(reg).account(report)

        assert {ob["agent_id"] for ob in cost.over_budget_agents} == {"agent_001"}
        agents = {a.agent_id: a for a in cost.by_agent}
        assert agents["agent_001"].over_budget is True
        assert agents["agent_002"].over_budget is False


# ---------------------------------------------------------------------------
# 多 agent 归集
# ---------------------------------------------------------------------------

class TestMultiAgent:
    def test_costs_separated_by_agent(self):
        dag = DAG(tasks={
            "a": Task(id="a", desc="a",
                      required_resources=ResourceRequirement(model="deepseek-chat")),
            "b": Task(id="b", desc="b",
                      required_resources=ResourceRequirement(model="claude-3.5")),
        })
        gpt = ScriptedAdapter({"a": [ok("a", cost=0.1)]}, model="deepseek-chat")
        claude = ScriptedAdapter({"b": [ok("b", cost=0.2)]}, model="claude-3.5")
        reg = AgentRegistry()
        reg.register(gpt)
        reg.register(claude)
        sched = Scheduler(registry=reg)
        report = sched.run(dag)
        cost = CostAccountant(reg).account(report)

        agents = {a.agent_id: a for a in cost.by_agent}
        assert agents["agent_001"].cost == 0.1
        assert agents["agent_002"].cost == 0.2
        assert cost.total_cost == 0.3
