"""调度器与 DAG 图算法测试（架构 §5 失败处理是核心差异化）。

覆盖：拓扑就绪、后代/反向可达、菱形剪枝（并行但需汇合的任务）、
双分支独立交付（多 final 时只剪失败分支）、全链失败（整棵取消）、
完整调度运行（输入传递 + 成本核算 + 状态机）。
"""
from typing import Optional

from orchestration.adapters.base import AgentAdapter
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


def make_scheduler(*adapters, retries=2):
    """registry 驱动的调度器构造 helper。"""
    reg = AgentRegistry()
    for a in adapters:
        reg.register(a)
    return Scheduler(registry=reg, retries=retries), reg


# ---------------------------------------------------------------------------
# 测试适配器：按脚本返回结果
# ---------------------------------------------------------------------------

class ScriptedAdapter(AgentAdapter):
    """script: task_id -> [Result|dict 序列]，每次调用消费一个。"""

    def __init__(self, script: dict[str, list[dict]], model="deepseek-chat"):
        super().__init__(model=model)
        self.script = {k: list(v) for k, v in script.items()}
        self.calls: list[tuple[str, str, dict]] = []  # (task_id, request_id, inputs)
        self.cancelled: list[str] = []

    def run_task(self, task: Task, request_id: str, inputs: Optional[dict] = None) -> Result:
        self.calls.append((task.id, request_id, inputs or {}))
        payload = self.script.get(task.id, [{}]).pop(0)
        if isinstance(payload, Result):
            return payload
        merged = dict(payload)
        merged.setdefault("task_id", task.id)  # 脚本可不带 task_id
        return Result(**merged)

    def cancel(self, task_id: str, request_id: str) -> Result:
        self.cancelled.append(task_id)
        return Result(task_id=task_id, success=True, output=None)

    def _call_llm(self, messages: list[dict]) -> str:
        raise NotImplementedError


def ok(task_id: str, output=None, cost: float = 0.01) -> dict:
    return {"task_id": task_id, "success": True, "output": output or {},
            "usage": {"tokens_in": 10, "tokens_out": 10, "cost": cost}}


def fail(task_id: str, code: str = "model_error") -> dict:
    return {"task_id": task_id, "success": False,
            "error": {"code": code, "message": "failed"}}


def dag_of(*specs: tuple[str, list[str]]) -> DAG:
    """specs: (task_id, deps) 快速构造 DAG。"""
    return DAG(tasks={tid: Task(id=tid, desc=tid, deps=list(deps)) for tid, deps in specs})


# ---------------------------------------------------------------------------
# 图基础
# ---------------------------------------------------------------------------

class TestGraphAlgos:
    def test_ready_tasks_topological(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]))
        assert set(dag.ready_tasks()) == {"a"}
        dag.tasks["a"].status = TaskStatus.SUCCESS
        assert set(dag.ready_tasks()) == {"b", "c"}

    def test_ready_waits_for_deps(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        dag.tasks["a"].status = TaskStatus.RUNNING
        assert dag.ready_tasks() == []

    def test_descendants(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]), ("d", ["a"]))
        assert dag.descendants("a") == {"b", "c", "d"}
        assert dag.descendants("b") == {"c"}

    def test_final_tasks(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]))
        assert dag.final_tasks() == {"b", "c"}

    def test_reverse_reachable(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        assert dag.reverse_reachable({"c"}) == {"c", "b", "a"}
        assert dag.reverse_reachable({"b"}) == {"b", "a"}


# ---------------------------------------------------------------------------
# 剪枝算法（架构 §5.2）——核心
# ---------------------------------------------------------------------------

class TestPrune:
    def test_simple_chain_full_cancel(self):
        """A→B→C(final)：A 失败 → 整棵取消。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["b"]))
        dag.tasks["a"].status = TaskStatus.FAILED
        dag.tasks["a"].result = Result(task_id="a", success=False,
                                       error=ErrorInfo(code="model_timeout"), retries=2)

        report = dag.prune_after_failure("a")

        assert report.pruned_final is True
        assert dag.tasks["b"].status == TaskStatus.CANCELLED
        assert dag.tasks["c"].status == TaskStatus.CANCELLED
        assert report.root_failure["reason"] == "model_timeout"
        assert report.root_failure["retries"] == 2
        reasons = {p["prune_reason"] for p in report.pruned}
        assert reasons == {"downstream_chain"}

    def test_diamond_prunes_parallel_merge(self):
        """A→{B,C}→D(final)：B 失败 → D 断 → C 的产出无人要 → 全部取消。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        dag.tasks["b"].status = TaskStatus.FAILED
        dag.tasks["b"].result = Result(task_id="b", success=False, error=ErrorInfo(code="x"))

        report = dag.prune_after_failure("b")

        assert report.pruned_final is True
        assert dag.tasks["c"].status == TaskStatus.CANCELLED  # 并行但需汇合 → 剪
        assert dag.tasks["d"].status == TaskStatus.CANCELLED  # 下游
        assert dag.tasks["a"].status == TaskStatus.CANCELLED  # 反向走不到根
        assert dag.tasks["b"].status == TaskStatus.FAILED

    def test_independent_branches_keep_alive(self):
        """A→{B→D(final), C→E(final)}：B 失败 → 只剪 B/D，C→E 分支保留。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b"]), ("e", ["c"]))
        dag.tasks["b"].status = TaskStatus.FAILED
        dag.tasks["b"].result = Result(task_id="b", success=False, error=ErrorInfo(code="x"))

        report = dag.prune_after_failure("b")

        assert report.pruned_final is False
        pruned_ids = {p["task_id"] for p in report.pruned}
        assert pruned_ids == {"d"}  # B 的后代
        assert dag.tasks["c"].status == TaskStatus.PENDING  # 存活分支不动
        assert dag.tasks["e"].status == TaskStatus.PENDING
        assert dag.tasks["a"].status == TaskStatus.PENDING  # 仍被 E 反向可达
        assert all(p["prune_reason"] == "downstream_chain" for p in report.pruned)

    def test_skip_success_but_pruned_task(self):
        """并行任务已成功，但最终被剪 → 状态 CANCELLED，产出作废（§5.4 成本照记）。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        dag.tasks["a"].status = TaskStatus.SUCCESS
        dag.tasks["c"].status = TaskStatus.SUCCESS  # 已跑完
        dag.tasks["b"].status = TaskStatus.FAILED
        dag.tasks["b"].result = Result(task_id="b", success=False, error=ErrorInfo(code="x"))

        report = dag.prune_after_failure("b")

        assert dag.tasks["c"].status == TaskStatus.CANCELLED
        assert report.pruned_final is True
        states = {p["task_id"]: p["state_at_cancel"] for p in report.pruned}
        assert states["c"] == "success"  # 记录剪枝时刻状态，供成本核算


# ---------------------------------------------------------------------------
# 完整调度运行
# ---------------------------------------------------------------------------

class TestSchedulerRun:
    def test_success_chain_passes_inputs(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        adapter = ScriptedAdapter({"a": [ok("a", {"n": 42})],
                                   "b": [ok("b")]})
        sched, _ = make_scheduler(adapter)
        report = sched.run(dag)

        assert report.final_status == "success"
        assert report.total_cost == 0.02
        # 输入传递：B 收到 A 的结果（数据流依赖）
        b_call = [c for c in adapter.calls if c[0] == "b"][0]
        assert b_call[2]["a"]["n"] == 42

    def test_retry_then_success(self):
        dag = dag_of(("a", []))
        adapter = ScriptedAdapter({"a": [fail("a"), ok("a")]})
        sched, _ = make_scheduler(adapter, retries=2)
        report = sched.run(dag)

        assert report.final_status == "success"
        assert dag.tasks["a"].result.retries == 1
        assert len(adapter.calls) == 2

    def test_failure_exhausts_retries_then_prunes(self):
        """A→{B,C}→D：B 连续失败 → 剪枝整棵，报告生成。

        c 先于 b 派发（插入序），先成功后仍被剪——已发生消耗计入成本。
        """
        dag = dag_of(("a", []), ("c", ["a"]), ("b", ["a"]), ("d", ["b", "c"]))
        adapter = ScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c")],
            "d": [ok("d")],
        })
        sched, _ = make_scheduler(adapter, retries=2)
        report = sched.run(dag)

        assert report.final_status == "failed"
        assert len(report.prune_reports) == 1
        pr = report.prune_reports[0]
        assert pr.root_failure["task_id"] == "b"
        assert pr.root_failure["retries"] == 2
        assert dag.tasks["c"].status == TaskStatus.CANCELLED
        assert dag.tasks["d"].status == TaskStatus.CANCELLED
        assert dag.tasks["b"].status == TaskStatus.FAILED
        # 成本：a、c 已发生（c 先成功后被剪），计入
        assert report.total_cost == 0.02

    def test_branch_failure_keeps_independent_branch(self):
        """双分支：B 分支失败剪枝，C→E 分支照常完成 → partial。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b"]), ("e", ["c"]))
        adapter = ScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c", {"v": 1})],
            "e": [ok("e")],
        })
        sched, _ = make_scheduler(adapter, retries=2)
        report = sched.run(dag)

        assert report.final_status == "partial"
        assert dag.tasks["c"].status == TaskStatus.SUCCESS
        assert dag.tasks["e"].status == TaskStatus.SUCCESS
        assert dag.tasks["d"].status == TaskStatus.CANCELLED
        pruned_ids = {p["task_id"] for p in report.prune_reports[0].pruned}
        assert pruned_ids == {"d"}

    def test_adapter_exception_counts_as_failure(self):
        """适配器层异常（网络）→ 视为失败，重试后成功。"""
        dag = dag_of(("a", []))

        class FlakyAdapter(ScriptedAdapter):
            def __init__(self):
                super().__init__({})
                self._n = 0

            def run_task(self, task, request_id, inputs=None):
                self._n += 1
                if self._n == 1:
                    raise ConnectionError("network down")
                return Result(task_id=task.id, success=True, output={})

        adapter = FlakyAdapter()
        sched, _ = make_scheduler(adapter, retries=1)
        report = sched.run(dag)

        assert report.final_status == "success"
        assert dag.tasks["a"].result.retries == 1


# ---------------------------------------------------------------------------
# 阶段三：任务分配 × 调度器集成（留痕 / 降级 / 摘除联动）
# ---------------------------------------------------------------------------

class TestSchedulerAssignments:
    def test_assignments_recorded_exact(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        adapter = ScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, _ = make_scheduler(adapter)
        report = sched.run(dag)

        assert len(report.assignments) == 2
        assert {x.task_id for x in report.assignments} == {"a", "b"}
        assert all(x.match_type == "exact" for x in report.assignments)
        assert report.assignments[0].agent_id == "agent_001"

    def test_degraded_assignment_recorded(self):
        """task.model 未注册 → 降级默认通用 LLM → 留痕 degraded。"""
        dag = DAG(tasks={
            "a": Task(
                id="a", desc="a",
                required_resources=ResourceRequirement(model="llama-3"),
            ),
        })
        adapter = ScriptedAdapter({"a": [ok("a")]})  # deepseek-chat，成为默认
        sched, _ = make_scheduler(adapter)
        report = sched.run(dag)

        a = report.assignments[0]
        assert a.match_type == "degraded"
        assert a.agent_id == "agent_001"
        assert "降级" in a.reason
        assert report.final_status == "success"

    def test_capability_assignment_through_scheduler(self):
        """无精确 model → 能力匹配，分配记录 capability。"""
        dag = DAG(tasks={
            "a": Task(
                id="a", desc="a",
                required_resources=ResourceRequirement(model="unknown-3"),
                required_capabilities=["code_review"],
            ),
        })
        gpt = ScriptedAdapter({"a": [ok("a")]}, model="deepseek-chat")
        claude = ScriptedAdapter({}, model="claude-3.5")
        sched, reg = make_scheduler(gpt, claude)
        reg.get("agent_002").capabilities = ["code_review"]

        report = sched.run(dag)
        a = report.assignments[0]
        assert a.match_type == "capability"
        assert a.agent_id == "agent_002"
        assert a.risk is False

    def test_exact_but_uncovered_capability_marks_risk(self):
        """精确 model 但能力声明未覆盖需求 → risk=True 留痕。"""
        dag = DAG(tasks={
            "a": Task(
                id="a", desc="a",
                required_capabilities=["code_review"],
            ),
        })
        adapter = ScriptedAdapter({"a": [ok("a")]})  # deepseek-chat 无能力声明
        sched, _ = make_scheduler(adapter)
        report = sched.run(dag)

        a = report.assignments[0]
        assert a.match_type == "exact"
        assert a.risk is True

    def test_failure_trips_agent_other_agent_untouched(self):
        """A(deepseek-chat) 任务失败 → 摘除（阈值1）；B(claude) 独立交付不受影响。"""
        dag = DAG(tasks={
            "a": Task(
                id="a", desc="a",
                required_resources=ResourceRequirement(model="deepseek-chat"),
            ),
            "b": Task(
                id="b", desc="b",
                required_resources=ResourceRequirement(model="claude-3.5"),
            ),
        })
        gpt = ScriptedAdapter(
            {"a": [fail("a"), fail("a"), fail("a")]}, model="deepseek-chat"
        )
        claude = ScriptedAdapter({"b": [ok("b")]}, model="claude-3.5")
        reg = AgentRegistry(max_consecutive_failures=1)  # 一次任务失败即摘除
        reg.register(gpt)
        reg.register(claude)
        sched = Scheduler(registry=reg, retries=2)
        report = sched.run(dag)

        assert reg.get("agent_001").status == "unavailable"  # gpt 摘除
        assert reg.get("agent_002").status == "available"
        assert dag.tasks["b"].status == TaskStatus.SUCCESS
        b_assign = [x for x in report.assignments if x.task_id == "b"][0]
        assert b_assign.match_type == "exact"
        assert b_assign.agent_id == "agent_002"
        assert report.final_status == "partial"
