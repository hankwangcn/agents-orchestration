"""阶段三测试：AgentRegistry（注册 / info_request 采集 / 任务分配 / 摘除）。

覆盖：自动 id、三类 scope 采集解析入库、采集失败容错、分配三级策略
（exact 精确匹配 / capability 能力匹配 / degraded 降级默认）、多实例轮询、
部分覆盖 risk 标记、exact 但能力未覆盖 risk 标记、连续失败摘除与恢复、
无可用 agent 抛错。
"""
from typing import Optional

import pytest

from orchestration.adapters.base import AgentAdapter
from orchestration.models import ResourceRequirement, Result, Task
from orchestration.registry import AgentRegistry, INFO_QUESTIONS, RegistryError


# ---------------------------------------------------------------------------
# 测试适配器：run_info 按 scope 返回脚本化的声明
# ---------------------------------------------------------------------------

class InfoAdapter(AgentAdapter):
    """run_info 按脚本返回；run_task/cancel 记录调用不执行。"""

    def __init__(
        self,
        model: str = "deepseek-chat",
        declarations: Optional[dict[str, dict]] = None,
    ):
        super().__init__(model=model)
        self.declarations = declarations or {}
        self.info_calls: list[tuple[str, str, list[str]]] = []
        self.fail_scopes: set[str] = set()

    def run_info(self, scope: str, questions: list[str], request_id: str) -> Result:
        self.info_calls.append((scope, request_id, questions))
        if scope in self.fail_scopes:
            return Result(task_id="", success=False,
                          error={"code": "no_answer", "message": "拒绝回答"})
        out = self.declarations.get(scope, {})
        return Result(task_id="", success=True, output=out)

    def run_task(self, task, request_id, inputs=None):
        return Result(task_id=task.id, success=True, output={})

    def cancel(self, task_id, request_id):
        return Result(task_id=task_id, success=True, output=None)

    def _call_llm(self, messages):
        raise NotImplementedError


def make_task(model: str = "deepseek-chat", caps: Optional[list[str]] = None) -> Task:
    return Task(
        id="t1",
        desc="t1",
        required_resources=ResourceRequirement(model=model),
        required_capabilities=caps or [],
    )


DECL = {
    "capability": {
        "q1": "code_review, data_analysis",
        "q2": "擅长代码审查与数据分析报告",
    },
    "resource": {
        "q1": "5",
        "q2": "60",
        "q3": "2.5",
    },
    "constraint": {
        "q1": "无",
        "q2": "Python, SQL",
    },
}


# ---------------------------------------------------------------------------
# 注册
# ---------------------------------------------------------------------------

class TestRegister:
    def test_auto_id_and_default(self):
        reg = AgentRegistry()
        a1 = InfoAdapter(model="deepseek-chat")
        a2 = InfoAdapter(model="claude-3.5")
        id1 = reg.register(a1)
        id2 = reg.register(a2)

        assert id1 == "agent_001"
        assert id2 == "agent_002"
        # 第一个注册的成为默认降级目标
        assert reg._default_id == id1

    def test_explicit_id_and_dup_rejected(self):
        reg = AgentRegistry()
        reg.register(InfoAdapter(), agent_id="reviewer")
        with pytest.raises(RegistryError):
            reg.register(InfoAdapter(), agent_id="reviewer")

    def test_register_default(self):
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="a")
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="b")
        reg.register_default("b")
        assert reg._default_id == "b"


# ---------------------------------------------------------------------------
# info_request 采集
# ---------------------------------------------------------------------------

class TestCollect:
    def test_collect_all_scopes_populates_declaration(self):
        reg = AgentRegistry()
        aid = reg.register(InfoAdapter(declarations=DECL))
        summary = reg.collect(agent_id=aid)

        assert len(summary) == 3
        assert all(s["ok"] for s in summary)
        agent = reg.get(aid)
        assert agent.capabilities == ["code_review", "data_analysis"]
        assert agent.description == "擅长代码审查与数据分析报告"
        assert agent.max_concurrency == 5
        assert agent.rate_limit_per_min == 60
        assert agent.budget_limit_usd == 2.5
        assert agent.forbidden == []
        assert agent.languages == ["Python", "SQL"]
        assert agent.last_collected_at is not None

    def test_collect_single_scope(self):
        reg = AgentRegistry()
        aid = reg.register(InfoAdapter(declarations=DECL))
        reg.collect(agent_id=aid, scope="resource")
        agent = reg.get(aid)
        assert agent.max_concurrency == 5
        assert agent.capabilities == []  # 未采集的 scope 保持空

    def test_collect_failure_is_isolated(self):
        """单 scope 采集失败不中断，摘要留痕。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        adapter.fail_scopes.add("resource")
        aid = reg.register(adapter)
        summary = reg.collect(agent_id=aid)

        ok_scopes = {s["scope"] for s in summary if s["ok"]}
        fail = [s for s in summary if not s["ok"]][0]
        assert ok_scopes == {"capability", "constraint"}
        assert fail["scope"] == "resource"
        assert "no_answer" in fail["error"]

    def test_collect_no_calls_for_empty_questions_bug(self):
        """采集必须发 info_request（问题集非空、request_id 带 scope）。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        reg.collect(agent_id=aid, scope="capability")

        scope, request_id, questions = adapter.info_calls[0]
        assert scope == "capability"
        assert questions == INFO_QUESTIONS["capability"]
        assert "capability" in request_id

    def test_output_string_fallback(self):
        """output 是裸字符串 → 整段当能力描述兜底。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations={"capability": "翻译, 校对"})
        aid = reg.register(adapter)
        reg.collect(agent_id=aid, scope="capability")
        assert reg.get(aid).capabilities == ["翻译", "校对"]


# ---------------------------------------------------------------------------
# 任务分配：三级策略
# ---------------------------------------------------------------------------

class TestAssign:
    def test_exact_model_match(self):
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt")
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="claude")
        assignment, adapter = reg.assign(make_task(model="deepseek-chat"))

        assert assignment.match_type == "exact"
        assert assignment.agent_id == "gpt"
        assert adapter.model == "deepseek-chat"
        assert assignment.risk is False

    def test_multi_instance_round_robin(self):
        """同 model 多实例轮询分流（多 agent 资源协调）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt_1")
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt_2")
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt_3")

        got = [
            reg.assign(make_task(model="deepseek-chat"))[0].agent_id
            for _ in range(4)
        ]
        assert got == ["gpt_1", "gpt_2", "gpt_3", "gpt_1"]

    def test_capability_match_when_no_exact_model(self):
        """无精确 model → 按能力标签匹配。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="reviewer")
        reg.get("reviewer").capabilities = ["code_review", "data_analysis"]

        assignment, _ = reg.assign(
            make_task(model="deepseek-chat", caps=["code_review"])
        )
        assert assignment.match_type == "capability"
        assert assignment.agent_id == "reviewer"
        assert assignment.risk is False

    def test_partial_capability_marks_risk(self):
        """部分覆盖 → 标记风险（审计重点盯）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="weak")
        reg.get("weak").capabilities = ["data_analysis"]

        assignment, _ = reg.assign(
            make_task(model="deepseek-chat", caps=["code_review", "data_analysis"])
        )
        assert assignment.match_type == "capability"
        assert assignment.risk is True
        assert "部分覆盖" in assignment.reason

    def test_degrade_to_default_with_trace(self):
        """无匹配 → 降级默认通用 LLM，显式留痕（degraded）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="claude")
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="general")
        reg.register_default("general")  # 通用 LLM 显式指定为默认

        assignment, adapter = reg.assign(
            make_task(model="llama-3", caps=["code_review"])
        )
        assert assignment.match_type == "degraded"
        assert assignment.agent_id == "general"
        assert adapter.model == "deepseek-chat"
        assert "降级" in assignment.reason

    def test_exact_but_capability_uncovered_marks_risk(self):
        """精确 model 匹配但能力声明未覆盖任务需求 → risk 标记（不降级）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt")
        reg.get("gpt").capabilities = ["data_analysis"]

        assignment, _ = reg.assign(
            make_task(model="deepseek-chat", caps=["code_review"])
        )
        assert assignment.match_type == "exact"
        assert assignment.risk is True
        assert "未覆盖" in assignment.reason

    def test_no_agent_at_all_raises(self):
        reg = AgentRegistry()
        with pytest.raises(RegistryError):
            reg.assign(make_task())


# ---------------------------------------------------------------------------
# 故障摘除与恢复
# ---------------------------------------------------------------------------

class TestHealth:
    def test_consecutive_failures_trips_agent(self):
        reg = AgentRegistry(max_consecutive_failures=2)
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt")
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="claude")

        for _ in range(2):
            reg.record_failure("gpt")
        assert reg.get("gpt").status == "unavailable"

        # 摘除后不再参与分配 → 降级到默认（claude）
        assignment, _ = reg.assign(make_task(model="deepseek-chat"))
        assert assignment.match_type == "degraded"
        assert assignment.agent_id == "claude"

    def test_success_resets_failure_counter(self):
        reg = AgentRegistry(max_consecutive_failures=3)
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt")
        reg.record_failure("gpt")
        reg.record_failure("gpt")
        reg.record_success("gpt")
        reg.record_failure("gpt")

        assert reg.get("gpt").status == "available"  # 2+1 未达阈值 3

    def test_mark_available_restores(self):
        reg = AgentRegistry(max_consecutive_failures=1)
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt")
        reg.record_failure("gpt")
        assert reg.get("gpt").status == "unavailable"

        reg.mark_available("gpt")
        assignment, _ = reg.assign(make_task(model="deepseek-chat"))
        assert assignment.match_type == "exact"
        assert assignment.agent_id == "gpt"

    def test_default_unavailable_falls_back_to_any_available(self):
        """默认 agent 不可用 → 回退任意可用 agent（不瘫痪）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="general")
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="claude")
        reg.mark_unavailable("general")

        assignment, adapter = reg.assign(make_task(model="llama-3", caps=["x"]))
        assert assignment.match_type == "degraded"
        assert assignment.agent_id == "claude"

    def test_all_unavailable_raises(self):
        """全部 agent 不可用 → 抛错（无任何降级目标）。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="general")
        reg.register(InfoAdapter(model="claude-3.5"), agent_id="claude")
        reg.mark_unavailable("general")
        reg.mark_unavailable("claude")

        with pytest.raises(RegistryError):
            reg.assign(make_task(model="llama-3", caps=["x"]))

    def test_unavailable_excluded_from_round_robin(self):
        """摘除的实例不占轮询槽位。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt_1")
        reg.register(InfoAdapter(model="deepseek-chat"), agent_id="gpt_2")
        reg.mark_unavailable("gpt_1")

        got = [reg.assign(make_task(model="deepseek-chat"))[0].agent_id for _ in range(3)]
        assert got == ["gpt_2", "gpt_2", "gpt_2"]
