"""阶段三测试：AgentRegistry（注册 / info_request 采集 / 任务分配 / 摘除）。

覆盖：自动 id、三类 scope 采集解析入库、采集失败容错、分配三级策略
（exact 精确匹配 / capability 能力匹配 / degraded 降级默认）、多实例轮询、
部分覆盖 risk 标记、exact 但能力未覆盖 risk 标记、连续失败摘除与恢复、
无可用 agent 抛错。
"""
from typing import Optional

import asyncio
import time

import pytest

from orchestration.adapters.base import AgentAdapter
from orchestration.agent_pool import AgentPool
from orchestration.allocator import Allocator
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


class AsyncInfoAdapter(InfoAdapter):
    """异步采集路径（决策点校验 avalidate_before_dispatch 用）。"""

    async def arun_info(self, scope: str, questions: list[str],
                        request_id: str) -> Result:
        return self.run_info(scope, questions, request_id)


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

    def test_constraint_time_window_text_excluded_from_forbidden(self):
        """q1 中误入的时间窗文本被剔除（time_windows 字段已废弃），
        仅真实禁忌项入 forbidden。"""
        decl = {
            "constraint": {
                "q1": "工作时间 9点~18点；禁止访问外网; 输出必须是JSON",
                "q2": "Python, SQL",
            },
        }
        reg = AgentRegistry()
        aid = reg.register(InfoAdapter(declarations=decl))
        reg.collect(agent_id=aid, scope="constraint")
        agent = reg.get(aid)
        assert agent.forbidden == ["禁止访问外网", "输出必须是JSON"]
        assert agent.languages == ["Python", "SQL"]
        assert not hasattr(agent, "time_windows")  # 字段已删除

    def test_constraint_none_and_blank_excluded(self):
        """『无』（含空白变体）与空回答不计入 forbidden。"""
        for raw in ("无", " 无 "):
            reg = AgentRegistry()
            aid = reg.register(InfoAdapter(declarations={"constraint": {"q1": raw}}))
            reg.collect(agent_id=aid, scope="constraint")
            assert reg.get(aid).forbidden == []

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
# 刷新策略：TTL 惰性刷新 + 决策点校验
# ---------------------------------------------------------------------------

class TestRefresh:
    def test_ensure_fresh_skips_fresh_agent(self):
        """刚采集过 → TTL 内 → 池级刷新零网络开销。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        reg.collect(agent_id=aid)
        before = len(adapter.info_calls)
        assert reg.ensure_fresh() == []
        assert len(adapter.info_calls) == before

    def test_ensure_fresh_refreshes_stale_agent(self):
        """超过 TTL → 重新采集全部三类。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        reg.collect(agent_id=aid)
        reg.get(aid).last_collected_at = "2000-01-01T00:00:00+00:00"
        before = len(adapter.info_calls)
        summary = reg.ensure_fresh()
        assert {s["scope"] for s in summary} == {"capability", "resource", "constraint"}
        assert len(adapter.info_calls) == before + 3

    def test_validate_before_dispatch_volatile_scopes_only(self):
        """决策点只复核易变维度（resource/constraint），不问 capability。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        reg.validate_before_dispatch(aid)
        assert [c[0] for c in adapter.info_calls] == ["resource", "constraint"]

    def test_validate_on_dispatch_can_be_disabled(self):
        reg = AgentRegistry(validate_on_dispatch=False)
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        assert reg.validate_before_dispatch(aid) == []
        assert adapter.info_calls == []

    def test_validate_failure_keeps_last_known_values(self):
        """校验失败不阻塞派发：保留上次已知画像。"""
        reg = AgentRegistry()
        adapter = InfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        reg.collect(agent_id=aid)
        adapter.fail_scopes |= {"resource", "constraint"}
        reg.validate_before_dispatch(aid)
        agent = reg.get(aid)
        assert agent.max_concurrency == 5
        assert agent.languages == ["Python", "SQL"]

    def test_avalidate_before_dispatch_async(self):
        """异步决策点校验（AsyncScheduler 路径）。"""
        reg = AgentRegistry()
        adapter = AsyncInfoAdapter(declarations=DECL)
        aid = reg.register(adapter)
        summary = asyncio.run(reg.avalidate_before_dispatch(aid))
        assert {s["scope"] for s in summary} == {"resource", "constraint"}


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


# ---------------------------------------------------------------------------
# from_config：配置驱动批量注册（N 个 agent 的场景）
# ---------------------------------------------------------------------------

class MiniAdapter(AgentAdapter):
    """极简进程内 adapter：from_config 测试用（避免 HTTP/API key 依赖）。"""

    def __init__(self, model: str = "deepseek-chat"):
        super().__init__(model=model)

    def _call_llm(self, messages):
        return "{}"

    def run_info(self, scope, questions, request_id):
        return Result(task_id="", success=True, output={})


def _factory(entry):
    return MiniAdapter(entry.get("model", "deepseek-chat"))


class TestFromConfig:
    def test_dict_config(self):
        """dict 配置批量注册：agent_id / model / 降级目标 / 摘除阈值。"""
        reg = AgentRegistry.from_config(
            {
                "max_consecutive_failures": 5,
                "default_agent": "translator",
                "agents": [
                    {"agent_id": "translator", "model": "m1"},
                    {"agent_id": "coder", "model": "m2"},
                ],
            },
            adapter_factory=_factory,
        )
        assert set(reg.agents) == {"translator", "coder"}
        assert reg.get("translator").model == "m1"
        assert reg.max_consecutive_failures == 5
        # 降级目标（degraded 分配落点）
        task = make_task(model="unknown-model")
        assignment, _ = reg.assign(task)
        assert assignment.agent_id == "translator"
        assert assignment.match_type == "degraded"

    def test_default_agent_auto_first(self):
        """未指定 default_agent → 第一个注册的作为降级目标。"""
        reg = AgentRegistry.from_config(
            {"agents": [{"agent_id": "a", "model": "m1"},
                        {"agent_id": "b", "model": "m2"}]},
            adapter_factory=_factory,
        )
        task = make_task(model="nope")
        assert reg.assign(task)[0].agent_id == "a"

    def test_yaml_file(self, tmp_path, monkeypatch):
        """YAML 文件注册：api_key_env 从环境变量取。"""
        monkeypatch.setenv("TRANS_KEY", "sk-env-1")
        cfg = tmp_path / "agents.yaml"
        cfg.write_text(
            "max_consecutive_failures: 4\n"
            "default_agent: translator\n"
            "agents:\n"
            "  - agent_id: translator\n"
            "    base_url: http://a:8000/v1\n"
            "    model: m1\n"
            "    api_key_env: TRANS_KEY\n"
            "  - agent_id: coder\n"
            "    base_url: http://b:8000/v1\n"
            "    model: m2\n"
            "    api_key: sk-2\n",
            encoding="utf-8",
        )
        calls: list[dict] = []

        class FakeDeepSeek:
            def __init__(self, model="deepseek-chat", base_url="",
                         api_key=None, template_mode="full"):
                self.model = model
                calls.append(dict(model=model, base_url=base_url,
                                  api_key=api_key, template_mode=template_mode))

        import orchestration.adapters.deepseek as ds_mod
        monkeypatch.setattr(ds_mod, "DeepSeekAdapter", FakeDeepSeek)
        reg = AgentRegistry.from_config(str(cfg))

        assert set(reg.agents) == {"translator", "coder"}
        assert reg.max_consecutive_failures == 4
        by_id = {c["model"]: c for c in calls}
        assert calls[0]["base_url"] == "http://a:8000/v1"
        assert calls[0]["api_key"] == "sk-env-1"     # api_key_env 优先环境变量
        assert calls[1]["api_key"] == "sk-2"         # 显式 api_key
        assert calls[0]["template_mode"] == "full"

    def test_json_file(self, tmp_path):
        cfg = tmp_path / "agents.json"
        cfg.write_text(
            '{"agents": [{"agent_id": "x", "model": "m1"}]}', encoding="utf-8")
        reg = AgentRegistry.from_config(str(cfg), adapter_factory=_factory)
        assert set(reg.agents) == {"x"}

    def test_inprocess_adapter_factory(self):
        """自定义工厂：进程内函数也走 from_config（传输可换）。"""
        from orchestration.adapters.inprocess import InProcessAdapter

        def fn(messages):
            return {"success": True, "task_id": "?"}

        reg = AgentRegistry.from_config(
            {"agents": [{"agent_id": "local", "model": "local-1"}]},
            adapter_factory=lambda e: InProcessAdapter(fn, model=e["model"]),
        )
        adapter = reg.get_adapter("local")
        assert isinstance(adapter, InProcessAdapter)
        assert adapter.model == "local-1"

    def test_invalid_extension(self, tmp_path):
        with pytest.raises(RegistryError):
            AgentRegistry.from_config(str(tmp_path / "agents.txt"))

    def test_missing_yaml_dep(self, tmp_path, monkeypatch):
        """无 pyyaml 时给出可操作报错（不裸 ImportError）。"""
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *a, **kw):
            if name == "yaml":
                raise ImportError("No module named 'yaml'")
            return real_import(name, *a, **kw)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        cfg = tmp_path / "agents.yaml"
        cfg.write_text("agents: []", encoding="utf-8")
        with pytest.raises(RegistryError, match="pyyaml"):
            AgentRegistry.from_config(str(cfg))


# ---------------------------------------------------------------------------
# 层职责切分（本次详细设计）：规划层 AgentPool × 调度层 Allocator
# ---------------------------------------------------------------------------

class TestLayerSplit:
    """registry 按六层架构切成两半，AgentRegistry 仅作零逻辑门面。

    - 规划层「资源统计器」= AgentPool：注册 / 采集 / 声明 / 刷新 / 画像
    - 调度层「资源协调器」= Allocator：三级分配 / 轮询 / 摘除
    """

    def test_pool_is_planning_only(self):
        """AgentPool 不做分配与摘除（那是调度层的职责）。"""
        pool = AgentPool()
        assert not hasattr(pool, "assign")
        assert not hasattr(pool, "record_failure")
        assert not hasattr(pool, "mark_unavailable")

    def test_allocator_is_scheduling_only(self):
        """Allocator 不采集、不刷新（那是规划层的职责）。"""
        alloc = Allocator(AgentPool())
        assert not hasattr(alloc, "collect")
        assert not hasattr(alloc, "ensure_fresh")
        assert not hasattr(alloc, "validate_before_dispatch")

    def test_facade_exposes_both_layers(self):
        reg = AgentRegistry()
        assert isinstance(reg.pool, AgentPool)
        assert isinstance(reg.allocator, Allocator)
        # 门面属性直通层内实例
        reg.collect_ttl_seconds = 42.0
        assert reg.pool.collect_ttl_seconds == 42.0
        reg.validate_on_dispatch = False
        assert reg.pool.validate_on_dispatch is False
        reg.max_consecutive_failures = 7
        assert reg.allocator.max_consecutive_failures == 7

    def test_facade_delegates_registration_to_pool(self):
        """经门面注册 → 池内可见（同一份档案，不是副本）。"""
        reg = AgentRegistry()
        aid = reg.register(InfoAdapter(), agent_id="a1")
        assert reg.pool.agents["a1"] is reg.agents[aid]
        assert reg.pool.get("a1").agent_id == "a1"

    def test_facade_delegates_assignment_to_allocator(self):
        """经门面分配 → 与直接调 Allocator 结果一致。"""
        reg = AgentRegistry()
        reg.register(InfoAdapter(model="m"), agent_id="a1")
        reg.collect("a1")
        assignment, adapter = reg.assign(make_task(model="m"))
        direct, _ = reg.allocator.assign(make_task(model="m"))
        assert assignment.match_type == "exact"
        assert assignment.agent_id == "a1"
        assert direct.agent_id == "a1"
        assert adapter is reg.pool.get_adapter("a1")

    def test_failure_removal_is_allocator_state(self):
        """摘除写的是池里同一个档案对象（层内共享，非拷贝）。"""
        reg = AgentRegistry(max_consecutive_failures=1)
        reg.register(InfoAdapter(), agent_id="a1")
        reg.record_failure("a1")
        assert reg.pool.get("a1").status == "unavailable"
        reg.mark_available("a1")
        assert reg.pool.get("a1").status == "available"

    def test_allocator_works_against_bare_pool(self):
        """Allocator 可直接依赖 AgentPool 使用（层间只靠画像接口耦合）。"""
        pool = AgentPool()
        pool.register(InfoAdapter(model="m", declarations=DECL), agent_id="a1")
        pool.collect("a1")
        alloc = Allocator(pool, max_consecutive_failures=2)
        assignment, _ = alloc.assign(make_task(model="m", caps=["code_review"]))
        assert assignment.match_type == "exact"
        assert assignment.risk is False
        assert pool.get("a1").capabilities == ["code_review", "data_analysis"]

    def test_no_assignment_in_pool_module(self):
        """模块归属断言：分配逻辑只存在于调度层模块。"""
        import orchestration.agent_pool as pool_mod
        import orchestration.allocator as alloc_mod

        assert not hasattr(pool_mod.AgentPool, "assign")
        assert hasattr(alloc_mod.Allocator, "assign")

    def test_registry_reexports_for_compat(self):
        """旧导入路径保持可用（层间切分不破坏既有调用点）。"""
        from orchestration.registry import (  # noqa: F401
            Assignment,
            INFO_QUESTIONS,
            RegisteredAgent,
            VOLATILE_SCOPES,
        )
        assert INFO_QUESTIONS is not None
        assert Assignment is not None
        assert RegisteredAgent is not None
        assert VOLATILE_SCOPES == ("resource", "constraint")


# ---------------------------------------------------------------------------
# info 采集框架侧超时（执行层详细设计缺口 b；与任务执行 #34 对称）
# ---------------------------------------------------------------------------

class SlowInfoAdapter(InfoAdapter):
    """run_info 卡死（sleep）——验证采集受框架侧 wall-clock 上限约束。"""

    def __init__(self, delay: float = 0.5, **kw):
        super().__init__(**kw)
        self.delay = delay

    def run_info(self, scope, questions, request_id):
        self.info_calls.append((scope, request_id, questions))
        time.sleep(self.delay)
        return Result(task_id="", success=True, output={})


class SlowAsyncInfoAdapter(SlowInfoAdapter):
    async def arun_info(self, scope, questions, request_id):
        self.info_calls.append((scope, request_id, questions))
        await asyncio.sleep(self.delay)
        return Result(task_id="", success=True, output={})


class ToggleInfoAdapter(InfoAdapter):
    """先正常应答；置 slow=True 后卡死——验证超时保留上次已知画像。"""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.slow = False

    def run_info(self, scope, questions, request_id):
        if self.slow:
            time.sleep(0.5)
            return Result(task_id="", success=True, output={})
        return super().run_info(scope, questions, request_id)


class TestInfoTimeout:
    def test_sync_collect_bounded(self):
        """采集卡死 → 到点即返（不再无封顶拖住调用方）。"""
        reg = AgentRegistry(info_timeout_seconds=0.05)
        reg.register(SlowInfoAdapter(delay=0.5, declarations=DECL), agent_id="slow")
        t0 = time.monotonic()
        summary = reg.collect()
        assert time.monotonic() - t0 < 0.4
        assert len(summary) == 3
        assert all(not s["ok"] for s in summary)
        assert all("超时" in s["error"] for s in summary)

    def test_ensure_fresh_bounded(self):
        reg = AgentRegistry(info_timeout_seconds=0.05)
        reg.register(SlowInfoAdapter(delay=0.5, declarations=DECL), agent_id="slow")
        t0 = time.monotonic()
        summary = reg.ensure_fresh()
        assert time.monotonic() - t0 < 0.4
        assert len(summary) == 3 and all(not s["ok"] for s in summary)

    def test_async_collect_bounded(self):
        reg = AgentRegistry(info_timeout_seconds=0.05)
        aid = reg.register(
            SlowAsyncInfoAdapter(delay=0.5, declarations=DECL), agent_id="slow"
        )
        summary = asyncio.run(reg.avalidate_before_dispatch(aid))
        assert {s["scope"] for s in summary} == {"resource", "constraint"}
        assert all(not s["ok"] and "超时" in s["error"] for s in summary)

    def test_sync_timeout_keeps_last_known(self):
        """超时按单点失败处理：保留上次已知画像，不阻塞派发。"""
        reg = AgentRegistry(info_timeout_seconds=0.05)
        adapter = ToggleInfoAdapter(declarations=DECL)
        aid = reg.register(adapter, agent_id="a")
        reg.collect("a")
        assert reg.get(aid).max_concurrency == 5
        adapter.slow = True
        summary = reg.collect("a")
        assert all(not s["ok"] for s in summary)
        assert reg.get(aid).max_concurrency == 5  # 上次画像保留

    def test_timeout_disabled(self):
        """info_timeout_seconds<=0 → 不设超时（既有行为不变）。"""
        reg = AgentRegistry(info_timeout_seconds=0)
        reg.register(InfoAdapter(declarations=DECL), agent_id="a")
        summary = reg.collect()
        assert all(s["ok"] for s in summary)
        assert reg.get("a").max_concurrency == 5

    def test_default_timeout_and_propagation(self):
        reg = AgentRegistry()
        assert reg.info_timeout_seconds == 30.0
        reg.info_timeout_seconds = 5.0
        assert reg.pool.info_timeout_seconds == 5.0
