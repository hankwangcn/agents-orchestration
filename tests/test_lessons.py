"""学习层闭环测试（#43/#44）：经验库落盘 + 跨 run 聚合 + 回馈拆解提示词。

学习层闭环 = 复盘事实 → 规则 → **落盘为跨 run 经验库** → **回馈拆解提示词**。
覆盖：
- 证据强度分级（objective 确定性事实 / judgment LLM 判定），禁止同级呈现
- 经验库：落盘幂等、跨 run 聚合（命中次数 / 贡献 run 数 / 最高 severity /
  最近证据）
- PromptAdvisor：注册表客观事实、复现门槛（跨 run 才写进提示词）、
  判定结论单独成节、条数上限、全空则不污染提示词
- Decomposer 指导块注入：前缀/结尾结构稳定、无 provider 行为不变、
  provider 故障退化
- 网关收尾接入：报告带 audit/cost/learning、经验库落盘、闭环端到端
  （两次 run → 经验库 → 提示词）
- 学习层故障不阻断 run 收尾（防御）
"""
from __future__ import annotations

import asyncio
import json

import pytest
from fastapi import HTTPException

from orchestration.api.gateway import RunManager
from orchestration.decomposer import (
    DECOMPOSITION_PROMPT,
    Decomposer,
)
from orchestration.learning import (
    JUDGMENT_CATEGORIES,
    LearningEngine,
    LearningRule,
    OBJECTIVE_CATEGORIES,
    tier_of,
)
from orchestration.lessons import (
    PromptAdvisor,
    build_digest,
)
from orchestration.models import DAG, Task
from orchestration.reflection import Reflector
from orchestration.registry import AgentRegistry
from orchestration.state_store import SqliteStateStore

from helpers import AsyncScriptedAdapter, fail, ok


def dag2() -> DAG:
    return DAG(tasks={
        "a": Task(id="a", desc="抓取价格"),
        "b": Task(id="b", desc="汇总报告", deps=["a"]),
    })


def row(rule_id, run_id, severity="medium", category="failure_pattern",
        message="m", action="", evidence=None, tier="", created_at="2026-01-01T00:00:00+00:00"):
    return {
        "run_id": run_id, "rule_id": rule_id,
        "tier": tier or tier_of(category), "severity": severity,
        "category": category, "message": message, "action": action,
        "evidence": evidence or {}, "created_at": created_at,
    }


# ---------------------------------------------------------------------------
# 证据强度分级（客观 / 判定 禁止同级）
# ---------------------------------------------------------------------------

class TestTiering:
    @pytest.mark.parametrize("category", sorted(OBJECTIVE_CATEGORIES))
    def test_deterministic_categories_are_objective(self, category):
        from orchestration.learning import LearningReport

        rule = LearningRule(rule_id="X", severity="low", category=category,
                            message="m")
        assert tier_of(category) == "objective"
        assert rule.tier == ""  # 分级在唯一入口 add() 补齐
        out = LearningReport()
        out.add(rule)
        assert rule.tier == "objective" and rule.objective is True

    @pytest.mark.parametrize("category", sorted(JUDGMENT_CATEGORIES))
    def test_judgment_categories_are_judgment(self, category):
        from orchestration.learning import LearningReport
        out = LearningReport()
        rule = LearningRule(rule_id="J", severity="high", category=category,
                            message="m")
        out.add(rule)
        assert rule.tier == "judgment" and rule.objective is False

    def test_explicit_tier_is_kept(self):
        from orchestration.learning import LearningReport
        out = LearningReport()
        rule = LearningRule(rule_id="X", severity="low",
                            category="failure_pattern", tier="judgment",
                            message="m")
        out.add(rule)
        assert rule.tier == "judgment"


# ---------------------------------------------------------------------------
# 经验库落盘（StateStore）
# ---------------------------------------------------------------------------

def report_with(*rules) -> object:
    from orchestration.learning import LearningReport
    out = LearningReport()
    for r in rules:
        out.add(r)
    return out


class TestLessonPersistence:
    def test_roundtrip_and_evidence(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        rep = report_with(LearningRule(
            rule_id="FP-boom", severity="medium", category="failure_pattern",
            message="错误码 boom 出现 2 次", action="换 agent",
            evidence={"code": "boom", "count": 2},
        ))
        store.save_lessons("r1", rep)
        rows = store.load_lessons()
        assert len(rows) == 1
        r = rows[0]
        assert r["run_id"] == "r1" and r["rule_id"] == "FP-boom"
        assert r["tier"] == "objective"
        assert r["evidence"] == {"code": "boom", "count": 2}
        assert r["message"] and r["action"] == "换 agent"

    def test_save_is_idempotent_per_run(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        rep = report_with(LearningRule(rule_id="DEG-1", severity="medium",
                                       category="degraded_assignment",
                                       message="m"))
        store.save_lessons("r1", rep)
        store.save_lessons("r1", rep)
        assert len(store.load_lessons()) == 1  # 同 run 重跑覆盖，不重复计数

    def test_empty_report_clears_run_rows(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        store.save_lessons("r1", report_with(LearningRule(
            rule_id="DEG-1", severity="medium",
            category="degraded_assignment", message="m")))
        store.save_lessons("r1", report_with())
        assert store.load_lessons() == []

    def test_delete_run_removes_lessons(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        store.save_run("r1", dag2())
        store.save_lessons("r1", report_with(LearningRule(
            rule_id="DEG-1", severity="medium",
            category="degraded_assignment", message="m")))
        store.delete_run("r1")
        assert store.load_lessons() == []

    def test_store_without_support_degrades(self):
        """未实现经验库的存储 → no-op / 空表（学习层退化为无跨 run 记忆）。"""
        from orchestration.state_store import StateStore

        class Minimal(StateStore):
            def save_run(self, *a, **k): ...
            def save_report(self, *a, **k): ...
            def load_run(self, *a, **k): return {}
            def has_run(self, *a, **k): return False
            def active_runs(self): return []
            def delete_run(self, *a, **k): ...

        store = Minimal()
        store.save_lessons("r1", report_with())
        assert store.load_lessons() == []


# ---------------------------------------------------------------------------
# 跨 run 聚合（LessonDigest）
# ---------------------------------------------------------------------------

class TestDigestAggregation:
    def test_empty(self):
        d = build_digest([])
        assert d.lessons == [] and d.runs_considered == 0 and d.generated_at

    def test_counts_and_runs(self):
        d = build_digest([
            row("FP-boom", "r1", severity="medium"),
            row("FP-boom", "r2", severity="medium"),
            row("DEG-1", "r2", severity="low", category="degraded_assignment"),
        ])
        assert d.runs_considered == 2
        by_id = {ls.rule_id: ls for ls in d.lessons}
        assert by_id["FP-boom"].occurrences == 2
        assert by_id["FP-boom"].runs == 2
        assert by_id["DEG-1"].occurrences == 1 and by_id["DEG-1"].runs == 1

    def test_takes_highest_severity(self):
        d = build_digest([row("FP-boom", "r1", severity="low"),
                          row("FP-boom", "r2", severity="high")])
        assert d.lessons[0].severity == "high"

    def test_latest_message_and_evidence_win(self):
        d = build_digest([
            row("FP-boom", "r1", message="旧", action="旧动",
                evidence={"count": 2}, created_at="2026-01-01T00:00:00+00:00"),
            row("FP-boom", "r2", message="新", action="新动",
                evidence={"count": 5}, created_at="2026-02-02T00:00:00+00:00"),
        ])
        ls = d.lessons[0]
        assert (ls.message, ls.action) == ("新", "新动")
        assert ls.evidence == {"count": 5} and ls.last_run_id == "r2"

    def test_ordering_severity_then_occurrences(self):
        d = build_digest([
            row("A", "r1", severity="low"),
            row("B", "r1", severity="high"),
            row("C", "r1", severity="high"),
            row("C", "r2", severity="high"),
        ])
        assert [ls.rule_id for ls in d.lessons] == ["C", "B", "A"]

    def test_skips_rows_without_rule_id(self):
        d = build_digest([{"run_id": "r1", "rule_id": ""}])
        assert d.lessons == [] and d.runs_considered == 0  # 无规则即未贡献经验

    def test_of_tier(self):
        d = build_digest([row("FP-boom", "r1"),
                          row("JUD-1", "r1", category="goal_mismatch")])
        assert [ls.rule_id for ls in d.of_tier("objective")] == ["FP-boom"]
        assert [ls.rule_id for ls in d.of_tier("judgment")] == ["JUD-1"]


# ---------------------------------------------------------------------------
# PromptAdvisor（闭环出口）
# ---------------------------------------------------------------------------

class _Lessons:
    """最小经验库替身（只提供 load_lessons）。"""

    def __init__(self, rows):
        self.rows = rows

    def load_lessons(self):
        return list(self.rows)


def registry_with(model="deepseek-chat", caps=("code_review",)) -> AgentRegistry:
    reg = AgentRegistry()
    adapter = AsyncScriptedAdapter({}, model=model)
    reg.register(adapter)
    reg.get("agent_001").capabilities = list(caps)
    return reg


class TestPromptAdvisor:
    def test_registry_facts_without_lessons(self):
        """注册表事实是直接测量，无需复现门槛——始终进入提示词。"""
        adv = PromptAdvisor(registry_with(), _Lessons([]))
        text = adv.guidance()
        assert "deepseek-chat" in text and "code_review" in text
        assert PromptAdvisor.OBJECTIVE_HEADER in text
        assert PromptAdvisor.JUDGMENT_HEADER not in text

    def test_empty_registry_and_lessons_yields_nothing(self):
        reg = AgentRegistry()  # 无 agent
        assert PromptAdvisor(reg, _Lessons([])).guidance() == ""
        assert PromptAdvisor(None, None).guidance() == ""

    def test_lessons_need_cross_run_recurrence(self):
        """单次出现不写进提示词——"客观支撑"即跨 run 复现。"""
        one = [row("FP-boom", "r1", message="错误码 boom 反复出现", action="换 agent")]
        adv = PromptAdvisor(None, _Lessons(one), min_occurrences=2)
        assert "FP-boom" not in adv.guidance()
        adv.min_occurrences = 1
        text = adv.guidance()
        assert "错误码 boom 反复出现" in text and "换 agent" in text
        assert "既往 1 次运行命中 1 次" in text  # 数值支撑随行给出

    def test_judgment_separated_from_objective(self):
        rows = [
            row("FP-boom", "r1"), row("FP-boom", "r2"),
            row("JUD-1", "r1", category="goal_mismatch", severity="high",
                message="判定认为未达成目标"),
            row("JUD-1", "r2", category="goal_mismatch", severity="high",
                message="判定认为未达成目标"),
        ]
        text = PromptAdvisor(None, _Lessons(rows)).guidance()
        obj_at = text.index(PromptAdvisor.OBJECTIVE_HEADER)
        jud_at = text.index(PromptAdvisor.JUDGMENT_HEADER)
        assert obj_at < jud_at
        # FP-* 只在客观节，JUD-* 只在判定节——禁止同级呈现
        assert "FP-boom" not in text[jud_at:]
        assert "判定认为未达成目标" not in text[obj_at:jud_at]
        assert "非确定来源" in text

    def test_include_judgment_can_be_disabled(self):
        rows = [row("JUD-1", "r1", category="goal_mismatch"),
                row("JUD-1", "r2", category="goal_mismatch")]
        adv = PromptAdvisor(None, _Lessons(rows), include_judgment=False)
        assert adv.guidance() == ""

    def test_non_prompt_categories_excluded(self):
        """框架自身问题（断链 REC-1）不是拆解指导，不进提示词。"""
        rows = [row("REC-1", "r1", category="reconciliation", severity="high"),
                row("REC-1", "r2", category="reconciliation", severity="high")]
        adv = PromptAdvisor(None, _Lessons(rows), min_occurrences=1)
        assert adv.guidance() == ""

    def test_max_items_caps_block(self):
        rows = []
        for i in range(6):
            rows += [row(f"FP-c{i}", "r1"), row(f"FP-c{i}", "r2")]
        text = PromptAdvisor(None, _Lessons(rows), max_items=3).guidance()
        assert text.count("[客观]") == 3

    def test_store_read_failure_degrades(self):
        class Boom:
            def load_lessons(self):
                raise RuntimeError("db down")

        adv = PromptAdvisor(registry_with(), Boom())
        text = adv.guidance()          # 不抛出
        assert "deepseek-chat" in text  # 注册表事实仍在
        assert adv.digest().lessons == []

    def test_long_message_is_clipped(self):
        rows = [row("FP-x", "r1", message="啊" * 400),
                row("FP-x", "r2", message="啊" * 400)]
        text = PromptAdvisor(None, _Lessons(rows), min_occurrences=1).guidance()
        assert "…" in text and max(len(line) for line in text.splitlines()) < 300


# ---------------------------------------------------------------------------
# Decomposer 指导块注入
# ---------------------------------------------------------------------------

VALID = json.dumps({"tasks": [
    {"id": "t1", "desc": "抓取", "deps": []},
    {"id": "t2", "desc": "汇总", "deps": ["t1"]},
]}, ensure_ascii=False)


class ScriptedLLM:
    def __init__(self, *responses: str):
        self.responses = list(responses)
        self.prompts: list[str] = []

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return self.responses.pop(0)


class TestDecomposerGuidanceInjection:
    def test_no_provider_keeps_prompt_shape(self):
        llm = ScriptedLLM(VALID)
        dec = Decomposer(llm)
        dec.decompose("目标X")
        assert llm.prompts[0] == f"{DECOMPOSITION_PROMPT}\n\n用户目标：目标X"
        assert dec.last_guidance == ""

    def test_guidance_injected_between_instruction_and_goal(self):
        llm = ScriptedLLM(VALID)
        dec = Decomposer(llm, guidance_provider=lambda: "【经验】可用模型：m1")
        dec.decompose("目标X")
        prompt = llm.prompts[0]
        # 结构稳定：前缀恒为固定指令、结尾恒为用户目标
        assert prompt.startswith(DECOMPOSITION_PROMPT)
        assert prompt.endswith("用户目标：目标X")
        assert "【经验】可用模型：m1" in prompt
        assert dec.last_guidance == "【经验】可用模型：m1"
        assert prompt == dec.last_prompt

    def test_retry_prompt_also_carries_guidance(self):
        llm = ScriptedLLM("not json", VALID)
        dec = Decomposer(llm, guidance_provider=lambda: "【经验】G块")
        dec.decompose("目标X")
        assert "【经验】G块" in llm.prompts[1]
        assert "上一次输出不合规" in llm.prompts[1]

    def test_provider_failure_degrades_to_no_guidance(self):
        def boom():
            raise RuntimeError("经验库挂了")

        llm = ScriptedLLM(VALID)
        dec = Decomposer(llm, guidance_provider=boom)
        dag = dec.decompose("目标X")
        assert set(dag.tasks) == {"t1", "t2"}  # 拆解照常
        assert dec.last_guidance == ""
        assert llm.prompts[0] == f"{DECOMPOSITION_PROMPT}\n\n用户目标：目标X"

    def test_blank_guidance_not_injected(self):
        llm = ScriptedLLM(VALID)
        Decomposer(llm, guidance_provider=lambda: "   \n ").decompose("目标X")
        assert llm.prompts[0] == f"{DECOMPOSITION_PROMPT}\n\n用户目标：目标X"


# ---------------------------------------------------------------------------
# 网关接入（学习层闭环端到端）
# ---------------------------------------------------------------------------

def failing_dag() -> DAG:
    """两个同错误码失败任务 → FP-* 客观规则（count 2 ≥ failure_pattern_min）。"""
    return DAG(tasks={
        "a": Task(id="a", desc="a"),
        "b": Task(id="b", desc="b"),
    })


class TestRunTailLearning:
    def _manager(self, store=None, reflector=None, learning=None, retries=0,
                 runs=3):
        # 脚本按调用次数消费：retries=0 时每 run 每任务一次，故按 run 数备足
        adapter = AsyncScriptedAdapter({
            "a": [fail("a", "boom")] * runs,
            "b": [fail("b", "boom")] * runs,
        })
        reg = AgentRegistry()
        reg.register(adapter)
        return RunManager(
            registry=reg, retries=retries, state_store=store,
            reflector=reflector, learning=learning,
        )

    def test_report_carries_audit_cost_learning(self):
        manager = self._manager()

        async def _flow():
            rid = await manager.submit(failing_dag(), run_id="r1")
            return await manager.wait(rid)

        asyncio.run(_flow())
        payload = manager.report("r1")

        assert payload["audit"]["verdict"] in ("ok", "warning")
        assert payload["cost"]["total_cost"] == 0.0
        rules = payload["learning"]["rules"]
        fp = [r for r in rules if r["rule_id"].startswith("FP-")]
        assert fp and fp[0]["tier"] == "objective"
        assert fp[0]["evidence"]["count"] == 2

    def test_lessons_persisted_and_aggregated_across_runs(self):
        store = SqliteStateStore(":memory:")
        manager = self._manager(store=store)

        async def _run(rid):
            rid = await manager.submit(failing_dag(), run_id=rid)
            await manager.wait(rid)

        asyncio.run(_run("r1"))
        asyncio.run(_run("r2"))

        snap = manager.lessons_snapshot()
        assert snap["runs_considered"] == 2
        fp = [ls for ls in snap["lessons"] if ls["rule_id"].startswith("FP-")]
        assert fp[0]["occurrences"] == 2 and fp[0]["runs"] == 2

    def test_closed_loop_reaches_decomposer_prompt(self):
        """闭环端到端：两次运行 → 经验库 → 提示词注入（含数值支撑）。"""
        store = SqliteStateStore(":memory:")
        manager = self._manager(store=store)
        reg = registry_with()

        async def _run(rid):
            rid = await manager.submit(failing_dag(), run_id=rid)
            await manager.wait(rid)

        asyncio.run(_run("r1"))
        asyncio.run(_run("r2"))

        advisor = PromptAdvisor(reg, store, min_occurrences=2)
        llm = ScriptedLLM(VALID)
        Decomposer(llm, guidance_provider=advisor.guidance).decompose("目标")
        prompt = llm.prompts[0]
        assert "boom" in prompt                       # 客观规则进了提示词
        assert "既往 2 次运行命中 2 次" in prompt      # 且带复现证据
        assert "code_review" in prompt                # 注册表事实

    def test_judgment_feeds_learning_as_judgment_tier(self):
        store = SqliteStateStore(":memory:")
        main = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        judge = AsyncScriptedAdapter(
            {"__reflection__": [ok("__reflection__", {
                "achieved": False, "score": 0.3,
                "reasons": ["只完成抓取"], "gaps": ["缺报告"]})]},
            model="judge-model",
        )
        reg = AgentRegistry()
        reg.register(main)
        reg.register(judge, agent_id="judge_bot")
        reg.get("judge_bot").capabilities = ["judge"]
        manager = RunManager(registry=reg, state_store=store,
                             reflector=Reflector(reg))

        rids: list[str] = []

        async def _flow():
            rid = await manager.submit(dag2(), goal="整理成比价报告")
            rids.append(rid)
            return await manager.wait(rid)

        asyncio.run(_flow())
        rules = manager.report(rids[0])["learning"]["rules"]
        jud = [r for r in rules if r["rule_id"] == "JUD-1"]
        assert jud and jud[0]["tier"] == "judgment"    # LLM 来源单独分级
        assert jud[0]["evidence"]["gaps"] == ["缺报告"]

    def test_learning_failure_does_not_break_run(self):
        class Boom(LearningEngine):
            def learn(self, audit, cost, reflection=None):
                raise RuntimeError("learning blew up")

        manager = self._manager(learning=Boom())

        async def _flow():
            rid = await manager.submit(failing_dag(), run_id="r1")
            return await manager.wait(rid)

        report = asyncio.run(_flow())
        assert report.final_status == "failed"     # 交付状态照旧
        assert report.learning is None             # 防御：学习层故障被隔离
        assert report.audit is None
        assert manager._runs["r1"].status == "done"

    def test_lessons_snapshot_requires_store(self):
        manager = self._manager()
        with pytest.raises(HTTPException) as ei:
            manager.lessons_snapshot()
        assert ei.value.status_code == 400

    def test_endpoint_lessons(self):
        from fastapi.testclient import TestClient

        from orchestration.api.gateway import create_app

        store = SqliteStateStore(":memory:")
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({"a": [fail("a", "boom")]}))
        app, manager = create_app(reg, retries=0, state_store=store)

        with TestClient(app) as client:
            rid = client.post("/api/runs", json={
                "dag": {"tasks": {"a": {"id": "a", "desc": "a"}}},
                "run_id": "r1",
            }).json()["run_id"]
            for _ in range(100):
                if client.get(f"/api/runs/{rid}").json()["status"] != "running":
                    break
            resp = client.get("/api/lessons")
            assert resp.status_code == 200
            body = resp.json()
            assert body["runs_considered"] == 1
            assert body["lesson_count"] >= 1

    def test_endpoint_lessons_without_store(self):
        from fastapi.testclient import TestClient

        from orchestration.api.gateway import create_app

        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({"a": [ok("a")]}))
        app, _ = create_app(reg)
        with TestClient(app) as client:
            assert client.get("/api/lessons").status_code == 400
