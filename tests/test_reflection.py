"""治理层反思/判定测试（#42）：最终交付 × 原始目标 → 判定结论（advisory）。

覆盖：
- 基准只能是 goal（子任务描述不作基准，子任务结果只作证据）
- 独立判定 agent（声明 judge 能力）优先；无则由 allow_self_judge 决定
  （降级自判须在报告中标记 non-independent）
- 无目标 / 无判定者 → 跳过（skipped_reason，不视为错误）
- 判定链路故障（协议解析失败 / 超时 / 判定 agent 报失败）→ error_code，
  但绝不抛出（advisory 不阻断 run 收尾）
- output_schema 强校验 + 解析重试接线（判定输出与任务产出一视同仁）
- 同步 / 异步双路径一致
"""
from __future__ import annotations

import asyncio
import json
import time

from orchestration.adapters.base import AgentAdapter
from orchestration.models import (
    DAG,
    Result,
    ScheduleReport,
    Task,
    TaskStatus,
    Usage,
)
from orchestration.reflection import (
    JUDGE_CAPABILITIES,
    REFLECTION_TASK_ID,
    VERDICT_SCHEMA,
    Reflector,
)
from orchestration.registry import AgentRegistry

_GOAL = "把抓取到的价格数据整理成一份比价报告"


# ---------------------------------------------------------------------------
# 测试替身
# ---------------------------------------------------------------------------

class TextAdapter(AgentAdapter):
    """按脚本返回原始文本——走基类协议装配 + 双层校验解析全路径。"""

    def __init__(self, texts: list[str], model: str = "deepseek-chat"):
        super().__init__(model=model)
        self.texts = list(texts)
        self.calls = 0
        self.user_contents: list[str] = []

    def _call_llm(self, messages: list[dict]) -> str:
        self.calls += 1
        self.user_contents.append(messages[-1]["content"])
        idx = min(self.calls - 1, len(self.texts) - 1)
        return self.texts[idx]


class SlowAdapter(AgentAdapter):
    """判定调用挂住（验证框架侧 wall-clock 超时）。"""

    def __init__(self, delay: float = 0.5, model: str = "slow-model"):
        super().__init__(model=model)
        self.delay = delay

    def _call_llm(self, messages: list[dict]) -> str:  # pragma: no cover
        raise NotImplementedError

    def run_task(self, task, request_id, inputs=None) -> Result:
        time.sleep(self.delay)
        return Result(task_id=task.id, success=True, output={})

    async def arun_task(self, task, request_id, inputs=None) -> Result:
        await asyncio.sleep(self.delay)
        return Result(task_id=task.id, success=True, output={})


def verdict_text(
    request_id: str,
    achieved: bool = True,
    score: float = 0.9,
    reasons: list[str] | None = None,
    gaps: list[str] | None = None,
    output: dict | None = None,
) -> str:
    """构造合法判定响应（request_id 原样回带）。"""
    if output is None:
        output = {
            "achieved": achieved,
            "score": score,
            "reasons": reasons or ["交付物结构完整，覆盖目标要求"],
            "gaps": gaps or [],
        }
    return json.dumps({
        "request_id": request_id,
        "task_id": REFLECTION_TASK_ID,
        "success": True,
        "output": output,
    })


def reference_report(final_status: str = "success") -> ScheduleReport:
    """t2 是最终交付（出度 0），t1 只是过程任务（证据）。"""
    dag = DAG(tasks={
        "t1": Task(id="t1", desc="抓取价格数据", status=TaskStatus.SUCCESS,
                   result=Result(task_id="t1", success=True,
                                 output={"rows": 3},
                                 usage=Usage(cost=0.01), duration_ms=10)),
        "t2": Task(id="t2", desc="生成比价报告", deps=["t1"],
                   status=TaskStatus.SUCCESS,
                   result=Result(task_id="t2", success=True,
                                 output={"report": "..."},
                                 usage=Usage(cost=0.01), duration_ms=20)),
    })
    return ScheduleReport(
        dag=dag,
        results={tid: t.result for tid, t in dag.tasks.items()},
        total_cost=0.02,
        final_status=final_status,
    )


def make_registry(adapter: AgentAdapter, judge: bool = False,
                  model: str = "deepseek-chat") -> tuple[AgentRegistry, str]:
    reg = AgentRegistry()
    aid = reg.register(adapter, agent_id="judge_bot" if judge else None)
    if judge:
        reg.get(aid).capabilities = ["judge"]
    return reg, aid


# ---------------------------------------------------------------------------
# 判定者选择 / 跳过
# ---------------------------------------------------------------------------

class TestReflectorSelection:
    def test_no_goal_skips(self):
        """无原始目标 → 无基准可判，跳过（不是错误）。"""
        reg, _ = make_registry(TextAdapter([verdict_text("reflection:r1")]),
                               judge=True)
        out = asyncio.run(Reflector(reg).areflect("", reference_report(), "r1"))
        assert out.enabled is False
        assert out.skipped_reason == "no_goal"
        assert out.judged is False

    def test_judge_capability_agent_used(self):
        """声明 judge 能力的 agent 被选为判定者，标记 independent。"""
        adapter = TextAdapter([verdict_text("reflection:r1")])
        reg, aid = make_registry(adapter, judge=True)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert out.judged is True
        assert out.independent is True
        assert out.judge_agent == aid == "judge_bot"
        assert out.achieved is True
        assert out.score == 0.9
        assert out.reasons and out.gaps == []

    def test_self_judge_marked_non_independent(self):
        """无判定 agent 但允许自判 → 跑，但报告标记 non-independent。"""
        adapter = TextAdapter([verdict_text("reflection:r1")])
        reg, aid = make_registry(adapter, judge=False)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert out.judged is True
        assert out.independent is False
        assert out.judge_agent == aid

    def test_no_self_judge_skips(self):
        """无判定 agent 且不允许自判 → 跳过。"""
        reg, _ = make_registry(TextAdapter([verdict_text("reflection:r1")]))
        reflector = Reflector(reg, allow_self_judge=False)
        out = asyncio.run(reflector.areflect(_GOAL, reference_report(), "r1"))
        assert out.enabled is False
        assert out.skipped_reason == "no_judge_agent"

    def test_no_agent_at_all_skips(self):
        """空注册表 → 跳过而非抛错（advisory）。"""
        reg = AgentRegistry()
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert out.enabled is False
        assert out.skipped_reason.startswith("no_agent")

    def test_judge_capability_tags(self):
        """judge / reviewer / reflection 任一标签都算判定能力。"""
        for tag in JUDGE_CAPABILITIES:
            reg = AgentRegistry()
            aid = reg.register(TextAdapter([verdict_text("reflection:r1")]))
            reg.get(aid).capabilities = [tag]
            out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
            assert out.independent is True, tag

    def test_unavailable_judge_agent_not_picked(self):
        """摘除的判定 agent 不参与判定（available 才用它）。"""
        reg, aid = make_registry(TextAdapter([verdict_text("reflection:r1")]),
                                 judge=True)
        reg.get(aid).status = "unavailable"
        reflector = Reflector(reg, allow_self_judge=False)
        out = asyncio.run(reflector.areflect(_GOAL, reference_report(), "r1"))
        assert out.enabled is False
        assert out.skipped_reason == "no_judge_agent"


# ---------------------------------------------------------------------------
# 素材组装（基准 / 待验 / 证据分离）
# ---------------------------------------------------------------------------

class TestReflectorInputs:
    def test_goal_is_criterion_deliverables_separate_from_evidence(self):
        adapter = TextAdapter([verdict_text("reflection:r1")])
        reg, _ = make_registry(adapter, judge=True)
        asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))

        content = adapter.user_contents[0]
        assert REFLECTION_TASK_ID in content            # 判定任务请求
        assert _GOAL in content                          # 基准是原始目标
        assert "deliverables" in content and "evidence" in content
        # 交付任务描述进 deliverables，过程任务进 evidence——
        # 两者分离，子任务只作证据不作基准
        req_and_input = content.split("=== INPUT ===")[-1]
        payload = json.loads(req_and_input)
        assert [d["task_id"] for d in payload["deliverables"]] == ["t2"]
        assert [e["task_id"] for e in payload["evidence"]] == ["t1"]
        assert payload["facts"]["final_status"] == "success"
        assert payload["facts"]["status_counts"]["success"] == 2

    def test_failed_process_task_summarized_with_error_code(self):
        adapter = TextAdapter([verdict_text("reflection:r1", achieved=False,
                                            score=0.2, gaps=["缺报告"])])
        reg, _ = make_registry(adapter, judge=True)
        report = reference_report(final_status="partial")
        report.dag.tasks["t1"].status = TaskStatus.FAILED
        report.dag.tasks["t1"].result = Result(
            task_id="t1", success=False,
            error={"code": "upstream_500", "message": "boom"},
        )
        asyncio.run(Reflector(reg).areflect(_GOAL, report, "r1"))
        payload = json.loads(adapter.user_contents[0].split("=== INPUT ===")[-1])
        summary = payload["evidence"][0]["summary"]
        assert "upstream_500" in summary
        assert payload["facts"]["final_status"] == "partial"

    def test_output_clipped(self):
        """大产出被截断（判定请求控规模）。"""
        adapter = TextAdapter([verdict_text("reflection:r1")])
        reg, _ = make_registry(adapter, judge=True)
        report = reference_report()
        report.dag.tasks["t2"].result.output = {"blob": "x" * 5000}
        asyncio.run(Reflector(reg, max_chars=100).areflect(_GOAL, report, "r1"))
        payload = json.loads(adapter.user_contents[0].split("=== INPUT ===")[-1])
        assert "已截断" in payload["deliverables"][0]["output"]


# ---------------------------------------------------------------------------
# 判定链路故障（advisory：绝不抛出）
# ---------------------------------------------------------------------------

class TestReflectorFailures:
    def test_judge_reports_failure(self):
        """判定 agent 报 success=false → error_code 落库，不抛。"""
        text = json.dumps({
            "request_id": "reflection:r1", "task_id": REFLECTION_TASK_ID,
            "success": False,
            "error": {"code": "cannot_judge", "message": "证据不足"},
        })
        reg, _ = make_registry(TextAdapter([text]), judge=True)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert out.judged is True
        assert out.achieved is None
        assert out.error_code == "cannot_judge"
        assert out.ok is False

    def test_output_schema_enforced_with_parse_retry(self):
        """判定输出缺字段 → §7.2 强校验 + §7.3 修正重试 → 仍失败则 judge_error。"""
        bad = verdict_text("reflection:r1", output={"achieved": True})  # 缺 score/reasons/gaps
        adapter = TextAdapter([bad, bad])
        reg, _ = make_registry(adapter, judge=True)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert adapter.calls == 2  # 解析重试恰好一次
        assert out.error_code == "judge_error"
        assert out.judged is True

    def test_parse_retry_recovers(self):
        """首次不合规、带修正提示重试成功 → 结论可用。"""
        bad = json.dumps({"request_id": "reflection:r1",
                          "task_id": REFLECTION_TASK_ID, "success": True,
                          "output": {}})
        adapter = TextAdapter([bad, verdict_text("reflection:r1")])
        reg, _ = make_registry(adapter, judge=True)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert adapter.calls == 2
        assert out.ok is True and out.achieved is True

    def test_timeout_does_not_raise(self):
        """判定挂死 → 框架侧 wall-clock 超时 → error_code=timeout。"""
        reg, _ = make_registry(SlowAdapter(delay=0.5))
        reflector = Reflector(reg, timeout_seconds=0.05, allow_self_judge=True)
        start = time.monotonic()
        out = asyncio.run(reflector.areflect(_GOAL, reference_report(), "r1"))
        assert out.error_code == "timeout"
        assert time.monotonic() - start < 0.45  # 未被慢调用拖住

    def test_verdict_schema_shape(self):
        """判定结构契约：全字段必需（框架侧强校验口径）。"""
        assert set(VERDICT_SCHEMA) == {"achieved", "score", "reasons", "gaps"}
        assert VERDICT_SCHEMA["achieved"] == "boolean"
        assert VERDICT_SCHEMA["reasons"] == ["string"]

    def test_cost_and_duration_recorded(self):
        """判定成本/耗时单列（判定调用有成本，须可观测）。"""
        text = json.dumps({
            "request_id": "reflection:r1", "task_id": REFLECTION_TASK_ID,
            "success": True,
            "output": {"achieved": True, "score": 1.0,
                       "reasons": ["ok"], "gaps": []},
            "usage": {"tokens_in": 10, "tokens_out": 5, "cost": 0.003},
            "duration_ms": 777,
        })
        reg, _ = make_registry(TextAdapter([text]), judge=True)
        out = asyncio.run(Reflector(reg).areflect(_GOAL, reference_report(), "r1"))
        assert out.cost == 0.003
        assert out.duration_ms == 777


# ---------------------------------------------------------------------------
# 同步路径
# ---------------------------------------------------------------------------

class TestReflectorSync:
    def test_sync_reflect_parity(self):
        adapter = TextAdapter([verdict_text("reflection:r1")])
        reg, aid = make_registry(adapter, judge=True)
        out = Reflector(reg).reflect(_GOAL, reference_report(), "r1")
        assert out.ok is True
        assert out.independent is True
        assert out.judge_agent == aid

    def test_sync_no_goal_skips(self):
        reg, _ = make_registry(TextAdapter([verdict_text("reflection:r1")]), judge=True)
        out = Reflector(reg).reflect("   ", reference_report(), "r1")
        assert out.enabled is False
        assert out.skipped_reason == "no_goal"

    def test_sync_timeout(self):
        reg, _ = make_registry(SlowAdapter(delay=0.5))
        out = Reflector(reg, timeout_seconds=0.05).reflect(
            _GOAL, reference_report(), "r1"
        )
        assert out.error_code == "timeout"

    def test_sync_does_not_mutate_tasks(self):
        """advisory：判定绝不改任务状态（不改状态、不自动重派）。"""
        reg, _ = make_registry(TextAdapter([verdict_text("reflection:r1")]),
                               judge=True)
        report = reference_report()
        Reflector(reg).reflect(_GOAL, report, "r1")
        assert all(t.status == TaskStatus.SUCCESS
                   for t in report.dag.tasks.values())
