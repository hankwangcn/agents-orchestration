"""人读投影（report_view）测试：确定性、结构、转义。

人读版是运行存档的**读时投影**（按需渲染、纯函数、确定性）——本测试验证
同一份存档 → 同一份视图，且渲染对不可信文本做转义（防注入）。
"""
from __future__ import annotations

import json

from orchestration.report_view import (
    build_run_view,
    render_index_html,
    render_run_html,
    render_text,
)

_REPORT = {
    "final_status": "partial",
    "total_cost": 0.03,
    "dag": {"tasks": {
        "a": {
            "id": "a", "desc": "抓取价格", "deps": [], "status": "success",
            "side_effects": "none",
            "result": {"success": True, "output": {"n": 1}, "error": None,
                       "usage": {"tokens_in": 10, "tokens_out": 5, "cost": 0.01},
                       "duration_ms": 100, "retries": 0},
        },
        "b": {
            "id": "b", "desc": "<script>alert(1)</script>", "deps": ["a"],
            "status": "failed",
            "result": {"success": False, "output": None,
                       "error": {"code": "boom", "message": ""},
                       "usage": {"tokens_in": 0, "tokens_out": 0, "cost": 0.0},
                       "duration_ms": 50, "retries": 2},
        },
        "c": {"id": "c", "desc": "汇总", "deps": ["b"], "status": "cancelled",
              "result": None},
    }},
    "prune_reports": [{
        "root_failure": {"task_id": "b", "reason": "boom", "retries": 2},
        "pruned": [{"task_id": "c", "state_at_cancel": "pending",
                    "prune_reason": "unreachable_from_final"}],
        "pruned_final": True,
    }],
    "assignments": [
        {"task_id": "a", "agent_id": "bot", "match_type": "exact",
         "risk": False, "reason": ""},
        {"task_id": "b", "agent_id": "bot2", "match_type": "degraded",
         "risk": True, "reason": "降级兜底"},
    ],
    "reflection": {"enabled": True, "achieved": False, "score": 0.3,
                   "reasons": ["缺最终报告"], "gaps": ["最终报告"],
                   "judge_agent": "judge_bot", "independent": True},
    "audit": {"verdict": "warning", "success_rate": 0.333,
              "issues": ["1 次剪枝（1 任务被剪）"]},
    "cost": {"total_cost": 0.03, "failed_cost": 0.0, "pruned_cost": 0.0,
             "total_tokens_in": 10, "total_tokens_out": 5,
             "by_match_type": [{"match_type": "exact", "cost": 0.01}]},
    "learning": {"rule_count": 1, "rules": [
        {"rule_id": "PRU-1", "severity": "high", "category": "pruning_quality",
         "tier": "objective", "message": "剪枝复盘拆解并行度", "action": "复盘 DAG"},
    ]},
}

_EVENTS = [
    {"ts": "2026-01-01T00:00:01+00:00", "event": "run_started",
     "task_id": "", "agent_id": "", "data": {"dag_size": 3}},
    {"ts": "2026-01-01T00:00:02+00:00", "event": "task_launched",
     "task_id": "a", "agent_id": "bot", "data": {"match_type": "exact"}},
    {"ts": "2026-01-01T00:00:03+00:00", "event": "task_done",
     "task_id": "a", "agent_id": "bot",
     "data": {"success": True, "duration_ms": 100, "cost": 0.01}},
    {"ts": "2026-01-01T00:00:04+00:00", "event": "prune",
     "task_id": "b", "agent_id": "",
     "data": {"pruned_count": 1, "pruned_final": True}},
    {"ts": "2026-01-01T00:00:05+00:00", "event": "run_finished",
     "task_id": "", "agent_id": "",
     "data": {"final_status": "partial", "total_cost": 0.03, "duration_ms": 500}},
]


def _view(**over):
    kwargs = dict(run_id="r1", goal="抓取并出报告", run_status="partial",
                  dag=_REPORT["dag"], events=_EVENTS, report=_REPORT)
    kwargs.update(over)
    return build_run_view(**kwargs)


class TestBuildView:
    def test_deterministic(self):
        """同一份存档 → 同一份视图（纯函数，可重放）。"""
        a = json.dumps(_view(), ensure_ascii=False, sort_keys=True)
        b = json.dumps(_view(), ensure_ascii=False, sort_keys=True)
        assert a == b

    def test_archive_and_summary(self):
        v = _view()
        ar = v["archive"]
        assert ar["final_status"] == "partial"
        assert ar["total_cost"] == 0.03
        assert ar["event_count"] == 5
        assert ar["report_available"] is True
        assert ar["started_at"] == "2026-01-01T00:00:01+00:00"
        assert ar["finished_at"] == "2026-01-01T00:00:05+00:00"
        assert ar["duration_ms"] == 500
        s = v["summary"]
        assert s["tasks_total"] == 3
        assert s["success"] == 1 and s["failed"] == 1 and s["cancelled"] == 1

    def test_timeline_details(self):
        v = _view()
        assert len(v["timeline"]) == 5
        details = [e["detail"] for e in v["timeline"]]
        assert any("运行开始" in d for d in details)
        assert any("派发 a" in d for d in details)
        assert any("剪枝" in d for d in details)
        assert any("运行结束" in d for d in details)
        assert all(d for d in details)

    def test_narrative_none_by_default(self):
        """叙述摘要非确定：未显式生成时为空（不默认生成）。"""
        assert _view()["narrative"] is None
        v = _view(narrative={"text": "总结", "model": "m", "created_at": "t"})
        assert v["narrative"]["text"] == "总结"

    def test_running_partial_view(self):
        """运行中（无终态报告）：仍可由事件流 + 存档 dag 投影过程。"""
        v = build_run_view(run_id="r2", goal="", run_status="running",
                           dag={"tasks": {"a": {
                               "id": "a", "desc": "a", "status": "running"}}},
                           events=_EVENTS[:2], report=None)
        assert v["archive"]["report_available"] is False
        assert v["archive"]["final_status"] == "running"
        assert v["governance"] == {"audit": None, "cost": None, "reflection": None}
        assert v["learning"]["rules"] == []

    def test_falls_back_to_archive_dag(self):
        """无报告体时用存档 dag 的任务状态。"""
        v = build_run_view(run_id="r3", run_status="running",
                           dag={"tasks": {"x": {"id": "x", "desc": "x",
                                                "status": "success"}}},
                           events=[])
        assert [t["task_id"] for t in v["tasks"]] == ["x"]
        assert v["tasks"][0]["status"] == "success"


class TestRenderText:
    def test_text_has_core_sections(self):
        out = render_text(_view())
        assert "run r1  partial" in out
        assert "目标: 抓取并出报告" in out
        assert "过程时间线：" in out
        assert "审计：warning" in out
        assert "学习：1 条规则" in out
        assert "PRU-1" in out


class TestRenderHtml:
    def test_html_escapes_untrusted(self):
        """视图内容来自不可信 agent 产出 → 必须转义，防注入。"""
        html = render_run_html(_view())
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html

    def test_html_has_sections(self):
        html = render_run_html(_view())
        assert "过程时间线" in html
        assert "任务与结果" in html
        assert "失败传播与剪枝" in html
        assert "学习层规则" in html
        # 叙述摘要未生成 → 明确提示"显式触发"，且不把它混进确定性内容
        assert "未生成" in html and "显式触发" in html

    def test_index_empty_and_list(self):
        empty = render_index_html([])
        assert "暂无运行记录" in empty
        listing = render_index_html([
            {"run_id": "r1", "goal": "g", "run_status": "success",
             "updated_at": "2026-01-01T00:00:00+00:00", "has_report": True},
        ])
        assert "/runs/r1" in listing and "已归档" in listing and "g" in listing
