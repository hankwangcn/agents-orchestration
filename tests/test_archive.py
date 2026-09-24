"""运行存档（持久化底座）测试：过程事件流 + 报告读回 + 运行枚举 + 叙述留痕。

归档分层：运行存档 = 过程（事件流）+ 终态报告，属持久化底座、单 run 不可变；
人读版是读时投影（report_view），不落盘。本测试覆盖底座的读写与级联清理。
"""
from __future__ import annotations

from orchestration.models import DAG, Task
from orchestration.state_store import SqliteStateStore, StateStore


def _store() -> SqliteStateStore:
    return SqliteStateStore(":memory:")


class TestEventStream:
    def test_append_and_load_ordered(self):
        s = _store()
        s.append_event("r1", "run_started", data={"dag_size": 2})
        s.append_event("r1", "task_launched", task_id="a", agent_id="bot",
                       data={"match_type": "exact"})
        s.append_event("r1", "task_done", task_id="a", agent_id="bot",
                       data={"success": True, "cost": 0.01})
        s.append_event("r1", "run_finished", data={"final_status": "success"})
        ev = s.load_events("r1")
        assert [e["event"] for e in ev] == [
            "run_started", "task_launched", "task_done", "run_finished"
        ]
        assert ev[1]["task_id"] == "a" and ev[1]["agent_id"] == "bot"
        assert ev[1]["data"] == {"match_type": "exact"}
        assert ev[0]["ts"]  # 时间戳落盘

    def test_events_isolated_per_run(self):
        s = _store()
        s.append_event("r1", "run_started")
        s.append_event("r2", "run_started")
        assert len(s.load_events("r1")) == 1
        assert len(s.load_events("r2")) == 1
        assert s.load_events("nope") == []

    def test_data_roundtrip_defaults(self):
        s = _store()
        s.append_event("r1", "prune", task_id="t")
        e = s.load_events("r1")[0]
        assert e["data"] == {} and e["agent_id"] == ""


class TestReportReadback:
    def test_report_roundtrip(self):
        s = _store()
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        s.save_run("r1", dag, goal="g")
        assert s.load_report("r1") is None  # 未收尾

        from orchestration.models import ScheduleReport
        rep = ScheduleReport(dag=dag, final_status="success", total_cost=0.5)
        s.save_report("r1", rep)
        got = s.load_report("r1")
        assert got["final_status"] == "success"
        assert got["total_cost"] == 0.5

    def test_load_report_missing_run(self):
        assert _store().load_report("nope") is None


class TestRunEnumeration:
    def test_list_runs_recent_first(self):
        s = _store()
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        s.save_run("r1", dag, goal="first")
        s.save_run("r2", dag, goal="second")
        runs = s.list_runs()
        assert {r["run_id"] for r in runs} == {"r1", "r2"}
        assert all(r["run_status"] == "running" for r in runs)
        assert all(r["has_report"] is False for r in runs)

    def test_list_runs_has_report_flag_and_limit(self):
        s = _store()
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        from orchestration.models import ScheduleReport
        s.save_run("r1", dag, goal="x")
        s.save_report("r1", ScheduleReport(dag=dag, final_status="success"))
        s.save_run("r2", dag, goal="y")
        runs = s.list_runs(limit=1)
        assert len(runs) == 1
        # 最近写入的在前
        assert runs[0]["run_id"] == "r2"
        r1 = [r for r in s.list_runs() if r["run_id"] == "r1"][0]
        assert r1["has_report"] is True and r1["goal"] == "x"


class TestNarrative:
    def test_narrative_roundtrip_and_overwrite(self):
        s = _store()
        assert s.load_narrative("r1") is None
        s.save_narrative("r1", {"text": "第一次", "model": "m",
                                "created_at": "2026-01-01T00:00:00+00:00"})
        assert s.load_narrative("r1")["text"] == "第一次"
        s.save_narrative("r1", {"text": "第二次", "model": "m2"})
        got = s.load_narrative("r1")
        assert got["text"] == "第二次" and got["model"] == "m2"
        assert got["created_at"]  # 未给则补默认时间


class TestDeleteCascade:
    def test_delete_run_clears_archive(self):
        s = _store()
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        s.save_run("r1", dag, goal="g")
        s.append_event("r1", "run_started")
        s.save_narrative("r1", {"text": "n", "model": "m"})
        s.delete_run("r1")
        assert not s.has_run("r1")
        assert s.load_events("r1") == []
        assert s.load_narrative("r1") is None
        assert s.load_report("r1") is None


class TestBaseDegradation:
    """存储未实现归档能力时应优雅退化（no-op / 空），不拖垮调用方。"""

    def test_base_defaults(self):
        class Minimal(StateStore):
            def save_run(self, *a, **k): ...
            def save_report(self, *a, **k): ...
            def load_run(self, *a, **k): return {}
            def has_run(self, run_id): return False
            def active_runs(self): return []
            def delete_run(self, run_id): ...

        m = Minimal()
        assert m.append_event("r", "e") is None   # no-op
        assert m.load_events("r") == []
        assert m.load_report("r") is None
        assert m.list_runs() == []
        assert m.save_narrative("r", {}) is None
        assert m.load_narrative("r") is None
