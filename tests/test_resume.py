"""断点持久化与恢复测试（A+B 策略）。

覆盖：
- StateStore 读写往返 / active_runs / delete
- resume_run 恢复语义：RUNNING 无副作用重派、有副作用置 INTERRUPTED、
  已终态复用（不重跑不重计费）、SKIPPED 依赖恢复
- 恢复后失败传播仍工作
- 网关 resume / resolve 全流程（含人工 complete 后下游继续）
"""
from __future__ import annotations

import asyncio
import time

from orchestration.models import (
    DAG,
    Assignment,
    PruneReport,
    Result,
    SideEffects,
    Task,
    TaskStatus,
)
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler
from orchestration.state_store import SqliteStateStore

from helpers import AsyncScriptedAdapter, ok


# ---------------------------------------------------------------------------
# StateStore
# ---------------------------------------------------------------------------

class TestStateStore:
    def test_save_load_roundtrip(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.SUCCESS,
                      result=Result(task_id="a", success=True)),
        })
        store.save_run(
            "r1", dag,
            assignments=[Assignment(task_id="a", agent_id="ag1",
                                    match_type="exact")],
            prune_reports=[PruneReport(
                root_failure={"task_id": "a", "reason": "x", "retries": 1},
                pruned=[], pruned_final=False,
            )],
        )
        data = store.load_run("r1")
        assert data["dag"].tasks["a"].status == TaskStatus.SUCCESS
        assert data["assignments"]["a"].agent_id == "ag1"
        assert len(data["prune_reports"]) == 1
        assert store.has_run("r1")

    def test_save_run_keeps_status_when_none(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        store.save_run("r1", DAG(tasks={"a": Task(id="a", desc="a")}),
                       run_status="success")
        # 局部变更（resolve）不覆盖调度状态
        store.save_run("r1", DAG(tasks={"a": Task(id="a", desc="a",
                                                  status=TaskStatus.SUCCESS)}))
        assert store.load_run("r1")["run_status"] == "success"

    def test_active_runs_and_delete(self, tmp_path):
        store = SqliteStateStore(str(tmp_path / "s.db"))
        store.save_run("r1", DAG(tasks={"a": Task(id="a", desc="a")}))
        store.save_run("r2", DAG(tasks={"a": Task(id="a", desc="a")}),
                       run_status="success")
        assert set(store.active_runs()) == {"r1"}
        store.delete_run("r1")
        assert not store.has_run("r1")
        assert store.has_run("r2")

    def test_memory_store(self):
        store = SqliteStateStore(":memory:")
        store.save_run("r1", DAG(tasks={"a": Task(id="a", desc="a")}))
        assert store.has_run("r1")


# ---------------------------------------------------------------------------
# 恢复语义
# ---------------------------------------------------------------------------

def make_scheduler(store, *adapters, retries=2, backoff_base=0.0):
    reg = AgentRegistry()
    for a in adapters:
        reg.register(a)
    return AsyncScheduler(registry=reg, retries=retries,
                          backoff_base=backoff_base, state_store=store), reg


class TestResume:
    def test_running_state_persisted_during_schedule(self, tmp_path):
        """事件驱动落盘：任务启动（RUNNING）即持久化，崩溃点可识别执行中任务。

        这是 A+B 策略生效的前提——若 RUNNING 不落盘，副作用任务崩溃恢复后
        会退化为 PENDING 被当普通任务重派（副作用可能执行两次）。
        """
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        adapter = AsyncScriptedAdapter({"a": [ok("a")]}, delay=0.2)
        reg = AgentRegistry()
        reg.register(adapter)
        sched = AsyncScheduler(registry=reg, state_store=store)

        async def _flow():
            t = asyncio.create_task(sched.run(dag, run_id="r1"))
            await asyncio.sleep(0.05)  # a 正在执行（delay 0.2）
            # 崩溃模拟：读取 store——a 必须是 RUNNING（而非 PENDING）
            persisted = store.load_run("r1")
            await t  # 等正常结束，清理
            return persisted["dag"].tasks["a"].status

        status = asyncio.run(_flow())
        assert status == TaskStatus.RUNNING

    def test_reruns_running_pure_task(self, tmp_path):
        """RUNNING 无副作用 → 重派，整棵跑完。"""
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.RUNNING),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        store.save_run("r1", dag)  # 崩溃现场
        adapter = AsyncScriptedAdapter({"a": [ok("a", {"n": 1})], "b": [ok("b")]})
        sched, _ = make_scheduler(store, adapter)
        report = asyncio.run(sched.resume_run("r1"))
        assert report.final_status == "success"
        assert report.dag.tasks["a"].status == TaskStatus.SUCCESS
        assert report.dag.tasks["b"].status == TaskStatus.SUCCESS
        assert [c[0] for c in adapter.calls] == ["a", "b"]

    def test_interrupts_side_effect_task(self, tmp_path):
        """RUNNING 声明副作用 → INTERRUPTED 不重派，下游 SKIPPED，final=interrupted。"""
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.RUNNING,
                      side_effects=SideEffects.EXTERNAL_API),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        store.save_run("r1", dag)
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, _ = make_scheduler(store, adapter)
        report = asyncio.run(sched.resume_run("r1"))
        assert report.final_status == "interrupted"
        assert report.dag.tasks["a"].status == TaskStatus.INTERRUPTED
        assert report.dag.tasks["b"].status == TaskStatus.SKIPPED
        assert adapter.calls == []  # 副作用任务未被再次执行

    def test_keeps_completed_results_and_cost(self, tmp_path):
        """已终态任务不重跑、结果与成本复用（不重计费）。"""
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(
                id="a", desc="a", status=TaskStatus.SUCCESS,
                result=Result(task_id="a", success=True, output={"n": 1},
                              usage={"tokens_in": 10, "tokens_out": 10,
                                     "cost": 0.01}),
            ),
            "b": Task(id="b", desc="b", deps=["a"], status=TaskStatus.RUNNING),
        })
        store.save_run("r1", dag)
        adapter = AsyncScriptedAdapter({"b": [ok("b")]})
        sched, _ = make_scheduler(store, adapter)
        report = asyncio.run(sched.resume_run("r1"))
        assert report.final_status == "success"
        assert [c[0] for c in adapter.calls] == ["b"]  # a 不重跑
        assert report.results["a"].output == {"n": 1}  # a 结果复用
        assert report.total_cost == 0.02  # a 0.01（复用）+ b 0.01

    def test_failure_propagation_after_resume(self, tmp_path):
        """恢复后失败传播/剪枝仍工作。"""
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.RUNNING),
            "b": Task(id="b", desc="b", deps=["a"]),
            "c": Task(id="c", desc="c", deps=["a"]),
        })
        store.save_run("r1", dag)
        adapter = AsyncScriptedAdapter({
            "a": [{"task_id": "a", "success": False,
                   "error": {"code": "model_error", "message": "x"}}],
        })
        sched, _ = make_scheduler(store, adapter, retries=0)
        report = asyncio.run(sched.resume_run("r1"))
        assert report.final_status == "failed"
        assert report.dag.tasks["a"].status == TaskStatus.FAILED
        assert report.dag.tasks["b"].status == TaskStatus.CANCELLED
        assert report.dag.tasks["c"].status == TaskStatus.CANCELLED
        assert len(report.prune_reports) == 1

    def test_skipped_reopens_after_interrupted_resolved(self, tmp_path):
        """中断任务人工 complete 后，下游 SKIPPED 重新可派发。"""
        store = SqliteStateStore(str(tmp_path / "s.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.INTERRUPTED,
                      side_effects=SideEffects.EXTERNAL_API),
            "b": Task(id="b", desc="b", deps=["a"], status=TaskStatus.SKIPPED),
        })
        store.save_run("r1", dag)
        # 人工 complete a（模拟网关 resolve）
        dag.tasks["a"].status = TaskStatus.SUCCESS
        dag.tasks["a"].result = Result(task_id="a", success=True)
        store.save_run("r1", dag)
        adapter = AsyncScriptedAdapter({"b": [ok("b")]})
        sched, _ = make_scheduler(store, adapter)
        report = asyncio.run(sched.resume_run("r1"))
        assert report.final_status == "success"
        assert report.dag.tasks["b"].status == TaskStatus.SUCCESS
        assert [c[0] for c in adapter.calls] == ["b"]


# ---------------------------------------------------------------------------
# 网关：resume / resolve 全流程
# ---------------------------------------------------------------------------

class TestGatewayResume:
    def test_resume_resolve_full_flow(self, tmp_path):
        """提交前崩溃现场 → resume 中断 → resolve complete → resume 跑完。"""
        store = SqliteStateStore(str(tmp_path / "g.db"))
        dag = DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.RUNNING,
                      side_effects=SideEffects.EXTERNAL_API),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        store.save_run("r1", dag)  # 崩溃现场
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(adapter)
        from orchestration.api.gateway import create_app
        app, _ = create_app(reg, state_store=store)
        from fastapi.testclient import TestClient

        with TestClient(app) as client:
            # 1. 恢复 → 副作用任务中断
            assert client.post("/api/runs/r1/resume").status_code == 200
            self._wait_done(client, "r1")
            snap = client.get("/api/runs/r1").json()
            assert {t["status"] for t in snap["tasks"]} == {"interrupted",
                                                            "skipped"}

            # 2. 人工确认：complete a
            r = client.post("/api/runs/r1/tasks/a/resolve", json={
                "action": "complete",
                "result": {"task_id": "a", "success": True,
                           "output": {"n": 1},
                           "usage": {"tokens_in": 5, "tokens_out": 5,
                                     "cost": 0.005}},
            })
            assert r.status_code == 200
            assert r.json()["status"] == "success"

            # 3. 再次恢复 → 下游 b 跑完
            assert client.post("/api/runs/r1/resume").status_code == 200
            self._wait_done(client, "r1")
            report = client.get("/api/runs/r1/report").json()
            assert report["final_status"] == "success"
            assert set(report["task_results"]) == {"a", "b"}

    def test_resolve_non_interrupted_conflict(self, tmp_path):
        """非 INTERRUPTED 任务 resolve → 409。"""
        store = SqliteStateStore(str(tmp_path / "g.db"))
        store.save_run("r1", DAG(tasks={
            "a": Task(id="a", desc="a", status=TaskStatus.PENDING),
        }))
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        from orchestration.api.gateway import create_app
        app, _ = create_app(reg, state_store=store)
        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            r = client.post("/api/runs/r1/tasks/a/resolve",
                            json={"action": "cancel"})
            assert r.status_code == 409

    def test_resume_requires_store(self):
        """未启用 state_store → resume 400。"""
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        from orchestration.api.gateway import create_app
        app, _ = create_app(reg)
        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            assert client.post("/api/runs/nope/resume").status_code == 400

    @staticmethod
    def _wait_done(client, run_id):
        for _ in range(200):
            snap = client.get(f"/api/runs/{run_id}").json()
            if snap["status"] != "running":
                return snap
            time.sleep(0.02)
        raise AssertionError("run 未在超时内结束")
