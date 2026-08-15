"""API 网关测试（阶段四）：RunManager 生命周期 + FastAPI 端点。"""
from __future__ import annotations

import asyncio
import time

from orchestration.api.gateway import RunManager, create_app
from orchestration.models import DAG, Task, TaskStatus
from orchestration.registry import AgentRegistry

from helpers import AsyncScriptedAdapter, ok


def make_manager(*adapters, retries=2):
    reg = AgentRegistry()
    for a in adapters:
        reg.register(a)
    return RunManager(registry=reg, retries=retries), reg


class TestRunManager:
    def test_submit_snapshot_wait_report(self):
        """编程式全流程：提交 → 快照 → 等待 → 报告。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        adapter = AsyncScriptedAdapter({"a": [ok("a", {"n": 1})], "b": [ok("b")]})
        manager, _ = make_manager(adapter)

        async def _flow():
            rid = await manager.submit(dag)
            snap = manager.snapshot(rid)
            assert snap["status"] == "running"
            assert {t["id"] for t in snap["tasks"]} == {"a", "b"}
            report = await manager.wait(rid)
            return rid, snap, report

        rid, snap, report = asyncio.run(_flow())

        assert snap["run_id"] == rid
        assert report.final_status == "success"
        assert set(report.results) == {"a", "b"}
        assert report.total_cost == 0.02
        # 快照任务初始状态：a 已派发或完成
        assert all(t["id"] in ("a", "b") for t in snap["tasks"])

    def test_report_while_running_conflict(self):
        """running 中取报告 → 409。"""
        dag = DAG(tasks={"a": Task(id="a", desc="a")})
        adapter = AsyncScriptedAdapter({"a": [ok("a")]}, delay=0.3)
        manager, _ = make_manager(adapter)

        async def _flow():
            rid = await manager.submit(dag)
            try:
                manager.report(rid)
                return None
            except Exception as e:
                return getattr(e, "status_code", None), rid

        code, rid = asyncio.run(_flow())
        assert code == 409

    def test_cancel_running_run(self):
        """取消运行中的 run → final_status=cancelled。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        adapter = AsyncScriptedAdapter(
            {"a": [ok("a")], "b": [ok("b")]}, delay=0.2)
        manager, _ = make_manager(adapter)

        async def _flow():
            rid = await manager.submit(dag)
            await asyncio.sleep(0.05)
            await manager.cancel(rid)
            report = await manager.wait(rid)
            return rid, report

        rid, report = asyncio.run(_flow())
        assert report.final_status == "cancelled"
        assert dag.tasks["a"].status in (TaskStatus.SUCCESS, TaskStatus.CANCELLED)
        assert dag.tasks["b"].status == TaskStatus.CANCELLED

    def test_agents_snapshot(self):
        reg = AgentRegistry()
        a1 = AsyncScriptedAdapter({}, model="deepseek-chat")
        reg.register(a1)
        reg.get("agent_001").capabilities = ["code_review"]
        manager = RunManager(registry=reg)
        snap = manager.agents_snapshot()
        assert snap["agent_001"]["model"] == "deepseek-chat"
        assert snap["agent_001"]["capabilities"] == ["code_review"]


class TestGatewayAPI:
    def test_submit_status_report_flow(self):
        """HTTP 端到端：提交 → 轮询状态 → 报告。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(adapter)
        app, manager = create_app(reg)

        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            resp = client.post("/api/runs", json={
                "dag": {
                    "tasks": {
                        "a": {"id": "a", "desc": "a"},
                        "b": {"id": "b", "desc": "b", "deps": ["a"]},
                    }
                }
            })
            assert resp.status_code == 201
            run_id = resp.json()["run_id"]

            # 轮询直到完成
            status = None
            for _ in range(100):
                snap = client.get(f"/api/runs/{run_id}").json()
                if snap["status"] != "running":
                    status = snap["status"]
                    break
                time.sleep(0.02)
            assert status == "done"

            report = client.get(f"/api/runs/{run_id}/report").json()
            assert report["final_status"] == "success"
            assert set(report["task_results"]) == {"a", "b"}
            assert report["total_cost"] == 0.02

            metrics = client.get(f"/api/runs/{run_id}/metrics").json()
            assert metrics["tasks"]["total"] == 2
            assert metrics["tasks"]["success"] == 2

    def test_404_unknown_run(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        app, _ = create_app(reg)
        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            assert client.get("/api/runs/nope").status_code == 404
            assert client.get("/api/runs/nope/report").status_code == 404

    def test_agents_endpoint(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}, model="deepseek-chat"))
        app, _ = create_app(reg)
        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            resp = client.get("/api/agents")
            assert resp.status_code == 200
            assert "agent_001" in resp.json()
