"""API 网关测试（阶段四）：RunManager 生命周期 + FastAPI 端点。"""
from __future__ import annotations

import asyncio
import json
import time

from orchestration.api.gateway import RunManager, create_app
from orchestration.decomposer import Decomposer
from orchestration.models import DAG, Task, TaskStatus
from orchestration.reflection import Reflector
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

    def test_running_snapshot_has_agent(self):
        """运行中快照 tasks[].agent 非空：任务派发即有归属，不等收尾报告。"""
        dag = DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b", deps=["a"]),
        })
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]}, delay=0.3)
        reg = AgentRegistry()
        aid = reg.register(adapter)
        manager = RunManager(registry=reg)

        async def _flow():
            rid = await manager.submit(dag)
            await asyncio.sleep(0.1)  # a 已派发执行中，run 未结束
            snap = manager.snapshot(rid)
            return {t["id"]: t["agent"] for t in snap["tasks"]}

        agents = asyncio.run(_flow())
        assert agents["a"] == aid

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


class TestDecompose:
    """规划层接入（#29）：目标 → DAG（RunManager.decompose + POST /api/decompose）。"""

    @staticmethod
    def _decomposer(payload: dict) -> Decomposer:
        return Decomposer(llm_call=lambda prompt: json.dumps(payload))

    PAYLOAD = {
        "tasks": [
            {"id": "a", "desc": "第一步"},
            {"id": "b", "desc": "第二步", "deps": ["a"]},
        ]
    }

    def test_decompose_returns_dag_without_submitting(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        manager = RunManager(registry=reg, decomposer=self._decomposer(self.PAYLOAD))

        out = asyncio.run(manager.decompose("做一件事"))

        assert out["status"] == "decomposed"
        assert out["run_id"] is None
        assert set(out["dag"]["tasks"]) == {"a", "b"}
        assert out["dag"]["tasks"]["b"]["deps"] == ["a"]
        # 规划层「依赖分析」产物：拓扑分层 = 并行前沿
        assert out["analysis"] == {
            "task_count": 2,
            "levels": [["a"], ["b"]],
            "depth": 2,
            "max_parallel_width": 1,
            "roots": ["a"],
            "final_tasks": ["b"],
        }
        # 未提交 → 无 run
        assert manager._runs == {}

    def test_decompose_submit_runs_to_completion(self):
        """submit=true：拆解 → 直接提交，返回的 run_id 可 wait 到报告（目标→结果一条链）。"""
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(adapter)
        manager = RunManager(registry=reg, decomposer=self._decomposer(self.PAYLOAD))

        async def _flow():
            out = await manager.decompose("做一件事", submit=True)
            return out, await manager.wait(out["run_id"])

        out, report = asyncio.run(_flow())

        assert out["status"] == "submitted"
        assert out["run_id"]
        assert report.final_status == "success"
        assert set(report.results) == {"a", "b"}

    def test_decompose_custom_run_id(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]}))
        manager = RunManager(registry=reg, decomposer=self._decomposer(self.PAYLOAD))
        out = asyncio.run(manager.decompose("目标", submit=True, run_id="my-run"))
        assert out["run_id"] == "my-run"

    def test_decompose_without_engine_400(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        manager = RunManager(registry=reg)  # 未注入 decomposer
        try:
            asyncio.run(manager.decompose("目标"))
            code = None
        except Exception as e:
            code = getattr(e, "status_code", None)
        assert code == 400

    def test_decompose_failure_maps_to_422(self):
        """拆解连续不合规 → DecomposeError → 422（不是 500）。"""
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        manager = RunManager(
            registry=reg,
            decomposer=Decomposer(llm_call=lambda prompt: "不是 JSON", max_retries=0),
        )
        try:
            asyncio.run(manager.decompose("目标"))
            code = None
        except Exception as e:
            code = getattr(e, "status_code", None)
        assert code == 422

    def test_decompose_http_endpoint(self):
        """HTTP 端到端：POST /api/decompose（拆解 + 直接提交 + 轮询报告）。"""
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(adapter)
        app, _ = create_app(reg, decomposer=self._decomposer(self.PAYLOAD))

        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            resp = client.post("/api/decompose", json={"goal": "做一件事"})
            assert resp.status_code == 200
            assert resp.json()["status"] == "decomposed"
            assert set(resp.json()["dag"]["tasks"]) == {"a", "b"}

            resp = client.post(
                "/api/decompose", json={"goal": "做一件事", "submit": True}
            )
            assert resp.status_code == 200
            run_id = resp.json()["run_id"]

            report = None
            for _ in range(100):
                r = client.get(f"/api/runs/{run_id}/report")
                if r.status_code == 200:
                    report = r.json()
                    break
                time.sleep(0.02)
            assert report and report["final_status"] == "success"

    def test_decompose_endpoint_400_without_engine(self):
        reg = AgentRegistry()
        reg.register(AsyncScriptedAdapter({}))
        app, _ = create_app(reg)  # 未注入拆解引擎
        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            resp = client.post("/api/decompose", json={"goal": "目标"})
            assert resp.status_code == 400
            assert "拆解引擎" in resp.json()["detail"]


class TestGatewayAPI:
    def test_submit_status_report_flow(self):
        """HTTP 端到端：提交 → 轮询状态 → 报告。"""
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


class TestGoalAndReflection:
    """run 级目标 + 治理层判定（#41/#42）：提交带 goal → 收尾按目标判定交付。"""

    JUDGE_VERDICT = {"achieved": False, "score": 0.4,
                     "reasons": ["只看到中间数据，最终报告缺失"],
                     "gaps": ["最终比价报告"]}

    def _manager(self):
        main = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        judge = AsyncScriptedAdapter(
            {"__reflection__": [ok("__reflection__", self.JUDGE_VERDICT)]},
            model="judge-model",
        )
        reg = AgentRegistry()
        reg.register(main)
        reg.register(judge, agent_id="judge_bot")
        reg.get("judge_bot").capabilities = ["judge"]  # 能力是问出来的/采集的
        return RunManager(registry=reg, reflector=Reflector(reg)), reg

    @staticmethod
    def _dag():
        return DAG(tasks={
            "a": Task(id="a", desc="a"),
            "b": Task(id="b", desc="b", deps=["a"]),
        })

    def test_report_carries_goal_and_verdict(self):
        """goal 随 run 走；判定结论进报告；advisory 不改交付状态。"""
        manager, _ = self._manager()

        async def _flow():
            rid = await manager.submit(self._dag(), goal="把数据整理成比价报告")
            return rid, await manager.wait(rid)

        rid, report = asyncio.run(_flow())

        assert manager.snapshot(rid)["goal"] == "把数据整理成比价报告"
        payload = manager.report(rid)
        assert payload["goal"] == "把数据整理成比价报告"

        ref = payload["reflection"]
        assert ref["judged"] is True
        assert ref["achieved"] is False        # 过程全绿（success）但目标未达成
        assert ref["gaps"] == ["最终比价报告"]
        assert ref["judge_agent"] == "judge_bot"
        assert ref["independent"] is True
        # advisory：不改状态、不阻断——交付仍按调度结果收尾
        assert report.final_status == "success"
        assert report.total_cost == 0.02       # 判定成本单列，不计入任务成本

    def test_no_goal_skips_reflection(self):
        """未给 goal → 无基准可判，跳过（记录原因，不报错）。"""
        manager, _ = self._manager()

        async def _flow():
            rid = await manager.submit(self._dag())
            return await manager.wait(rid)

        report = asyncio.run(_flow())
        assert report.reflection["enabled"] is False
        assert report.reflection["skipped_reason"] == "no_goal"

    def test_no_reflector_leaves_report_clean(self):
        """未注入 Reflector → 不做判定（框架其余功能不受影响）。"""
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(adapter)
        manager = RunManager(registry=reg)

        async def _flow():
            rid = await manager.submit(self._dag(), goal="目标")
            return await manager.wait(rid)

        report = asyncio.run(_flow())
        assert report.reflection is None
        assert report.final_status == "success"

    def test_reflection_failure_does_not_break_run(self):
        """判定链路整体异常 → run 仍正常收尾（advisory 不阻断）。"""
        main = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        reg = AgentRegistry()
        reg.register(main)
        manager = RunManager(registry=reg, reflector=Reflector(reg))

        class Boom(Reflector):
            async def areflect(self, goal, report, run_id=""):
                raise RuntimeError("judge blew up")

        manager._reflector = Boom(reg)

        async def _flow():
            rid = await manager.submit(self._dag(), goal="目标")
            return await manager.wait(rid)

        report = asyncio.run(_flow())
        assert report.final_status == "success"
        assert report.reflection is None  # 判定失败，但交付正常

    def test_http_submit_with_goal(self):
        """HTTP 端到端：POST /api/runs 带 goal → 快照可见目标。"""
        manager, reg = self._manager()
        app, _ = create_app(reg, reflector=Reflector(reg))

        from fastapi.testclient import TestClient
        with TestClient(app) as client:
            resp = client.post("/api/runs", json={
                "dag": {"tasks": {"a": {"id": "a", "desc": "a"}}},
                "goal": "生成比价报告",
            })
            assert resp.status_code == 201
            run_id = resp.json()["run_id"]
            snap = None
            for _ in range(100):
                snap = client.get(f"/api/runs/{run_id}").json()
                if snap["status"] != "running":
                    break
                time.sleep(0.02)
            assert snap["goal"] == "生成比价报告"
            assert snap["status"] == "done"
