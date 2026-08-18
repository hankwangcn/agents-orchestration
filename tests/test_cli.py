"""CLI 测试：网关瘦客户端的命令解析 / payload 构造 / 输出格式化。

CLI 是纯 HTTP 客户端——测试 mock 掉网络层 _request，验证各子命令
拼出的 URL / payload / 输出；真实网关往返由冒烟脚本覆盖。
"""
from __future__ import annotations

import json
from unittest import mock

import pytest

import orchestration.cli as cli


def run_cli(argv, fake_requests, capsys=None):
    """调 main(argv)，按 (method, url) 分发伪造响应。"""
    def _fake(method, url, payload=None, timeout=15):
        for key, resp in fake_requests.items():
            if url.endswith(key):
                if resp is None:
                    raise cli.CliError("HTTP 500: boom")
                if callable(resp):
                    return resp(method, payload)
                return resp
        raise AssertionError(f"未 mock 的请求：{method} {url}")

    with mock.patch("orchestration.cli._request", side_effect=_fake):
        return cli.main(argv)


SNAP = {
    "run_id": "r1", "status": "done",
    "started_at": "2026-08-18T00:00:00+00:00", "finished_at": "2026-08-18T00:00:01+00:00",
    "tasks": [{"id": "t1", "status": "success", "agent": "a"},
              {"id": "t2", "status": "success", "agent": "b"}],
}
REPORT = {
    "run_id": "r1", "final_status": "success", "total_cost": 0.02,
    "task_results": {"t1": {"success": True, "output": {"n": 1}, "error": None,
                            "cost": 0.01, "duration_ms": 100, "retries": 0},
                     "t2": {"success": True, "output": None, "error": None,
                            "cost": 0.01, "duration_ms": 100, "retries": 0}},
    "assignments": [{"task_id": "t1", "agent_id": "a", "match_type": "exact",
                     "risk": False, "reason": "覆盖能力：[]"}],
    "prune_reports": [],
}
METRICS = {
    "run_id": "r1", "duration_ms": 500, "tasks": {"total": 2, "success": 2,
    "failed": 0, "cancelled": 0, "skipped": 0}, "pruned_count": 0, "total_cost": 0.02,
    "agents": {"a": {"tasks": 1, "success_rate": 1.0, "avg_duration_ms": 100,
                     "peak_concurrency": 1, "cost": 0.01, "degraded": False}},
}


class TestSubmit:
    def test_submit_dag_json(self, tmp_path, capsys):
        dag_file = tmp_path / "dag.json"
        dag_file.write_text(json.dumps({
            "tasks": {"t1": {"desc": "t1"}, "t2": {"desc": "t2", "deps": ["t1"]}}
        }), encoding="utf-8")
        captured = {}

        def _fake(method, url, payload=None, timeout=15):
            captured["payload"] = payload
            return {"run_id": "r1", "status": "submitted"}

        with mock.patch("orchestration.cli._request", side_effect=_fake):
            rc = cli.main(["submit", str(dag_file)])

        assert rc == 0
        assert captured["payload"]["dag"]["tasks"]["t2"]["deps"] == ["t1"]
        out = capsys.readouterr().out
        assert "run_id: r1" in out

    def test_submit_run_id_flag(self, tmp_path, capsys):
        dag_file = tmp_path / "dag.json"
        dag_file.write_text('{"tasks": {"t1": {"desc": "t1"}}}', encoding="utf-8")
        captured = {}

        def _fake(method, url, payload=None, timeout=15):
            captured["payload"] = payload
            return {"run_id": "custom-1", "status": "submitted"}

        with mock.patch("orchestration.cli._request", side_effect=_fake):
            cli.main(["submit", str(dag_file), "--run-id", "custom-1"])
        assert captured["payload"]["run_id"] == "custom-1"


class TestQuery:
    def test_status_table(self, capsys):
        rc = run_cli(["status", "r1"], {"/api/runs/r1": SNAP})
        assert rc == 0
        out = capsys.readouterr().out
        assert "r1" in out and "done" in out
        assert "t1" in out and "a" in out

    def test_report_output(self, capsys):
        rc = run_cli(["report", "r1"], {"/api/runs/r1/report": REPORT})
        assert rc == 0
        out = capsys.readouterr().out
        assert "final_status: success" in out
        assert "总成本" in out
        assert "分配留痕" in out and "exact" in out

    def test_metrics_output(self, capsys):
        rc = run_cli(["metrics", "r1"], {"/api/runs/r1/metrics": METRICS})
        assert rc == 0
        out = capsys.readouterr().out
        assert "成功 2" in out

    def test_agents_output(self, capsys):
        agents = {"a": {"agent_id": "a", "model": "m1", "status": "available",
                        "capabilities": ["code_review"], "max_concurrency": 4,
                        "rate_limit_per_min": 60, "budget_limit_usd": 0.5,
                        "languages": ["python"]}}
        rc = run_cli(["agents"], {"/api/agents": agents})
        assert rc == 0
        out = capsys.readouterr().out
        assert "code_review" in out and "available" in out

    def test_agents_empty(self, capsys):
        rc = run_cli(["agents"], {"/api/agents": {}})
        assert rc == 0
        assert "空注册表" in capsys.readouterr().out


class TestControl:
    def test_cancel(self, capsys):
        calls = []
        with mock.patch("orchestration.cli._request") as req:
            req.return_value = {"run_id": "r1", "cancel_requested": True}
            rc = cli.main(["cancel", "r1"])
        assert rc == 0
        assert req.call_args[0][:2] == ("POST", cli._api("http://127.0.0.1:8000",
                                                         "/api/runs/r1/cancel"))

    def test_resume(self, capsys):
        with mock.patch("orchestration.cli._request") as req:
            req.return_value = {"run_id": "r1", "status": "resumed"}
            rc = cli.main(["resume", "r1"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "resumed" in out

    def test_resolve_cancel(self, capsys):
        calls = {}
        with mock.patch("orchestration.cli._request") as req:
            req.return_value = {"run_id": "r1", "task_id": "t1",
                                "action": "cancel", "status": "cancelled", "hint": ""}
            rc = cli.main(["resolve", "r1", "t1", "--action", "cancel"])
        assert rc == 0
        assert req.call_args[0][1].endswith("/api/runs/r1/tasks/t1/resolve")
        assert req.call_args[0][2] == {"action": "cancel"}

    def test_resolve_complete(self, capsys):
        with mock.patch("orchestration.cli._request") as req:
            req.return_value = {"run_id": "r1", "task_id": "t1",
                                "action": "complete", "status": "success", "hint": ""}
            rc = cli.main(["resolve", "r1", "t1", "--action", "complete",
                           "--result", '{"success": true, "task_id": "t1"}'])
        assert rc == 0
        payload = req.call_args[0][2]
        assert payload["action"] == "complete"
        assert payload["result"] == {"success": True, "task_id": "t1"}

    def test_resolve_complete_without_result(self, capsys):
        rc = run_cli(["resolve", "r1", "t1", "--action", "complete"],
                     {})
        assert rc == 1
        assert "需要 --result" in capsys.readouterr().err

    def test_resolve_bad_json_result(self, capsys):
        rc = run_cli(["resolve", "r1", "t1", "--action", "complete",
                      "--result", "{bad"], {})
        assert rc == 1
        assert "不是合法 JSON" in capsys.readouterr().err

    def test_unknown_action_rejected(self):
        with pytest.raises(SystemExit):
            cli.main(["resolve", "r1", "t1", "--action", "explode"])


class TestWait:
    def test_wait_until_done(self, capsys):
        state = {"calls": 0}

        def status_resp(method, payload):
            state["calls"] += 1
            if state["calls"] == 1:
                return {"run_id": "r1", "status": "running", "started_at": "",
                        "finished_at": "", "tasks": []}
            return SNAP

        rc = run_cli(["wait", "r1", "--timeout", "10"],
                     {"/api/runs/r1/report": REPORT,
                      "/api/runs/r1": status_resp})
        assert rc == 0
        assert state["calls"] == 2
        out = capsys.readouterr().out
        assert "final_status: success" in out


class TestErrors:
    def test_http_error_exit_code(self, capsys):
        rc = run_cli(["status", "nope"], {"/api/runs/nope": None})
        assert rc == 1
        assert "HTTP 500" in capsys.readouterr().err

    def test_unknown_command_rejected(self):
        with pytest.raises(SystemExit):
            cli.main(["frobnicate"])


class TestUrl:
    def test_custom_url_flag(self, tmp_path, capsys):
        dag_file = tmp_path / "d.json"
        dag_file.write_text('{"tasks": {"t1": {"desc": "t1"}}}', encoding="utf-8")
        with mock.patch("orchestration.cli._request") as req:
            req.return_value = {"run_id": "r1", "status": "submitted"}
            rc = cli.main(["-u", "http://host:9999", "submit", str(dag_file)])
        assert rc == 0
        assert req.call_args[0][1].startswith("http://host:9999")
