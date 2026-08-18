"""CLI 交互 shell 测试：REPL 会话、run_id 记忆、引导式 submit/resolve。

测试策略：mock builtins.input 逐行驱动 REPL，mock 网络层 _request 按
URL 分发响应；非交互命令行回归由 test_cli.py 覆盖。
"""
from __future__ import annotations

import json
from unittest import mock

import pytest

import orchestration.cli as cli

SNAP = {
    "run_id": "r1", "status": "done",
    "started_at": "2026-08-18T00:00:00+00:00", "finished_at": "2026-08-18T00:00:01+00:00",
    "tasks": [{"id": "t1", "status": "success", "agent": "a"}],
}
REPORT = {
    "run_id": "r1", "final_status": "success", "total_cost": 0.01,
    "task_results": {"t1": {"success": True, "output": {"n": 1}, "error": None,
                            "cost": 0.01, "duration_ms": 100, "retries": 0}},
    "assignments": [], "prune_reports": [],
}


@pytest.fixture(autouse=True)
def _reset_session():
    """每个测试前清空会话状态（模块级 _session 跨测试污染）。"""
    cli._session["run_id"] = None
    yield
    cli._session["run_id"] = None


def _dispatch(fake_requests):
    def _fake(method, url, payload=None, timeout=15):
        for key, resp in fake_requests.items():
            if url.endswith(key):
                if callable(resp):
                    return resp(method, payload)
                return resp
        raise AssertionError(f"未 mock 的请求：{method} {url}")
    return _fake


class TestReplSession:
    def test_no_args_enters_repl_with_run_id_memory(self, tmp_path, capsys):
        """submit 后 status 免 run_id——会话记忆生效。"""
        dag = tmp_path / "dag.json"
        dag.write_text('{"tasks": {"t1": {"desc": "t1"}}}', encoding="utf-8")
        requests = {
            "/api/runs": {"run_id": "r1", "status": "submitted"},
            "/api/runs/r1": SNAP,
        }
        inputs = [f"submit {dag}", "status", "exit"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request",
                        side_effect=_dispatch(requests)):
            rc = cli.main([])
        assert rc == 0
        out = capsys.readouterr().out
        assert "run_id: r1" in out
        assert "done" in out  # status 复用会话 run_id 返回快照

    def test_shell_command_is_alias(self, capsys):
        with mock.patch("builtins.input", side_effect=["exit"]):
            rc = cli.main(["shell"])
        assert rc == 0
        assert "交互 shell" in capsys.readouterr().out

    def test_error_does_not_exit_session(self, capsys):
        """无 run_id 报错后会话继续，下一条命令正常执行。"""
        inputs = ["status", "status r1", "exit"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request",
                        side_effect=_dispatch({"/api/runs/r1": SNAP})):
            rc = cli.main([])
        assert rc == 0
        captured = capsys.readouterr()
        assert "缺少 run_id" in captured.err
        assert "done" in captured.out

    def test_unknown_command_does_not_exit(self, capsys):
        """argparse 拒绝（SystemExit）被捕获，会话继续。"""
        inputs = ["frobnicate", "status r1", "exit"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request",
                        side_effect=_dispatch({"/api/runs/r1": SNAP})):
            rc = cli.main([])
        assert rc == 0
        assert "done" in capsys.readouterr().out

    def test_help_output(self, capsys):
        with mock.patch("builtins.input", side_effect=["help", "exit"]):
            rc = cli.main([])
        assert rc == 0
        out = capsys.readouterr().out
        assert "run_id 记忆" in out and "tab 补全" in out

    def test_quit_and_eof(self, capsys):
        with mock.patch("builtins.input", side_effect=["quit"]):
            assert cli.main([]) == 0
        with mock.patch("builtins.input", side_effect=["exit"]):
            assert cli.main([]) == 0
        with mock.patch("builtins.input", side_effect=EOFError):
            assert cli.main([]) == 0

    def test_explicit_run_id_overrides_session(self, tmp_path, capsys):
        dag = tmp_path / "dag.json"
        dag.write_text('{"tasks": {"t1": {"desc": "t1"}}}', encoding="utf-8")
        requests = {
            "/api/runs": {"run_id": "r1", "status": "submitted"},
            "/api/runs/r2": SNAP | {"run_id": "r2", "status": "done"},
        }
        inputs = [f"submit {dag}", "status r2", "exit"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request",
                        side_effect=_dispatch(requests)):
            rc = cli.main([])
        assert rc == 0
        # status r2 显式覆盖，不带 run_id 的记忆仍指向 r1
        assert cli._session["run_id"] == "r1"


class TestInteractiveSubmit:
    @pytest.fixture(autouse=True)
    def _force_tty(self, monkeypatch):
        monkeypatch.setenv("AO_INTERACTIVE", "1")

    def _submit(self, inputs, requests=None, capsys=None):
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request",
                        side_effect=_dispatch(requests or {})):
            return cli.main(["submit"])

    def test_guided_single_task(self, capsys):
        captured = {}

        def _fake(method, url, payload=None, timeout=15):
            captured["payload"] = payload
            return {"run_id": "r1", "status": "submitted"}

        inputs = ["把句子翻成英文", "", "", "y"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request", side_effect=_fake):
            rc = cli.main(["submit"])
        assert rc == 0
        dag = captured["payload"]["dag"]
        assert dag["tasks"]["t1"]["id"] == "t1"
        assert dag["tasks"]["t1"]["desc"] == "把句子翻成英文"
        assert "deps" not in dag["tasks"]["t1"]
        assert "run_id: r1" in capsys.readouterr().out

    def test_guided_multi_task_with_deps(self, capsys):
        captured = {}

        def _fake(method, url, payload=None, timeout=15):
            captured["payload"] = payload
            return {"run_id": "r1", "status": "submitted"}

        # 任务1: 翻译（无依赖）→ 任务2: 总结（依赖 t1）→ 空描述结束 → 确认
        inputs = ["把句子翻成英文", "", "总结上一步", "t1", "", "y"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request", side_effect=_fake):
            rc = cli.main(["submit"])
        assert rc == 0
        tasks = captured["payload"]["dag"]["tasks"]
        assert set(tasks) == {"t1", "t2"}
        assert tasks["t2"]["id"] == "t2"
        assert tasks["t2"]["deps"] == ["t1"]
        out = capsys.readouterr().out
        assert "共 2 个任务" in out

    def test_guided_abort_on_empty(self, capsys):
        """第一个描述就回车——不收集、不请求。"""
        with mock.patch("builtins.input", side_effect=[""]), \
             mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["submit"])
        assert rc == 0
        req.assert_not_called()
        assert "未收集到任务" in capsys.readouterr().out

    def test_guided_decline_confirmation(self, capsys):
        inputs = ["翻译", "", "", "n"]
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["submit"])
        assert rc == 0
        req.assert_not_called()

    def test_non_tty_without_file_errors(self, capsys, monkeypatch):
        monkeypatch.delenv("AO_INTERACTIVE", raising=False)
        with mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["submit"])
        assert rc == 1
        assert "缺少 dag_file" in capsys.readouterr().err


class TestInteractiveResolve:
    def test_guided_task_action_result(self, capsys, monkeypatch):
        monkeypatch.setenv("AO_INTERACTIVE", "1")
        cli._session["run_id"] = "r1"
        captured = {}

        def _fake(method, url, payload=None, timeout=15):
            captured["url"] = url
            captured["payload"] = payload
            return {"run_id": "r1", "task_id": "t2", "action": "complete",
                    "status": "success", "hint": ""}

        inputs = ["t2", "complete", '{"success": true, "task_id": "t2"}']
        with mock.patch("builtins.input", side_effect=inputs), \
             mock.patch("orchestration.cli._request", side_effect=_fake):
            rc = cli.main(["resolve"])
        assert rc == 0
        assert captured["url"].endswith("/api/runs/r1/tasks/t2/resolve")
        assert captured["payload"] == {"action": "complete",
                                       "result": {"success": True, "task_id": "t2"}}

    def test_guided_invalid_action_rejected(self, capsys, monkeypatch):
        monkeypatch.setenv("AO_INTERACTIVE", "1")
        cli._session["run_id"] = "r1"
        with mock.patch("builtins.input", side_effect=["t1", "explode"]), \
             mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["resolve"])
        assert rc == 1
        req.assert_not_called()
        assert "非法 action" in capsys.readouterr().err

    def test_non_tty_missing_task_id_errors(self, capsys, monkeypatch):
        monkeypatch.delenv("AO_INTERACTIVE", raising=False)
        with mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["resolve", "r1"])
        assert rc == 1
        assert "缺少 task_id" in capsys.readouterr().err


class TestNonInteractiveGuard:
    """非终端命令行：缺 run_id 给友好错误，不静默挂起。"""

    def test_status_without_run_id(self, capsys):
        with mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["status"])
        assert rc == 1
        req.assert_not_called()
        assert "缺少 run_id" in capsys.readouterr().err

    def test_resolve_without_run_id(self, capsys):
        with mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["resolve", "--action", "cancel"])
        assert rc == 1
        assert "缺少 run_id" in capsys.readouterr().err

    def test_wait_without_run_id(self, capsys):
        with mock.patch("orchestration.cli._request") as req:
            rc = cli.main(["wait"])
        assert rc == 1
        assert "缺少 run_id" in capsys.readouterr().err
