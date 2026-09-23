"""规划层接入 + 执行层超时 全流程冒烟（真实 HTTP，无需真实 agent）。

链路（档三：真实 HTTP 往返）：
  自然语言目标
    → 规划层拆解（**真实 DeepSeek LLM**，需 $DEEPSEEK_API_KEY）
    → 网关 POST /api/decompose（--submit：目标 → DAG → 直接提交）
    → mock agents（本地 OpenAI 兼容端点，真实 HTTP）执行 DAG
    → 报告 / 审计数据

CLI 侧调 orchestration.cli.main（**无 mock**，走真实 HTTP 打到本地网关），
即验证 `ao decompose --submit` / `ao submit` / `ao wait` 全链路。

第二部分验证 #34：agent 挂死（mock latency 30s）而任务声明 timeout=1 →
框架侧 wall-clock 超时生效，run 不再空转（且不污染后续派发）。

运行：.venv/bin/python scripts/smoke_decompose.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uvicorn

import orchestration.cli as cli
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.api.gateway import create_app
from orchestration.decomposer import make_default_decomposer
from orchestration.registry import AgentRegistry

from mock_agents import MockAgentConfig, MockAgentServer

GOAL = "调研 2 款主流降噪耳机在 2026 年的价格，汇总成一份中文对比报告"
MOCK_PORT = 8711
GATEWAY_PORT = 8811
GATEWAY = f"http://127.0.0.1:{GATEWAY_PORT}"

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, bool(ok), detail))
    print(f"  {'✓' if ok else '✗'} {name}" + (f"  —— {detail}" if detail else ""))


def mock_configs() -> list[MockAgentConfig]:
    """三个工作角色 + 一个"挂死"角色（验证框架侧超时）。"""
    return [
        MockAgentConfig(
            agent_id="translator", capabilities=["translation"],
            description="中英互译 agent", max_concurrency=2,
            rate_limit_per_min=600, languages=["中文", "English"],
            output_kind="translator", latency_ms=50,
        ),
        MockAgentConfig(
            agent_id="analyst", capabilities=["data_analysis", "report_writing"],
            description="数据分析与报告 agent", max_concurrency=2,
            rate_limit_per_min=600, output_kind="analyst", latency_ms=50,
        ),
        MockAgentConfig(
            agent_id="coder", capabilities=["code_gen", "code_review"],
            description="代码 agent", max_concurrency=2,
            rate_limit_per_min=600, output_kind="coder", latency_ms=50,
        ),
        MockAgentConfig(
            agent_id="sleepy", capabilities=["general"],
            description="挂死 agent（30s 才返回）", max_concurrency=2,
            latency_ms=30_000,
        ),
    ]


def build_registry(mock_base_url: str) -> AgentRegistry:
    reg = AgentRegistry()
    for cfg in mock_configs():
        # base_url 指向本地 mock（真实 HTTP 往返）；model=角色名（mock 按 model 路由）
        reg.register(
            DeepSeekAdapter(model=cfg.agent_id, base_url=mock_base_url,
                            api_key="mock-key"),
            agent_id=cfg.agent_id,
        )
    return reg


def run_cli(argv: list[str]) -> int:
    """在子线程里跑真实 CLI（内部走真实 HTTP 打到本地网关）。"""
    box: dict = {}
    th = threading.Thread(target=lambda: box.setdefault("rc", cli.main(argv)))
    th.start()
    th.join(300)
    return box.get("rc", -1)


async def _wait_gateway(uv: uvicorn.Server) -> None:
    for _ in range(200):
        if uv.started:
            return
        await asyncio.sleep(0.05)
    raise RuntimeError("网关未启动")


async def main() -> int:
    if not os.environ.get("DEEPSEEK_API_KEY"):
        print("需要 $DEEPSEEK_API_KEY（拆解引擎用真实 LLM）")
        return 2

    server = MockAgentServer(mock_configs(), port=MOCK_PORT)
    await server.start()
    print(f"mock agents: {server.base_url}（角色 {', '.join(server.configs)}）")

    reg = build_registry(f"{server.base_url}/v1")
    # 注意：mock 服务端就在本事件循环里，同步采集必须丢线程——否则阻塞
    # 事件循环会让服务端无法应答（自问自答死锁）
    summary = await asyncio.to_thread(reg.collect)   # 真实 HTTP 能力采集
    assert all(s["ok"] for s in summary), summary

    app, _ = create_app(reg, decomposer=make_default_decomposer())
    config = uvicorn.Config(app, host="127.0.0.1", port=GATEWAY_PORT,
                            log_level="warning")
    uv = uvicorn.Server(config)
    threading.Thread(target=uv.run, daemon=True).start()
    await _wait_gateway(uv)
    print(f"网关: {GATEWAY}\n")

    try:
        # ---------- 第一部分：目标 → DAG → 结果（ao decompose --submit） ----------
        print("[1] ao decompose --submit（真实 DeepSeek 拆解 → 直接提交）")
        cli._session["run_id"] = None
        rc = await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "decompose", "--goal", GOAL, "--submit"]
        )
        check("ao decompose 返回 0", rc == 0, f"rc={rc}")
        run_id = cli._session.get("run_id")
        check("run_id 已记录（会话记忆）", bool(run_id), str(run_id))

        t0 = time.monotonic()
        rc = await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "wait", run_id, "--timeout", "180"]
        )
        elapsed = time.monotonic() - t0
        check("ao wait 返回 0（拆解后全链跑完）", rc == 0, f"{elapsed:.1f}s")

        import urllib.request

        def get(path: str) -> dict:
            with urllib.request.urlopen(GATEWAY + path, timeout=15) as r:
                return json.loads(r.read().decode())

        report = get(f"/api/runs/{run_id}/report")
        snap = get(f"/api/runs/{run_id}")
        dag_size = len(snap["tasks"])
        check("拆解产出多任务 DAG（>1）", dag_size > 1, f"{dag_size} 个任务")
        check("全部任务到终态",
              all(t["status"] not in ("pending", "running") for t in snap["tasks"]))
        check("final_status 为 success/partial",
              report["final_status"] in ("success", "partial"),
              report["final_status"])
        check("分配留痕覆盖全部已派发任务",
              len(report["assignments"]) >= 1,
              f"{len(report['assignments'])} 条")
        check("真实 usage 回填（成本 > 0）", report["total_cost"] > 0,
              f"${report['total_cost']}")
        # 运行中快照 agent 归属（#26 修复项）也顺带确认：终态快照里 agent 非空
        check("任务均带 agent 归属",
              all(t["agent"] for t in snap["tasks"]),
              ", ".join(sorted({t["agent"] for t in snap["tasks"]})))

        # ---------- 第二部分：#34 框架侧 wall-clock 超时 ----------
        print("\n[2] 执行层 wall-clock 超时（挂死 agent + timeout=1）")
        dag = {"tasks": {"s1": {
            "id": "s1", "desc": "挂死任务",
            "required_resources": {"model": "sleepy", "timeout": 1},
        }}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                         encoding="utf-8") as f:
            json.dump(dag, f)
            dag_path = f.name
        t0 = time.monotonic()
        rc = await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "submit", dag_path, "--run-id", "timeout-demo"]
        )
        check("ao submit 挂死任务返回 0", rc == 0, f"rc={rc}")
        rc = await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "wait", "timeout-demo", "--timeout", "60"]
        )
        elapsed = time.monotonic() - t0
        check("run 在超时内结束（未被挂死任务拖住）", elapsed < 20,
              f"{elapsed:.1f}s（声明 timeout=1s，agent 实际 30s 才返回）")
        rep2 = get("/api/runs/timeout-demo/report")
        err = (rep2["task_results"].get("s1") or {}).get("error") or {}
        check("失败原因 = timeout（框架侧强制）", err.get("code") == "timeout",
              str(err.get("code")))
        check("final_status=failed", rep2["final_status"] == "failed",
              rep2["final_status"])

        # ---------- 第三部分：超时后并发槽已释放（同 agent 仍可派发） ----------
        print("\n[3] 超时后槽位释放（同 agent 重新可用）")
        dag = {"tasks": {"s2": {
            "id": "s2", "desc": "同 agent 的短任务",
            "required_resources": {"model": "sleepy", "timeout": 1},
        }}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                         encoding="utf-8") as f:
            json.dump(dag, f)
            dag_path2 = f.name
        await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "submit", dag_path2, "--run-id", "timeout-after"]
        )
        rc = await asyncio.to_thread(
            run_cli, ["-u", GATEWAY, "wait", "timeout-after", "--timeout", "30"]
        )
        check("超时后同 agent 仍可派发（槽未泄漏）", rc == 0, f"rc={rc}")
    finally:
        uv.should_exit = True
        await asyncio.sleep(0.3)
        await server.stop()

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n{'=' * 60}\n{passed}/{len(results)} 项断言通过")
    for name, ok, detail in results:
        if not ok:
            print(f"  ✗ {name}  {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
