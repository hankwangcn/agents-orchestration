"""运行存档 + 接入层 Web 页面冒烟（档三：真实 HTTP + 真实 CLI + 真实网关）。

链路（对话共识：归档归持久化底座，过程口径=事件流，人读版=按需渲染）：
提交带 goal 的 run（mock agents 真实 HTTP 执行，含失败 / 剪枝 / 降级）→
**过程事件流**逐条落盘（状态变更即写）→ 终态报告归档（治理/学习产物挂回）→

① 运行枚举：`/api/runs`、`ao runs`（崩溃后 / 换进程后仍可发现已有 run）
② 人读投影：`/api/runs/{id}/view`——**确定性、可重放**（同一份存档 → 同一份视图）
③ 接入层 Web 页面：`/`（列表）+ `/runs/{id}`（详情：过程时间线 + 任务 + 治理 +
   学习）——**服务端渲染、同源、零构建、零 CORS**
④ 叙述摘要：`POST /api/runs/{id}/narrative`——LLM 产出、**非确定**、
   **显式触发**、单独留痕、**绝不默认生成、不混入确定性报告**

运行：.venv/bin/python scripts/smoke_archive.py（无需 API key——全部打到本地 mock 端点）
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading

import uvicorn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orchestration.cli as cli
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.api.gateway import create_app
from orchestration.models import DAG, ResourceRequirement, Task
from orchestration.narrative import Narrator
from orchestration.registry import AgentRegistry
from orchestration.state_store import SqliteStateStore

from mock_agents import MockAgentServer, default_configs

GATEWAY_PORT = 8241
GATEWAY = f"http://127.0.0.1:{GATEWAY_PORT}"
GOAL = "把抓取到的两地价格整理成一份比价报告"


def build_dag() -> DAG:
    """一次 run 同时产出失败（FP）、剪枝（PRU）与降级（DEG）——过程足够丰富。"""
    return DAG(tasks={
        "a": Task(id="a", desc="抓取站点 A 的价格",
                  required_resources=ResourceRequirement(model="flaky")),
        "b": Task(id="b", desc="抓取站点 B 的价格",
                  required_resources=ResourceRequirement(model="flaky")),
        "c": Task(id="c", desc="合并 A/B 价格", deps=["a"],
                  required_resources=ResourceRequirement(model="flaky")),
        "d": Task(id="d", desc="生成比价报告",
                  required_resources=ResourceRequirement(model="ghost"),
                  required_capabilities=["nonexistent_cap"]),
    })


def build_registry(base_url: str) -> AgentRegistry:
    reg = AgentRegistry()
    for agent_id in ("translator", "coder", "analyst", "flaky"):
        reg.register(
            DeepSeekAdapter(model=agent_id, base_url=f"{base_url}/v1",
                            api_key="mock"),
            agent_id=agent_id,
        )
    return reg


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"   {'✅' if ok else '❌'} {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def run_cli(argv: list[str]) -> int:
    """子线程里跑真实 CLI（内部走真实 HTTP 打到本地网关）。"""
    box: dict = {}
    th = threading.Thread(target=lambda: box.setdefault("rc", cli.main(argv)))
    th.start()
    th.join(120)
    return box.get("rc", -1)


def http_get(path: str) -> tuple[int, str]:
    """真实 HTTP GET（标准库），返回 (status, body)。"""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(GATEWAY + path, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def http_post(path: str) -> tuple[int, str]:
    import urllib.error
    import urllib.request
    req = urllib.request.Request(GATEWAY + path, data=b"{}", method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


async def _wait_gateway(uv: uvicorn.Server) -> None:
    for _ in range(200):
        if uv.started:
            return
        await asyncio.sleep(0.05)
    raise RuntimeError("网关未启动")


async def main(demo_dir: str | None = None) -> int:
    server = MockAgentServer(default_configs())
    await server.start()
    print(f"mock agents 已启动: {server.base_url}（角色: {', '.join(server.configs)}）")

    registry = build_registry(server.base_url)
    db_path = os.path.join(tempfile.mkdtemp(prefix="ao-smoke-archive-"), "state.db")
    store = SqliteStateStore(db_path)
    results: list[bool] = []

    def fake_narrator_llm(prompt: str) -> str:
        return "本次运行抓取了两地价格，合并与比价报告未能完成：一处抓取失败触发剪枝。"

    app, manager = create_app(
        registry, retries=0, state_store=store,
        narrator=Narrator(fake_narrator_llm, model="smoke-narrator"),
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=GATEWAY_PORT,
                            log_level="warning")
    uv = uvicorn.Server(config)
    threading.Thread(target=uv.run, daemon=True).start()
    await _wait_gateway(uv)
    print(f"   网关 {GATEWAY}（运行存档 + 接入层 Web）")

    try:
        print("\n== 1. 提交带 goal 的 run（真实 HTTP 执行）==")
        await registry.aensure_fresh()
        run_id = await manager.submit(build_dag(), goal=GOAL)
        report = await manager.wait(run_id)
        print(f"   run={run_id} final_status={report.final_status} "
              f"cost=${report.total_cost}")

        print("\n== 2. 过程事件流落盘（状态变更逐条，可回放）==")
        events = store.load_events(run_id)
        kinds = [e["event"] for e in events]
        print(f"   事件 {len(events)} 条：{kinds}")
        results.append(check("事件流首尾为 run_started / run_finished",
                             kinds[0] == "run_started" and kinds[-1] == "run_finished"))
        results.append(check("含派发与完成事件（过程可回放）",
                             "task_launched" in kinds and "task_done" in kinds
                             and "prune" in kinds))

        print("\n== 3. 运行枚举（跨进程可发现）==")
        st, body = http_get("/api/runs")
        runs = json.loads(body)["runs"]
        print(f"   GET /api/runs → {len(runs)} 条；{run_id} has_report="
              f"{next(r['has_report'] for r in runs if r['run_id'] == run_id)}")
        results.append(check("运行列表包含本 run 且标记已归档",
                             st == 200
                             and any(r["run_id"] == run_id and r["has_report"]
                                     for r in runs)))

        print("\n== 4. 人读投影：确定性、可重放 ==")
        s1, b1 = http_get(f"/api/runs/{run_id}/view")
        s2, b2 = http_get(f"/api/runs/{run_id}/view")
        v = json.loads(b1)
        print(f"   archive: events={v['archive']['event_count']} "
              f"final={v['archive']['final_status']} "
              f"started={v['archive']['started_at']}")
        print(f"   时间线 {len(v['timeline'])} 段；任务 "
              f"{[t['task_id'] + ':' + t['status'] for t in v['tasks']]}")
        results.append(check("投影含过程时间线 / 任务 / 治理 / 学习",
                             s1 == 200 and bool(v["timeline"]) and bool(v["tasks"])
                             and v["governance"]["audit"] is not None
                             and v["learning"]["rules"]))
        results.append(check("同一份存档 → 同一份视图（确定性、可重放）",
                             b1 == b2))

        print("\n== 5. 叙述摘要：非确定、显式触发、单独留痕 ==")
        results.append(check("未显式触发时叙述摘要为空（不默认生成）",
                             v["narrative"] is None))
        sn, bn = http_post(f"/api/runs/{run_id}/narrative")
        narr = json.loads(bn)
        print(f"   POST narrative → source={narr['source']} "
              f"deterministic={narr['deterministic']} model={narr['model']}")
        results.append(check("叙述显式标注来源（llm / 非确定）",
                             sn == 200 and narr["source"] == "llm"
                             and narr["deterministic"] is False))
        saved = store.load_narrative(run_id)
        results.append(check("叙述单独留痕（不与确定性报告合并）",
                             saved is not None and saved["text"] == narr["text"]))
        _, b3 = http_get(f"/api/runs/{run_id}/view")
        results.append(check("叙述并入视图的独立字段（生成后可见）",
                             json.loads(b3)["narrative"]["text"] == narr["text"]))

        print("\n== 6. 接入层 Web 页面（服务端渲染、同源）==")
        si, bi = http_get("/")
        sd, bd = http_get(f"/runs/{run_id}")
        print(f"   GET /           → {si}；GET /runs/{run_id} → {sd}")
        results.append(check("列表页含本 run 链接",
                             si == 200 and f"/runs/{run_id}" in bi))
        results.append(check("详情页含过程时间线 / 任务 / 学习规则",
                             sd == 200 and "过程时间线" in bd
                             and "任务与结果" in bd and "学习层规则" in bd))
        results.append(check("详情页标注唯一真源 = 运行存档",
                             "运行存档" in bd and "按需渲染" in bd))
        sm, bm = http_get("/runs/nope")
        results.append(check("未知 run → 错误页（非裸 JSON）",
                             sm == 404 and "无法打开" in bm))

        print("\n== 7. 真实 CLI：ao runs / ao view（走真实 HTTP）==")
        rc = await asyncio.to_thread(run_cli, ["-u", GATEWAY, "runs"])
        results.append(check("ao runs 打印运行枚举", rc == 0))
        rc = await asyncio.to_thread(run_cli, ["-u", GATEWAY, "view", run_id])
        results.append(check("ao view 打印人读视图（时间线 + 治理 + 学习）", rc == 0))

        print("\n== 8. mock 观测 ==")
        for aid in server.configs:
            print(f"   {aid}: calls={server.calls.get(aid, 0)}")
        print(f"   事件流 落盘于 {db_path}（run_events 表）")

        if demo_dir:
            from orchestration.report_view import render_index_html, render_run_html

            from pathlib import Path

            out = Path(demo_dir)
            out.mkdir(parents=True, exist_ok=True)
            view = manager.archive_view(run_id)
            view["narrative"] = narr  # 演示页展示叙述摘要（真实生成的那份）
            (out / "web_run.html").write_text(
                render_run_html(view), encoding="utf-8"
            )
            (out / "web_index.html").write_text(
                render_index_html(manager.list_runs()), encoding="utf-8"
            )
            print(f"\n   已写出 Web 演示页 → {demo_dir}/web_run.html, web_index.html")

    finally:
        await server.stop()

    ok = all(results)
    print("\n== 冒烟结论 ==")
    print("PASS：运行存档闭环 + 接入层 Web 页面（事件流 → 人读投影 → Web / 叙述）"
          if ok else f"FAIL：{results.count(False)} 项断言未通过")
    return 0 if ok else 1


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="运行存档 + Web 页面冒烟")
    ap.add_argument("--demo-dir", help="额外写出 Web 演示页（web_index/web_run.html）")
    ns = ap.parse_args()
    sys.exit(asyncio.run(main(ns.demo_dir)))
