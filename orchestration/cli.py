"""agents-orchestration CLI —— 网关的瘦客户端（架构 §3.1 接入层）。

定位：框架作为系统存在，上行是 HTTP API 网关。CLI **不绕过网关直连
scheduler**——它只是网关的运维 / 人工出口客户端：提交 DAG、查进度 /
报告 / 指标、取消、断点恢复、人工 resolve、看 agent 档案。

典型场景：
- 人工出口刚需：resolve（complete/cancel/retry）与 resume 本就是"人工"
  操作，curl 手拼 JSON 不友好——`ao resolve` 一步到位；
- 运维查询：`ao status/report/metrics/agents` 免记 URL；
- 零代码体验：装完包不用写 Python，`ao submit dag.json` 即提交。

依赖：仅标准库 urllib（网关是 FastAPI JSON 接口，无需额外 HTTP 客户端）。
网关地址：--url / -u，或环境变量 AO_GATEWAY，默认 http://127.0.0.1:8000。

`serve` 子命令需要 fastapi + uvicorn（pip install 'agents-orchestration[gateway]'）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

DEFAULT_GATEWAY = os.environ.get("AO_GATEWAY", "http://127.0.0.1:8000")


class CliError(Exception):
    """CLI 业务错误：打印消息后退出（不打印堆栈）。"""


# ---------------------------------------------------------------------------
# HTTP 层（标准库）
# ---------------------------------------------------------------------------

def _request(
    method: str,
    url: str,
    payload: dict | None = None,
    timeout: float = 15,
) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace").strip()
        raise CliError(f"HTTP {e.code}: {detail}") from None
    except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
        raise CliError(
            f"无法连接网关 {url}（{e}）。"
            f"先启动：ao serve --config agents.yaml"
        ) from None


def _api(gateway: str, path: str) -> str:
    return gateway.rstrip("/") + path


# ---------------------------------------------------------------------------
# 输出格式化
# ---------------------------------------------------------------------------

def _table(headers: list[str], rows: list[list[str]]) -> str:
    cols = list(zip(*rows)) if rows else [[] for _ in headers]
    widths = [
        max(len(h), *(len(c) for c in col)) for h, col in zip(headers, cols)
    ]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    sep = "  ".join("-" * w for w in widths)
    body = "\n".join(
        "  ".join(str(c).ljust(w) for c, w in zip(row, widths))
        for row in rows
    )
    return "\n".join([line, sep, body]) if rows else line


# ---------------------------------------------------------------------------
# 各子命令
# ---------------------------------------------------------------------------

def cmd_serve(args: argparse.Namespace) -> int:
    """启动 API 网关（框架作为系统的对外门）。"""
    try:
        import uvicorn
    except ImportError:
        raise CliError(
            "serve 需要 fastapi + uvicorn：pip install 'agents-orchestration[gateway]'"
        ) from None
    from orchestration.api.gateway import create_app
    from orchestration.registry import AgentRegistry
    from orchestration.state_store import SqliteStateStore

    if args.config:
        registry = AgentRegistry.from_config(args.config)
        print(f"[serve] 已从 {args.config} 注册 {len(registry.agents)} 个 agent："
              f"{', '.join(registry.agents)}")
        print("[serve] 正在采集能力声明（info_request）...")
        summary = registry.collect()
        ok = [s for s in summary if s["ok"]]
        print(f"[serve] 采集完成：{len(ok)}/{len(summary)} 成功")
    else:
        registry = AgentRegistry()
        print("[serve] 警告：未提供 --config，注册表为空（可编程式接入或后补）")
    store = SqliteStateStore(args.state_store) if args.state_store else None
    app, _ = create_app(registry, state_store=store)
    if store:
        print(f"[serve] 断点持久化已启用：{args.state_store}"
              f"（resume / resolve 人工出口可用）")
    print(f"[serve] 网关已启动：http://{args.host}:{args.port}")
    print("[serve] 查看 agent：ao agents | 提交：ao submit dag.json")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def cmd_submit(args: argparse.Namespace) -> int:
    with open(args.dag_file, "r", encoding="utf-8") as f:
        dag = json.load(f)
    payload: dict = {"dag": dag}
    if args.run_id:
        payload["run_id"] = args.run_id
    resp = _request("POST", _api(args.url, "/api/runs"), payload)
    print(f"run_id: {resp['run_id']}")
    print(f"status: {resp['status']}")
    print(f"查询进度：ao status {resp['run_id']} | 等待结束：ao wait {resp['run_id']}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    resp = _request("GET", _api(args.url, f"/api/runs/{args.run_id}"))
    print(f"run {resp['run_id']}  {resp['status']}"
          f"  开始 {resp['started_at'] or '-'}  结束 {resp['finished_at'] or '-'}")
    rows = [[t["id"], t["status"], t["agent"] or "-"] for t in resp["tasks"]]
    print(_table(["task", "status", "agent"], rows))
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    resp = _request("GET", _api(args.url, f"/api/runs/{args.run_id}/report"))
    print(f"final_status: {resp['final_status']}   总成本: ${resp['total_cost']}")
    rows = []
    for tid, r in resp["task_results"].items():
        err = (r.get("error") or {}).get("code", "") if not r["success"] else ""
        rows.append([tid, "success" if r["success"] else "failed",
                     json.dumps(r.get("output"), ensure_ascii=False) or "-",
                     err])
    print(_table(["task", "result", "output", "error_code"], rows))
    if resp["assignments"]:
        print("\n分配留痕：")
        rows = [[a["task_id"], a["agent_id"], a["match_type"],
                 "RISK" if a["risk"] else "", a["reason"]] for a in resp["assignments"]]
        print(_table(["task", "agent", "match", "risk", "reason"], rows))
    if resp["prune_reports"]:
        print("\n剪枝（失败传播）：")
        for p in resp["prune_reports"]:
            root = p["root_failure"]
            print(f"  ✕ 根失败 {root.get('task_id')}（{root.get('error', {}).get('code', '')}）"
                  f" → 剪枝 {len(p['pruned'])} 个任务"
                  + ("（含最终任务，整棵取消）" if p["pruned_final"] else ""))
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    resp = _request("GET", _api(args.url, f"/api/runs/{args.run_id}/metrics"))
    t = resp["tasks"]
    print(f"run {resp['run_id']}  耗时 {resp['duration_ms']}ms"
          f"  总成本 ${resp['total_cost']}")
    print(f"任务: 总 {t['total']} | 成功 {t['success']} | 失败 {t['failed']} | "
          f"取消 {t['cancelled']} | 跳过 {t['skipped']} | 剪枝 {resp['pruned_count']}")
    rows = []
    for aid, a in resp["agents"].items():
        rows.append([aid, str(a["tasks"]), f"{a['success_rate']*100:.0f}%",
                     str(a["avg_duration_ms"]), str(a["peak_concurrency"]),
                     f"${a['cost']}", "yes" if a["degraded"] else ""])
    print(_table(["agent", "tasks", "success", "avg_ms", "peak_conc",
                  "cost", "degraded"], rows))
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout
    while True:
        snap = _request("GET", _api(args.url, f"/api/runs/{args.run_id}"))
        if snap["status"] != "running":
            break
        if time.monotonic() > deadline:
            raise CliError(f"等待超时（{args.timeout}s），run 仍在运行中")
        time.sleep(1)
    return cmd_report(args)


def cmd_cancel(args: argparse.Namespace) -> int:
    resp = _request("POST", _api(args.url, f"/api/runs/{args.run_id}/cancel"))
    print(json.dumps(resp, ensure_ascii=False))
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    resp = _request("POST", _api(args.url, f"/api/runs/{args.run_id}/resume"))
    print(f"run_id: {resp['run_id']}  status: {resp['status']}")
    print(f"查询进度：ao status {resp['run_id']}")
    return 0


def cmd_resolve(args: argparse.Namespace) -> int:
    payload: dict = {"action": args.action}
    if args.action == "complete":
        if not args.result:
            raise CliError("complete 需要 --result '<json 结果契约>'")
        try:
            payload["result"] = json.loads(args.result)
        except json.JSONDecodeError:
            raise CliError("--result 不是合法 JSON") from None
    resp = _request(
        "POST",
        _api(args.url, f"/api/runs/{args.run_id}/tasks/{args.task_id}/resolve"),
        payload,
    )
    print(f"task {resp['task_id']} → {resp['status']}（action: {resp['action']}）")
    if resp.get("hint"):
        print(resp["hint"])
    return 0


def cmd_agents(args: argparse.Namespace) -> int:
    resp = _request("GET", _api(args.url, "/api/agents"))
    if not resp:
        print("（空注册表——无 agent）")
        return 0
    rows = []
    for aid, a in resp.items():
        rows.append([aid, a["model"], a["status"],
                     ", ".join(a["capabilities"]) or "-",
                     str(a["max_concurrency"]), f"${a['budget_limit_usd']}"])
    print(_table(["agent_id", "model", "status", "capabilities",
                  "max_conc", "budget"], rows))
    return 0


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ao",
        description="agents-orchestration 网关客户端（提交 DAG / 查询 / 人工出口）",
    )
    p.add_argument("-u", "--url", default=DEFAULT_GATEWAY,
                   help=f"网关地址（默认 {DEFAULT_GATEWAY}，或环境变量 AO_GATEWAY）")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("serve", help="启动 API 网关（需 fastapi+uvicorn）")
    sp.add_argument("--config", help="agents.yaml/.json——批量注册 N 个 agent")
    sp.add_argument("--state-store", help="SQLite 断点持久化文件（启用 resume/resolve）")
    sp.add_argument("--host", default="0.0.0.0")
    sp.add_argument("--port", type=int, default=8000)
    sp.set_defaults(func=cmd_serve)

    sp = sub.add_parser("submit", help="提交 DAG（JSON 文件）")
    sp.add_argument("dag_file", help="DAG JSON：{\"tasks\": {id: {desc, deps, ...}}}")
    sp.add_argument("--run-id")
    sp.set_defaults(func=cmd_submit)

    for name, help_text, fn in [
        ("status", "run 进度快照", cmd_status),
        ("report", "run 收尾报告（结果/分配/剪枝）", cmd_report),
        ("metrics", "run 运行指标", cmd_metrics),
        ("cancel", "请求取消 run", cmd_cancel),
        ("resume", "断点恢复（需启用 state_store）", cmd_resume),
        ("agents", "agent 注册表档案", cmd_agents),
    ]:
        sp = sub.add_parser(name, help=help_text)
        sp.add_argument("run_id") if name != "agents" else None
        sp.set_defaults(func=fn)

    sp = sub.add_parser("wait", help="等待 run 结束并打印报告")
    sp.add_argument("run_id")
    sp.add_argument("--timeout", type=float, default=300.0)
    sp.set_defaults(func=cmd_wait)

    sp = sub.add_parser("resolve", help="人工确认中断任务（仅 INTERRUPTED）")
    sp.add_argument("run_id")
    sp.add_argument("task_id")
    sp.add_argument("--action", required=True, choices=["complete", "cancel", "retry"])
    sp.add_argument("--result", help="complete 时的人工核实结果契约（JSON 字符串）")
    sp.set_defaults(func=cmd_resolve)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except CliError as e:
        print(f"错误：{e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\n已中断", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
