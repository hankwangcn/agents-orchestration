"""运行存档的人读投影（接入层 Web / CLI 共用）。

定位（对话共识）：**运行存档**（过程事件流 + 终态报告，落持久化底座）是唯一
真源、单 run 不可变；**人读版是读时投影**——按需从真源渲染，不落盘成第二份
真相（同"源 .md → HTML 发布副本"范式：源不动、视图按需）。

分层（不可混）：
- **结构化人读版**（本模块 `build_run_view`）＝**纯确定性投影**：时间线 / 任务表
  / 成本 / 治理结论 / 学习规则，全部来自存档事实；渲染确定性、可重放、零成本。
- **叙述性摘要**（`narrative.py` 产出、经 `narrative` 字段并入）＝LLM 产出、
  **非确定、有成本**——显式触发、单独成节、标注来源，**绝不默认生成、不得混入
  确定性报告**（同"判定 vs 审计"的分离口径）。

因此 `build_run_view` 是纯函数：同一份存档 → 同一份视图（不含渲染时刻等易变
元数据；时间戳只出现在 HTML/文本页脚）。渲染（`render_text` / `render_run_html`
/ `render_index_html`）只读视图，不读存档、不触网。
"""
from __future__ import annotations

import html as _h
from typing import Any, Optional

__all__ = [
    "build_run_view",
    "render_index_html",
    "render_run_html",
    "render_text",
]

_STATUS_LABEL = {
    "success": "成功", "failed": "失败", "cancelled": "已取消",
    "interrupted": "中断", "skipped": "跳过", "pending": "待执行",
    "running": "执行中",
}
_STATUS_COLOR = {
    "success": "#16a34a", "failed": "#dc2626", "cancelled": "#94a3b8",
    "interrupted": "#f59e0b", "skipped": "#64748b", "pending": "#0284c7",
    "running": "#0284c7",
}
_FINAL_COLOR = {
    "success": "#16a34a", "partial": "#f59e0b", "failed": "#dc2626",
    "cancelled": "#94a3b8", "interrupted": "#f59e0b", "running": "#0284c7",
}
_VERDICT_COLOR = {"ok": "#16a34a", "warning": "#f59e0b", "critical": "#dc2626"}
_SEV_COLOR = {"high": "#dc2626", "medium": "#f59e0b", "low": "#94a3b8"}
_TIER_LABEL = {"objective": "客观", "judgment": "判定"}


# ---------------------------------------------------------------------------
# 确定性投影
# ---------------------------------------------------------------------------

def build_run_view(
    *,
    run_id: str,
    goal: str = "",
    run_status: str = "",
    dag: Optional[dict] = None,
    events: Optional[list[dict]] = None,
    report: Optional[dict] = None,
    narrative: Optional[dict] = None,
) -> dict:
    """把运行存档投影为结构化人读视图（纯函数、确定性）。

    dag：DAG 转储 {tasks: {tid: {...}}}（from 报告体或存档；含每任务 status/result）
    events：过程事件流（增量时序，唯一过程真源）
    report：终态报告转储（ScheduleReport dump，含治理/学习产物）；运行中为 None
    narrative：叙述摘要（非确定，None = 未生成）
    """
    events = list(events or [])
    # 报告体自带 dag（终态）优先；否则用存档 dag（可能运行中，状态为当前值）
    dag_tasks: dict = {}
    if report and isinstance(report.get("dag"), dict):
        dag_tasks = (report["dag"] or {}).get("tasks") or {}
    if not dag_tasks and dag:
        dag_tasks = dag.get("tasks") or {}

    assigned = {
        a.get("task_id"): a
        for a in (report or {}).get("assignments") or []
        if a.get("task_id")
    }

    task_rows = _task_rows(dag_tasks, assigned)
    timeline = _timeline(events)
    summary = _summary(task_rows)
    started_at = events[0].get("ts", "") if events else ""
    finished_at = ""
    duration_ms = 0
    for e in events:
        if e.get("event") == "run_finished":
            finished_at = e.get("ts", "")
            duration_ms = int((e.get("data") or {}).get("duration_ms") or 0)
    if not finished_at and events:
        finished_at = events[-1].get("ts", "")

    final_status = (report or {}).get("final_status") or run_status or "running"

    return {
        "run_id": run_id,
        "goal": goal or "",
        "archive": {
            "run_status": run_status or "",
            "final_status": final_status,
            "total_cost": round(float((report or {}).get("total_cost") or 0.0), 6),
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "event_count": len(events),
            "report_available": report is not None,
            "source": "运行存档（事件流 + 终态报告）——持久化底座的不可变真源",
        },
        "summary": summary,
        "timeline": timeline,
        "tasks": task_rows,
        "assignments": _assignment_rows(report),
        "prunes": _prune_rows(report),
        "governance": {
            "audit": (report or {}).get("audit"),
            "cost": (report or {}).get("cost"),
            "reflection": (report or {}).get("reflection"),
        },
        "learning": _learning(report),
        "narrative": narrative or None,
    }


def _task_rows(dag_tasks: dict, assigned: dict) -> list[dict]:
    rows: list[dict] = []
    for tid, t in dag_tasks.items():
        res = t.get("result") or {}
        usage = res.get("usage") or {}
        err = res.get("error") or {}
        ass = assigned.get(tid) or {}
        rows.append({
            "task_id": tid,
            "desc": t.get("desc", ""),
            "status": t.get("status", ""),
            "deps": list(t.get("deps") or []),
            "side_effects": t.get("side_effects", "none"),
            "agent_id": ass.get("agent_id", ""),
            "match_type": ass.get("match_type", ""),
            "risk": bool(ass.get("risk")),
            "assign_reason": ass.get("reason", ""),
            "success": res.get("success"),
            "duration_ms": int(res.get("duration_ms") or 0),
            "cost": round(float(usage.get("cost") or 0.0), 6),
            "tokens_in": int(usage.get("tokens_in") or 0),
            "tokens_out": int(usage.get("tokens_out") or 0),
            "retries": int(res.get("retries") or 0),
            "error_code": err.get("code", ""),
            "output": res.get("output"),
        })
    return rows


def _summary(task_rows: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for r in task_rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    total = len(task_rows)
    success = counts.get("success", 0)
    return {
        "tasks_total": total,
        "by_status": counts,
        "success": success,
        "failed": counts.get("failed", 0),
        "cancelled": counts.get("cancelled", 0),
        "skipped": counts.get("skipped", 0),
        "interrupted": counts.get("interrupted", 0),
        "success_rate": round(success / total, 4) if total else 0.0,
    }


def _timeline(events: list[dict]) -> list[dict]:
    out: list[dict] = []
    for e in events:
        out.append({
            "ts": e.get("ts", ""),
            "event": e.get("event", ""),
            "task_id": e.get("task_id", ""),
            "agent_id": e.get("agent_id", ""),
            "detail": _event_detail(e),
        })
    return out


def _event_detail(e: dict) -> str:
    ev = e.get("event", "")
    d = e.get("data") or {}
    tid = e.get("task_id", "")
    if ev == "run_started":
        return f"运行开始（{d.get('dag_size', '?')} 个任务）"
    if ev == "task_launched":
        return (f"派发 {tid} → {e.get('agent_id', '')}"
                f"（{d.get('match_type', '')}）")
    if ev == "task_done":
        if d.get("success"):
            return (f"完成 {tid}：成功（{d.get('duration_ms', 0)}ms，"
                    f"${d.get('cost', 0)}）")
        return f"完成 {tid}：失败（{d.get('error_code') or 'unknown'}）"
    if ev == "prune":
        tail = "，波及最终交付（整棵取消）" if d.get("pruned_final") else ""
        return (f"失败传播剪枝：根失败 {tid} → 取消 {d.get('pruned_count', 0)} 个任务"
                f"{tail}")
    if ev == "result_dropped":
        return f"丢弃晚到结果 {tid}（任务已取消）"
    if ev == "run_resumed":
        return (f"断点恢复：重派 {len(d.get('rerun') or [])} 个 / "
                f"中断待人工 {len(d.get('interrupted') or [])} 个")
    if ev == "run_cancelled":
        return f"运行被取消（{d.get('cancelled_tasks', 0)} 个任务置取消）"
    if ev == "run_finished":
        return (f"运行结束：{d.get('final_status', '')}"
                f"（总成本 ${d.get('total_cost', 0)}，"
                f"耗时 {d.get('duration_ms', 0)}ms）")
    return ev


def _assignment_rows(report: Optional[dict]) -> list[dict]:
    rows: list[dict] = []
    for a in (report or {}).get("assignments") or []:
        rows.append({
            "task_id": a.get("task_id", ""),
            "agent_id": a.get("agent_id", ""),
            "match_type": a.get("match_type", ""),
            "risk": bool(a.get("risk")),
            "reason": a.get("reason", ""),
        })
    return sorted(rows, key=lambda r: r["task_id"])


def _prune_rows(report: Optional[dict]) -> list[dict]:
    rows: list[dict] = []
    for p in (report or {}).get("prune_reports") or []:
        rows.append({
            "root_failure": p.get("root_failure") or {},
            "pruned": list(p.get("pruned") or []),
            "pruned_final": bool(p.get("pruned_final")),
        })
    return rows


def _learning(report: Optional[dict]) -> dict:
    raw = (report or {}).get("learning") or {}
    rules = []
    for r in raw.get("rules") or []:
        rules.append({
            "rule_id": r.get("rule_id", ""),
            "severity": r.get("severity", ""),
            "category": r.get("category", ""),
            "tier": r.get("tier") or "objective",
            "message": r.get("message", ""),
            "action": r.get("action", ""),
            "evidence": r.get("evidence") or {},
        })
    return {"rules": rules, "rule_count": len(rules)}


# ---------------------------------------------------------------------------
# 文本渲染（CLI：ao view）
# ---------------------------------------------------------------------------

def render_text(view: dict) -> str:
    """把视图渲染为纯文本（人读；确定性）。"""
    a = view["archive"]
    lines: list[str] = []
    lines.append(f"run {view['run_id']}  {a['final_status']}"
                 f"  开始 {a['started_at'] or '-'}  结束 {a['finished_at'] or '-'}")
    if view.get("goal"):
        lines.append(f"目标: {view['goal']}")
    lines.append(
        f"存档: 事件 {a['event_count']} 条 · 终态报告 "
        f"{'已归档' if a['report_available'] else '未产出（运行中）'}"
        f" · 总成本 ${a['total_cost']}"
    )

    s = view["summary"]
    lines.append(
        f"任务: 总 {s['tasks_total']} | 成功 {s['success']} | 失败 {s['failed']}"
        f" | 取消 {s['cancelled']} | 跳过 {s['skipped']} | 中断 {s['interrupted']}"
        f" | 成功率 {s['success_rate'] * 100:.0f}%"
    )

    if view["timeline"]:
        lines.append("\n过程时间线：")
        for e in view["timeline"]:
            lines.append(f"  {e['ts']}  {e['detail']}")

    if view["tasks"]:
        lines.append("\n任务：")
        for t in view["tasks"]:
            tail = f"  {t['error_code']}" if t["error_code"] else ""
            lines.append(
                f"  {t['task_id']:<8} {_STATUS_LABEL.get(t['status'], t['status']):<4}"
                f"  {t['agent_id'] or '-':<12} {t['match_type'] or '-':<10}"
                f"  {t['duration_ms']}ms  ${t['cost']:.4f}"
                f"{'  ⚠' if t['risk'] else ''}{tail}"
            )

    g = view["governance"]
    if g.get("audit"):
        au = g["audit"]
        line = (f"\n审计：{au.get('verdict')}  成功率 "
                f"{(au.get('success_rate') or 0) * 100:.0f}%")
        if au.get("issues"):
            line += "  问题：" + "；".join(au["issues"])
        lines.append(line)
    if g.get("cost"):
        c = g["cost"]
        lines.append(f"成本：总 ${c.get('total_cost')}  失败沉没 ${c.get('failed_cost')}"
                     f"  剪枝沉没 ${c.get('pruned_cost')}")
    if g.get("reflection"):
        lines.append(_reflection_line(g["reflection"]))

    rules = view["learning"]["rules"]
    if rules:
        lines.append(f"\n学习：{len(rules)} 条规则")
        for r in rules:
            lines.append(
                f"  {r['rule_id']:<12} {r['severity']:<6}"
                f" [{_TIER_LABEL.get(r['tier'], r['tier'])}]  {r['message']}"
            )

    if view.get("narrative"):
        n = view["narrative"]
        lines.append(f"\n叙述摘要（非确定来源 · LLM 生成 · model={n.get('model', '')}）：")
        lines.append(f"  {n.get('text', '')}")
    return "\n".join(lines)


def _reflection_line(ref: dict) -> str:
    if not ref.get("enabled"):
        return f"\n判定：未执行（{ref.get('skipped_reason') or '未知原因'}）"
    if ref.get("error_code"):
        return f"\n判定：未产出结论（{ref['error_code']}）"
    out = f"\n判定：{'达成' if ref.get('achieved') else '未达成'}"
    if ref.get("score") is not None:
        out += f"（score {ref['score']}）"
    out += f"  判定者 {ref.get('judge_agent') or '-'}"
    if not ref.get("independent", True):
        out += "（非独立·自判）"
    return out


# ---------------------------------------------------------------------------
# HTML 渲染（接入层 Web 页面；自包含、零依赖、同源）
# ---------------------------------------------------------------------------

_CSS = """
:root{--ink:#0f172a;--ink2:#334155;--ink3:#64748b;--ink4:#94a3b8;
 --line:#e2e8f0;--bg:#f1f5f9;--card:#fff;}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:system-ui,-apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
 background:var(--bg);color:var(--ink);line-height:1.55;}
a{color:#0284c7;text-decoration:none;} a:hover{text-decoration:underline;}
.wrap{max-width:1080px;margin:0 auto;padding:24px 20px 60px;}
header{background:linear-gradient(135deg,#0f172a,#1e293b 60%,#312e81 130%);
 color:#fff;border-radius:16px;padding:22px 26px;box-shadow:0 10px 30px rgba(15,23,42,.18);}
header .top{display:flex;align-items:center;gap:14px;flex-wrap:wrap;}
.logo{width:46px;height:46px;border-radius:12px;flex:0 0 auto;
 background:linear-gradient(135deg,#0ea5e9,#6366f1);display:flex;align-items:center;
 justify-content:center;font-weight:800;font-size:17px;color:#fff;}
h1{font-size:20px;font-weight:700;} header .sub{color:#cbd5e1;font-size:12.5px;margin-top:3px;}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;color:#fff;
 font-size:11px;font-weight:600;white-space:nowrap;}
.cards{display:flex;flex-wrap:wrap;gap:12px;margin:18px 0;}
.cell{background:#fff;border:1px solid var(--line);border-radius:12px;padding:10px 18px;
 min-width:104px;text-align:center;} .cell-v{font-size:19px;font-weight:700;}
.cell-k{font-size:11.5px;margin-top:2px;color:var(--ink3);}
.card{background:#fff;border:1px solid var(--line);border-radius:14px;padding:16px 20px;margin:14px 0;
 box-shadow:0 1px 3px rgba(15,23,42,.04);}
.card h2{font-size:14.5px;margin:0 0 12px;color:var(--ink);}
table{border-collapse:collapse;width:100%;font-size:12.5px;}
th{text-align:left;color:var(--ink3);font-weight:600;padding:6px 8px;border-bottom:2px solid var(--line);}
td{padding:6px 8px;border-bottom:1px solid #eef2f7;vertical-align:top;}
code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px;}
.muted{color:var(--ink4);}
.tl{list-style:none;font-size:12.5px;}
.tl li{display:flex;gap:12px;padding:5px 0;border-bottom:1px dashed #eef2f7;}
.tl .ts{color:var(--ink4);font-family:ui-monospace,monospace;font-size:11.5px;white-space:nowrap;min-width:150px;}
.chips{display:flex;flex-wrap:wrap;gap:5px;margin-top:8px;}
.chips span{font-size:10.5px;color:var(--ink2);background:#eef2f7;border:1px solid var(--line);
 border-radius:999px;padding:2px 8px;}
.grid3{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;}
.grid3 .card{margin:0;}
ul.tight{margin:6px 0;padding-left:18px;font-size:12.5px;} ul.tight li{margin:3px 0;}
.notice{background:#fffbeb;border:1px solid #fde68a;border-radius:10px;padding:10px 14px;
 font-size:12px;color:#92400e;}
.narrative{background:#faf5ff;border:1px solid #e9d5ff;border-radius:12px;padding:14px 16px;font-size:13px;}
footer{margin-top:26px;text-align:center;font-size:11.5px;color:var(--ink4);}
.runrow td{padding:9px 8px;} .runrow .goal{max-width:520px;}
"""


def _esc(v: Any) -> str:
    return _h.escape("" if v is None else str(v))


def _badge(text: str, color: str) -> str:
    return f'<span class="badge" style="background:{color}">{_esc(text)}</span>'


def _head(title: str, subtitle: str = "") -> str:
    return f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{_esc(title)}</title><style>{_CSS}</style></head><body><div class="wrap">
<header><div class="top"><div class="logo">AO</div>
<div><h1>{_esc(title)}</h1>{f'<div class="sub">{_esc(subtitle)}</div>' if subtitle else ''}</div>
</div></header>"""


def _foot(note: str = "") -> str:
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    n = f" · {_esc(note)}" if note else ""
    return (f'<footer>Agents Orchestration · 由运行存档按需渲染（确定性投影）'
            f' · {ts}{n}</footer></div></body></html>')


def render_run_html(view: dict) -> str:
    """运行详情页（整条业务完成过程：时间线 + 任务 + 治理 + 学习）。"""
    a = view["archive"]
    s = view["summary"]
    final = a["final_status"]
    parts: list[str] = [_head(f"run {view['run_id']}",
                              view.get("goal") or "（未提交原始目标）")]
    parts.append(
        '<div class="cards">'
        + _cell(_STATUS_LABEL.get(final, final), "最终状态",
                _FINAL_COLOR.get(final, "#64748b"))
        + _cell(f"{s['success']}/{s['tasks_total']}", "成功/总数", "#16a34a")
        + _cell(f"{s['success_rate'] * 100:.0f}%", "成功率", "#0284c7")
        + _cell(a["event_count"], "过程事件", "#6d28d9")
        + _cell(f"${a['total_cost']}", "总成本", "#0f172a")
        + _cell(f"{a['duration_ms']}ms", "耗时", "#0f172a")
        + '</div>'
    )
    if not a["report_available"]:
        parts.append('<div class="notice">终态报告尚未产出（run 运行中或未收尾）'
                     '——以下为过程事件流投影。</div>')

    parts.append(_timeline_html(view["timeline"]))
    parts.append(_tasks_html(view["tasks"]))
    if view["assignments"]:
        parts.append(_assign_html(view["assignments"]))
    if view["prunes"]:
        parts.append(_prune_html(view["prunes"]))
    parts.append(_gov_html(view["governance"], view["learning"]))
    parts.append(_narrative_html(view.get("narrative")))
    parts.append(_foot("唯一真源：运行存档；本页为按需读时投影"))
    return "".join(parts)


def _cell(value: Any, key: str, color: str) -> str:
    return (f'<div class="cell"><div class="cell-v" style="color:{color}">'
            f'{_esc(value)}</div><div class="cell-k">{_esc(key)}</div></div>')


def _timeline_html(timeline: list[dict]) -> str:
    if not timeline:
        return ('<div class="card"><h2>过程时间线</h2>'
                '<div class="muted">无过程事件（未启用事件流归档）</div></div>')
    items = "".join(
        f'<li><span class="ts">{_esc(e["ts"])}</span>'
        f'<span>{_esc(e["detail"])}</span></li>'
        for e in timeline
    )
    return (f'<div class="card"><h2>过程时间线（{len(timeline)} 个事件）</h2>'
            f'<ul class="tl">{items}</ul></div>')


def _tasks_html(tasks: list[dict]) -> str:
    if not tasks:
        return ""
    rows = ""
    for t in tasks:
        color = _STATUS_COLOR.get(t["status"], "#64748b")
        risk = (" " + _badge("risk", "#dc2626")) if t["risk"] else ""
        err = f' <span class="muted">{_esc(t["error_code"])}</span>' if t["error_code"] else ""
        deps = ", ".join(t["deps"]) or "—"
        rows += (
            "<tr>"
            f'<td><b>{_esc(t["task_id"])}</b></td>'
            f'<td>{_esc(t["desc"])}</td>'
            f'<td><span class="mono">{_esc(deps)}</span></td>'
            f'<td>{_badge(_STATUS_LABEL.get(t["status"], t["status"]), color)}{err}</td>'
            f'<td>{_esc(t["agent_id"]) or "—"}{risk}'
            f'<div class="muted mono">{_esc(t["match_type"])}</div></td>'
            f'<td>{t["duration_ms"]}ms</td>'
            f'<td>${t["cost"]:.4f}</td>'
            "</tr>"
        )
    return (
        '<div class="card"><h2>任务与结果</h2><table><thead><tr>'
        "<th>任务</th><th>描述</th><th>依赖</th><th>状态</th><th>分配</th>"
        "<th>耗时</th><th>成本</th></tr></thead><tbody>"
        + rows + "</tbody></table></div>"
    )


def _assign_html(rows: list[dict]) -> str:
    body = ""
    for a in rows:
        risk = (" " + _badge("risk", "#dc2626")) if a["risk"] else ""
        body += (
            "<tr>"
            f'<td><b>{_esc(a["task_id"])}</b></td>'
            f'<td>{_esc(a["agent_id"])}</td>'
            f'<td class="mono">{_esc(a["match_type"])}{risk}</td>'
            f'<td>{_esc(a["reason"])}</td>'
            "</tr>"
        )
    return (
        '<div class="card"><h2>分配留痕（三级策略）</h2><table><thead><tr>'
        "<th>任务</th><th>agent</th><th>匹配</th><th>说明</th></tr></thead><tbody>"
        + body + "</tbody></table></div>"
    )


def _prune_html(prunes: list[dict]) -> str:
    out = ""
    for p in prunes:
        root = p["root_failure"]
        out += (f'<div style="margin:6px 0">{_badge("根失败", "#dc2626")} '
                f'<b>{_esc(root.get("task_id"))}</b> — {_esc(root.get("reason"))}'
                f'（重试 {_esc(root.get("retries"))} 次耗尽）</div>')
        for it in p["pruned"]:
            out += (f'<div class="muted" style="margin:3px 0 3px 18px;font-size:12.5px">'
                    f'✂ <b>{_esc(it.get("task_id"))}</b>'
                    f'（取消时 {_esc(it.get("state_at_cancel"))}）— '
                    f'{_esc(it.get("prune_reason"))}</div>')
        if p["pruned_final"]:
            out += ('<div class="muted" style="margin-left:18px">⚠ 全部最终交付点'
                    '被波及，整棵 DAG 取消</div>')
    return f'<div class="card"><h2>失败传播与剪枝</h2>{out}</div>'


def _gov_html(gov: dict, learning: dict) -> str:
    audit = gov.get("audit")
    cost = gov.get("cost")
    ref = gov.get("reflection")
    cards = ""

    if audit:
        v = audit.get("verdict", "")
        issues = "".join(f"<li>{_esc(i)}</li>" for i in audit.get("issues") or []) \
            or "<li class='muted'>无问题</li>"
        cards += (f'<div class="card"><h2>审计 {_badge(v, _VERDICT_COLOR.get(v, "#64748b"))}'
                  f'</h2><ul class="tight">{issues}</ul></div>')
    if cost:
        cards += (f'<div class="card"><h2>成本</h2><ul class="tight">'
                  f'<li>总成本 <b>${_esc(cost.get("total_cost"))}</b>'
                  f'（in {_esc(cost.get("total_tokens_in"))} + '
                  f'out {_esc(cost.get("total_tokens_out"))} tokens）</li>'
                  f'<li>失败消耗 ${_esc(cost.get("failed_cost"))}　'
                  f'剪枝沉没 ${_esc(cost.get("pruned_cost"))}</li>'
                  + "".join(f'<li>{_esc(m.get("match_type"))}：'
                            f'${_esc(m.get("cost"))}</li>'
                            for m in cost.get("by_match_type") or [])
                  + '</ul></div>')
    if ref:
        cards += f'<div class="card"><h2>反思 / 判定（advisory）</h2>{_reflection_html(ref)}</div>'

    rules = learning.get("rules") or []
    if rules:
        body = ""
        for r in rules:
            tier = _TIER_LABEL.get(r["tier"], r["tier"])
            body += (
                "<tr>"
                f'<td class="mono"><b>{_esc(r["rule_id"])}</b></td>'
                f'<td>{_badge(r["severity"], _SEV_COLOR.get(r["severity"], "#64748b"))}</td>'
                f'<td>{_esc(tier)}</td>'
                f'<td>{_esc(r["message"])}'
                + (f'<div class="muted">→ {_esc(r["action"])}</div>' if r["action"] else "")
                + "</td></tr>"
            )
        cards += ('<div class="card"><h2>学习层规则（证据强度分级：客观 / 判定）</h2>'
                  '<table><thead><tr><th>规则</th><th>严重度</th><th>分级</th>'
                  '<th>结论 / 建议</th></tr></thead><tbody>'
                  + body + "</tbody></table></div>")

    if not cards:
        return ""
    return f'<div class="grid3">{cards}</div>'


def _reflection_html(ref: dict) -> str:
    if not ref.get("enabled"):
        return (f'<div class="muted">未执行（{_esc(ref.get("skipped_reason") or "未知原因")}）'
                '</div>')
    if ref.get("error_code"):
        return f'<div class="muted">未产出结论（{_esc(ref["error_code"])}）</div>'
    head = ("达成" if ref.get("achieved") else "未达成")
    color = "#16a34a" if ref.get("achieved") else "#dc2626"
    score = f'（score {_esc(ref["score"])}）' if ref.get("score") is not None else ""
    judge = _esc(ref.get("judge_agent") or "-")
    indep = "" if ref.get("independent", True) else "（非独立·自判）"
    reasons = "".join(f"<li>{_esc(r)}</li>" for r in ref.get("reasons") or [])
    gaps = "".join(f"<li>✕ 缺口：{_esc(g)}</li>" for g in ref.get("gaps") or [])
    return (f'<div>{_badge(head, color)} {score}　判定者 {judge}{indep}</div>'
            f'<ul class="tight">{reasons}{gaps}</ul>')


def _narrative_html(narrative: Optional[dict]) -> str:
    if not narrative:
        return ('<div class="card"><h2>叙述摘要</h2>'
                '<div class="muted">未生成——叙述摘要是 LLM 产出的非确定内容，'
                '需<b>显式触发</b>（POST /api/runs/{run_id}/narrative），'
                '不默认生成、不混入确定性报告。</div></div>')
    return ('<div class="card"><h2>叙述摘要 '
            + _badge("非确定 · LLM 生成", "#6d28d9") +
            '</h2><div class="narrative">' + _esc(narrative.get("text", "")) +
            f'</div><div class="muted mono" style="margin-top:8px">'
            f'model={_esc(narrative.get("model"))} · '
            f'generated_at={_esc(narrative.get("created_at"))}</div></div>')


def render_index_html(runs: list[dict], *, title: str = "运行存档") -> str:
    """运行列表页（run 枚举 + 跳转详情）。"""
    parts = [_head(title, "已完成 / 进行中的运行——点击查看完整过程")]
    if not runs:
        parts.append('<div class="card"><div class="muted">暂无运行记录'
                     '（提交 run 后自动出现；需启用 state_store 持久化）</div></div>')
    else:
        body = ""
        for r in runs:
            st = r.get("run_status", "")
            has_report = r.get("has_report")
            color = _FINAL_COLOR.get(st, "#64748b")
            body += (
                '<tr class="runrow">'
                f'<td><a class="mono" href="/runs/{_esc(r.get("run_id"))}">'
                f'{_esc(r.get("run_id"))}</a></td>'
                f'<td class="goal">{_esc(r.get("goal") or "—")}</td>'
                f'<td>{_badge(st or "?", color)}</td>'
                f'<td class="muted">{"已归档" if has_report else "运行中"}</td>'
                f'<td class="muted mono">{_esc(r.get("updated_at"))}</td>'
                "</tr>"
            )
        parts.append(
            '<div class="card"><h2>运行记录（最近在前）</h2><table><thead><tr>'
            "<th>run_id</th><th>目标</th><th>状态</th><th>存档</th><th>更新时间</th>"
            "</tr></thead><tbody>" + body + "</tbody></table></div>"
        )
    parts.append(_foot("数据源：state_store 运行枚举"))
    return "".join(parts)
