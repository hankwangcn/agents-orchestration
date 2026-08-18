"""冒烟可视化报告渲染：把一次多 agent 全流程调度渲染为自包含 HTML。

无外部依赖（纯 Python 字符串模板 + 内嵌 SVG），浏览器直接打开即可，
与 docs/architecture-diagram.html 同风格（单文件自包含）。

由 smoke_multiagent.py --visual 调用：
    .venv/bin/python scripts/smoke_multiagent.py --visual
    # 生成 reports/smoke_multiagent_report.html

报告内容：
  1. 摘要（final_status / 各状态计数 / 总耗时 / 总成本 / 审计结论）
  2. DAG 全景图（SVG）：节点=任务（颜色=最终状态），边=依赖，
     标注 agent 分配 / 匹配类型 / risk / 剪枝原因 / 失败根
  3. Agent 阵容（声明 vs 观测：调用次数 / 并发峰值 / 摘除状态）
  4. 分配留痕表（三级策略 exact / capability / degraded）
  5. 失败传播与剪枝明细
  6. 治理层（审计 / 成本归集 / 学习规则）
"""
from __future__ import annotations

import html as _h
import os
import time
from typing import Any, Optional

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

STATUS_COLOR = {
    "success": "#16a34a",
    "failed": "#dc2626",
    "cancelled": "#9ca3af",
    "interrupted": "#f59e0b",
    "skipped": "#94a3b8",
    "pending": "#2563eb",
    "running": "#2563eb",
}
STATUS_LABEL = {
    "success": "成功", "failed": "失败", "cancelled": "已取消",
    "interrupted": "中断", "skipped": "跳过", "pending": "待执行",
    "running": "执行中",
}
MATCH_COLOR = {
    "exact": "#16a34a",
    "capability": "#2563eb",
    "degraded": "#f59e0b",
}
VERDICT_COLOR = {"ok": "#16a34a", "warning": "#f59e0b", "critical": "#dc2626"}

_NODE_W, _NODE_H = 216, 88
_GAP_X, _GAP_Y = 64, 26
_PAD = 24


def _esc(s: Any) -> str:
    return _h.escape(str(s))


# ---------------------------------------------------------------------------
# DAG 布局（拓扑分层：level = 依赖最大 level + 1）
# ---------------------------------------------------------------------------

def _layout(tasks: dict) -> dict[str, tuple[int, int]]:
    levels: dict[str, int] = {}
    for tid in tasks:
        levels[tid] = 0
    changed = True
    while changed:
        changed = False
        for tid, t in tasks.items():
            if t.deps:
                lv = max(levels[d] for d in t.deps) + 1
                if levels[tid] != lv:
                    levels[tid] = lv
                    changed = True
    order: dict[str, tuple[int, int]] = {}
    by_level: dict[int, list[str]] = {}
    for tid, lv in levels.items():
        by_level.setdefault(lv, []).append(tid)
    for lv, tids in by_level.items():
        for i, tid in enumerate(sorted(tids)):
            order[tid] = (lv, i)
    return order


def _pos(tid: str, order: dict[str, tuple[int, int]]) -> tuple[float, float]:
    lv, idx = order[tid]
    return _PAD + lv * (_NODE_W + _GAP_X), _PAD + idx * (_NODE_H + _GAP_Y)


# ---------------------------------------------------------------------------
# SVG：节点 + 依赖边
# ---------------------------------------------------------------------------

def _build_svg(tasks: dict, assignments: dict, results: dict,
               prune_map: dict, failed_roots: set) -> str:
    order = _layout(tasks)
    max_lv = max(lv for lv, _ in order.values())
    max_cnt = max(i for _, i in order.values()) + 1
    width = _PAD * 2 + (max_lv + 1) * _NODE_W + max_lv * _GAP_X
    height = _PAD * 2 + max_cnt * _NODE_H + (max_cnt - 1) * _GAP_Y

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}" '
        'font-family="system-ui,-apple-system,sans-serif">',
        '<defs>'
        '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#94a3b8"/></marker>'
        '</defs>',
    ]

    # 依赖边（先画，节点盖在上面）
    for tid, t in tasks.items():
        if not t.deps:
            continue
        x1, y1 = _pos(tid, order)
        for d in t.deps:
            x2, y2 = _pos(d, order)
            d_y2 = y2 + _NODE_H / 2
            parts.append(
                f'<path d="M{x2 + _NODE_W},{d_y2} '
                f'C{x2 + _NODE_W + 24},{d_y2} {x1 - 24},{y1 + _NODE_H / 2} '
                f'{x1},{y1 + _NODE_H / 2}" fill="none" stroke="#94a3b8" '
                'stroke-width="1.6" marker-end="url(#arrow)"/>'
            )

    # 节点
    for tid, t in tasks.items():
        x, y = _pos(tid, order)
        st = t.status.value
        color = STATUS_COLOR.get(st, "#64748b")
        ass = assignments.get(tid)
        res = results.get(tid)
        is_root = tid in failed_roots
        is_pruned = tid in prune_map

        stroke_style = (
            f'stroke="{color}" stroke-width="2.4"'
            if is_root else
            f'stroke="{color}" stroke-dasharray="5,4" stroke-width="1.8"'
            if is_pruned else
            f'stroke="{color}" stroke-width="1.8"'
        )
        fill = "#ffffff" if st != "cancelled" else "#f3f4f6"
        parts.append(
            f'<rect x="{x}" y="{y}" width="{_NODE_W}" height="{_NODE_H}" '
            f'rx="10" fill="{fill}" {stroke_style}/>'
        )

        # 状态徽章（右上角）
        badge_w = 66
        parts.append(
            f'<rect x="{x + _NODE_W - badge_w - 8}" y="{y + 8}" '
            f'width="{badge_w}" height="18" rx="9" fill="{color}"/>'
            f'<text x="{x + _NODE_W - 8 - badge_w / 2}" y="{y + 20.5}" '
            f'font-size="11" fill="#fff" text-anchor="middle">'
            f'{STATUS_LABEL.get(st, st)}</text>'
        )
        # 任务 id
        parts.append(
            f'<text x="{x + 12}" y="{y + 21}" font-size="13.5" '
            f'font-weight="700" fill="#0f172a">{_esc(tid)}</text>'
        )
        # 描述（截断）
        desc = t.desc
        desc = desc if len(desc) <= 16 else desc[:16] + "…"
        parts.append(
            f'<text x="{x + 12}" y="{y + 40}" font-size="11.5" '
            f'fill="#475569">{_esc(desc)}</text>'
        )
        # agent 分配行
        if ass is not None:
            mcolor = MATCH_COLOR.get(ass.match_type, "#64748b")
            risk = " ⚠" if ass.risk else ""
            parts.append(
                f'<text x="{x + 12}" y="{y + 58}" font-size="11.5">'
                f'<tspan fill="#334155">{_esc(ass.agent_id)}</tspan>'
                f'<tspan fill="{mcolor}" font-weight="600"> · '
                f'{_esc(ass.match_type)}{risk}</tspan></text>'
            )
        # 第四行：耗时/错误/剪枝原因
        info = ""
        if is_pruned:
            info = f"✂ {prune_map[tid]}"
        elif is_root:
            info = f"✕ {res.error.code if res and res.error else 'failed'}"
        elif res is not None and st == "success":
            info = f"{res.duration_ms}ms · {res.usage.tokens_in + res.usage.tokens_out}tok"
        if info:
            parts.append(
                f'<text x="{x + 12}" y="{y + 75}" font-size="10.5" '
                f'fill="#94a3b8">{_esc(info)}</text>'
            )

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# HTML 片段
# ---------------------------------------------------------------------------

def _badge(text: str, color: str) -> str:
    return (f'<span class="badge" style="background:{color}">'
            f'{_esc(text)}</span>')


def _summary(report: Any, audit: Any, cost: Any, duration_s: float, ok: bool) -> str:
    tasks = report.dag.tasks
    counts: dict[str, int] = {}
    for t in tasks.values():
        counts[t.status.value] = counts.get(t.status.value, 0) + 1
    n = len(tasks)
    cells = [
        ("最终状态", report.final_status, VERDICT_COLOR.get(
            report.final_status, "#64748b")),
        ("成功", counts.get("success", 0), STATUS_COLOR["success"]),
        ("失败", counts.get("failed", 0), STATUS_COLOR["failed"]),
        ("已取消", counts.get("cancelled", 0), STATUS_COLOR["cancelled"]),
        ("总耗时", f"{duration_s:.1f}s", "#0f172a"),
        ("总成本", f"${cost.total_cost:.4f}", "#0f172a"),
        ("审计结论", audit.verdict, VERDICT_COLOR.get(audit.verdict, "#64748b")),
        ("冒烟结论", "PASS" if ok else "FAIL",
         "#16a34a" if ok else "#dc2626"),
    ]
    items = "".join(
        f'<div class="cell"><div class="cell-v">{_esc(v)}</div>'
        f'<div class="cell-k" style="color:{c}">{_esc(k)}</div></div>'
        for k, v, c in cells
    )
    return f'<div class="summary">{items}</div>'


def _agent_table(agent_meta: dict, agent_stats: dict) -> str:
    rows = ""
    for aid, m in sorted(agent_meta.items()):
        st = agent_stats.get(aid, {})
        status = m.get("status", "ok")
        st_badge = (f'<span class="badge" style="background:#dc2626">已摘除 '
                    f'(连续失败 {m.get("consecutive_failures", 0)})</span>'
                    if status == "unavailable" else
                    f'<span class="badge" style="background:#16a34a">正常</span>')
        rows += (
            "<tr>"
            f'<td><b>{_esc(aid)}</b></td>'
            f'<td>{_esc(", ".join(m.get("capabilities", [])) or "—")}</td>'
            f'<td>{_esc(m.get("max_concurrency"))}</td>'
            f'<td>{_esc(m.get("rate_limit_per_min"))}/min · '
            f'${_esc(m.get("budget_limit_usd"))}</td>'
            f'<td>{_esc(", ".join(m.get("languages", [])))}</td>'
            f'<td>{st.get("calls", 0)}</td>'
            f'<td>{st.get("active_peak", 0)}</td>'
            f'<td>{st_badge}</td>'
            "</tr>"
        )
    return (
        '<table><thead><tr>'
        "<th>agent</th><th>能力声明</th><th>并发上限</th>"
        "<th>限速 / 预算</th><th>语言</th><th>HTTP 调用数</th>"
        "<th>并发峰值</th><th>状态</th>"
        "</tr></thead><tbody>" + rows + "</tbody></table>"
    )


def _assign_table(assignments: list) -> str:
    rows = ""
    for a in sorted(assignments, key=lambda x: x.task_id):
        mcolor = MATCH_COLOR.get(a.match_type, "#64748b")
        risk = ('<span class="badge" style="background:#dc2626">risk</span>'
                if a.risk else "")
        rows += (
            "<tr>"
            f'<td><b>{_esc(a.task_id)}</b></td>'
            f'<td>{_esc(a.agent_id)}</td>'
            f'<td><span class="badge" style="background:{mcolor}">'
            f'{_esc(a.match_type)}</span> {risk}</td>'
            f'<td>{_esc(a.reason)}</td>'
            "</tr>"
        )
    return (
        '<table><thead><tr><th>任务</th><th>分配 agent</th>'
        "<th>匹配类型</th><th>说明</th></tr></thead><tbody>"
        + rows + "</tbody></table>"
    )


def _prune_section(report: Any) -> str:
    if not report.prune_reports:
        return '<div class="muted">无失败传播事件</div>'
    out = ""
    for pr in report.prune_reports:
        root = pr.root_failure
        out += (
            f'<div class="prune-root">'
            f'<span class="badge" style="background:#dc2626">根失败</span> '
            f'<b>{_esc(root["task_id"])}</b> — {_esc(root["reason"])}'
            f'（重试 {_esc(root["retries"])} 次耗尽）'
            f'</div>'
        )
        for p in pr.pruned:
            out += (
                f'<div class="prune-item">✂ <b>{_esc(p["task_id"])}</b> '
                f'<span class="muted">(取消时 {_esc(p["state_at_cancel"])})</span>'
                f' — {_esc(p["prune_reason"])}</div>'
            )
        if pr.pruned_final:
            out += '<div class="prune-item">⚠ 全部最终交付点被波及，整棵 DAG 取消</div>'
    return out


def _governance_section(audit: Any, cost: Any, learned: Any) -> str:
    # 审计
    vcolor = VERDICT_COLOR.get(audit.verdict, "#64748b")
    issues = "".join(
        f'<li>{_esc(i)}</li>' for i in audit.issues
    ) or "<li class='muted'>无问题</li>"
    audit_html = (
        f'<h3>审计 <span class="badge" style="background:{vcolor}">'
        f'{_esc(audit.verdict)}</span></h3><ul>{issues}</ul>'
    )
    # 成本
    by_match = "".join(
        f'<li>{_esc(m.match_type)}：${m.cost:.4f}</li>'
        for m in cost.by_match_type
    )
    cost_html = (
        f"<h3>成本</h3><ul>"
        f"<li>总成本：<b>${cost.total_cost:.4f}</b> "
        f"(in {cost.total_tokens_in} + out {cost.total_tokens_out} tokens)</li>"
        f"<li>失败消耗：${cost.failed_cost:.4f}　剪枝取消消耗："
        f"${cost.pruned_cost:.4f}</li>{by_match}</ul>"
    )
    # 学习规则
    rules = ""
    for r in learned.rules:
        sev_color = {"high": "#dc2626", "medium": "#f59e0b",
                     "low": "#94a3b8"}.get(r.severity, "#64748b")
        detail = getattr(r, "message", "") or ""
        evidence = getattr(r, "evidence", "") or ""
        detail = detail or evidence
        rules += (
            f'<li><b>{_esc(r.rule_id)}</b> '
            f'<span class="badge" style="background:{sev_color}">'
            f'{_esc(r.severity)}</span> '
            f'<span class="muted">{_esc(r.category)}</span>'
            + (f' — {_esc(detail)}' if detail else "") + "</li>"
        )
    learn_html = (
        f"<h3>学习规则（{len(learned.rules)}）</h3><ul>{rules}</ul>"
        if rules else "<h3>学习规则</h3><div class='muted'>无</div>"
    )
    return (
        '<div class="grid3">'
        f'<div class="card">{audit_html}</div>'
        f'<div class="card">{cost_html}</div>'
        f'<div class="card">{learn_html}</div>'
        "</div>"
    )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def render_smoke_report(*, report: Any, agent_meta: dict, agent_stats: dict,
                        audit: Any, cost: Any, learned: Any, ok: bool,
                        output_path: Optional[str] = None) -> str:
    """渲染自包含 HTML 报告；output_path 给定则写盘，返回 HTML 字符串。"""
    tasks = report.dag.tasks
    assignments = {a.task_id: a for a in report.assignments}
    results = report.results
    prune_map: dict[str, str] = {}
    failed_roots: set[str] = set()
    for pr in report.prune_reports:
        failed_roots.add(pr.root_failure["task_id"])
        for p in pr.pruned:
            prune_map[p["task_id"]] = p["prune_reason"]

    svg = _build_svg(tasks, assignments, results, prune_map, failed_roots)
    duration_s = sum(r.duration_ms for r in results.values()) / 1000

    legend = "".join(
        f'<span class="lg"><i style="background:{STATUS_COLOR[s]}"></i>'
        f"{STATUS_LABEL[s]}</span>"
        for s in ("success", "failed", "cancelled", "interrupted")
    ) + (
        '<span class="lg muted">虚线边框 = 被剪枝（✂）　</span>'
        '<span class="lg muted">红色粗边 = 失败根（✕）　</span>'
        '<span class="lg muted">⚠ = 能力风险留痕</span>'
    )

    html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>多 Agent 全流程冒烟 — 可视化报告</title>
<style>
  body {{ font-family: system-ui,-apple-system,"Segoe UI",sans-serif;
         margin:0; background:#f1f5f9; color:#0f172a; }}
  .wrap {{ max-width: 1180px; margin: 0 auto; padding: 24px 20px 60px; }}
  header h1 {{ font-size: 22px; margin: 0 0 4px; }}
  header .sub {{ color:#64748b; font-size: 13px; }}
  .summary {{ display:flex; flex-wrap:wrap; gap:12px; margin: 18px 0; }}
  .cell {{ background:#fff; border:1px solid #e2e8f0; border-radius:12px;
          padding:10px 18px; min-width:96px; text-align:center; }}
  .cell-v {{ font-size:20px; font-weight:700; }}
  .cell-k {{ font-size:12px; margin-top:2px; }}
  .card {{ background:#fff; border:1px solid #e2e8f0; border-radius:14px;
          padding:16px 20px; margin:14px 0; }}
  .card h2 {{ font-size:15px; margin:0 0 12px; }}
  .card h3 {{ font-size:13.5px; margin:10px 0 6px; }}
  table {{ border-collapse:collapse; width:100%; font-size:12.5px; }}
  th {{ text-align:left; color:#64748b; font-weight:600; padding:6px 8px;
       border-bottom:2px solid #e2e8f0; }}
  td {{ padding:6px 8px; border-bottom:1px solid #eef2f7; }}
  .badge {{ display:inline-block; padding:2px 10px; border-radius:999px;
           color:#fff; font-size:11px; font-weight:600; }}
  .grid3 {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr));
           gap:14px; }}
  .grid3 .card {{ margin:0; }}
  ul {{ margin:6px 0; padding-left:18px; font-size:12.5px; }}
  li {{ margin:3px 0; }}
  .muted {{ color:#94a3b8; }}
  .lg {{ display:inline-flex; align-items:center; gap:6px; font-size:12px;
        color:#475569; margin-right:16px; }}
  .lg i {{ width:12px; height:12px; border-radius:3px; display:inline-block; }}
  .prune-root {{ font-size:13px; margin:6px 0; }}
  .prune-item {{ font-size:12.5px; color:#475569; margin:3px 0 3px 20px; }}
  footer {{ margin-top:26px; color:#94a3b8; font-size:12px; text-align:center; }}
  .svgbox {{ overflow-x:auto; }}
</style>
</head>
<body><div class="wrap">
<header>
  <h1>多 Agent 全流程冒烟 — 可视化报告</h1>
  <div class="sub">本地 mock agent（真实 HTTP 往返）· 生成于 {time.strftime("%Y-%m-%d %H:%M:%S")}</div>
</header>
{_summary(report, audit, cost, duration_s, ok)}
<div class="card"><h2>DAG 执行全景</h2><div class="svgbox">{svg}</div>
<div style="margin-top:10px">{legend}</div></div>
<div class="card"><h2>Agent 阵容（声明 vs 观测）</h2>{_agent_table(agent_meta, agent_stats)}</div>
<div class="card"><h2>分配留痕（三级策略）</h2>{_assign_table(report.assignments)}</div>
<div class="card"><h2>失败传播与剪枝</h2>{_prune_section(report)}</div>
<div class="card"><h2>治理层</h2>{_governance_section(audit, cost, learned)}</div>
<footer>由 scripts/smoke_multiagent.py --visual 生成 ·
  <a href="https://github.com/hankwangcn/agents-orchestration">agents-orchestration</a></footer>
</div></body></html>"""

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
    return html
