"""多 agent 全流程冒烟（档三：真实 HTTP 往返，无需真实 LLM 与 API key）。

验证链路：注册 → info_request 能力采集（真实 HTTP）→ 异步并发调度（真实
HTTP）→ 三级分配留痕 → 解析层兜底（杂文重试）→ 传输层故障（HTTP 500）
→ 任务重试耗尽 → 自动摘除 → 失败传播/反向可达剪枝 → 审计 → 成本 → 学习。

agent 阵容（本地 mock 端点 scripts/mock_agents.py）：
  translator  翻译 agent（并发上限 2、150ms 延迟、能力 translation）
  coder       代码 agent（能力 code_gen/code_review）
  analyst     数据分析 agent（第 1 次调用返回杂文 → 触发解析层重试）
  flaky       易故障 agent（前 3 次调用连续失败 → 摘除）

DAG（9 任务）覆盖的分配/失败矩阵：
  t1  model=coder-agent, caps=[code_gen]        → exact（coder，能力覆盖）
  t2  model=coder-agent, caps=[translation]     → exact 但能力不覆盖 → risk 留痕
  t3  model=translator-agent                    → exact（translator）
  t4  caps=[data_analysis]                      → capability（analyst，杂文重试）
  t5  caps=[video_editing]（无人有）            → degraded（translator）
  t6a-c  model=flaky                            → exact（flaky，连续 3 任务失败 → 摘除）
  t7  deps=[t6a]                                → 失败传播被剪（downstream_chain）
  t8  deps=[t3,t4,t5] 无 model/caps             → degraded（translator）
  t9  caps=[weekly_report]（无人有，独立分支）  → degraded（translator，不误杀）

预期：t6a/b/c FAILED、t7 CANCELLED，其余 SUCCESS；final_status=partial；
flaky 被摘除且后续同 model 任务回退降级；translator 并发峰值 ≤ 2；
成功结果 usage 真实回填（tokens>0）。

运行：.venv/bin/python scripts/smoke_multiagent.py
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine
from orchestration.metrics import MetricsCollector
from orchestration.models import DAG, ResourceRequirement, Task, TaskStatus
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler

from mock_agents import MockAgentConfig, MockAgentServer, default_configs


def build_dag() -> DAG:
    tasks = {
        "t1": Task(
            id="t1", desc="实现一个 Python 排序函数",
            required_resources=ResourceRequirement(model="coder"),
            required_capabilities=["code_gen"],
        ),
        "t2": Task(
            id="t2", desc="把 README 翻译成英文",
            required_resources=ResourceRequirement(model="coder"),
            required_capabilities=["translation"],
        ),
        "t3": Task(
            id="t3", desc="把 Hello World 翻译成中文",
            required_resources=ResourceRequirement(model="translator"),
        ),
        "t4": Task(
            id="t4", desc="分析 2025 年销售数据并给出结论",
            required_capabilities=["data_analysis"],
        ),
        "t5": Task(
            id="t5", desc="剪辑宣传视频",
            required_capabilities=["video_editing"],
        ),
        "t6a": Task(
            id="t6a", desc="flaky 任务 1（注定失败）",
            required_resources=ResourceRequirement(model="flaky"),
        ),
        "t6b": Task(
            id="t6b", desc="flaky 任务 2（注定失败）",
            required_resources=ResourceRequirement(model="flaky"),
        ),
        "t6c": Task(
            id="t6c", desc="flaky 任务 3（注定失败，触发摘除）",
            required_resources=ResourceRequirement(model="flaky"),
        ),
        "t7": Task(id="t7", desc="依赖 t6a 的后续任务", deps=["t6a"]),
        "t8": Task(id="t8", desc="汇总 t3/t4/t5 的结果", deps=["t3", "t4", "t5"]),
        "t9": Task(
            id="t9", desc="独立交付：写项目周报",
            required_capabilities=["weekly_report"],
        ),
    }
    return DAG(tasks=tasks)


def check(name: str, cond: bool, detail: str = "") -> bool:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}"
          + (f" — {detail}" if detail else ""))
    return cond


async def main() -> int:
    server = MockAgentServer(default_configs())
    base_url = await server.start()
    print(f"== mock agents 已启动: {base_url} ==")

    results: list[bool] = []
    try:
        # 1. 注册 + 能力采集（真实 HTTP info_request）
        registry = AgentRegistry()
        for agent_id, cfg in server.configs.items():
            adapter = DeepSeekAdapter(model=agent_id, base_url=base_url,
                                      api_key="sk-mock")
            registry.register(adapter, agent_id=agent_id)

        print("\n== 1. info_request 能力采集（真实 HTTP）==")
        # collect 走同步 OpenAI client（_call_llm），丢线程避免阻塞事件循环
        # （uvicorn mock 服务与调度在同一 loop，同步阻塞会死锁）
        for item in await asyncio.to_thread(registry.collect):
            print(f"   [{item['agent_id']}] {item['scope']}: "
                  f"ok={item['ok']} err={item.get('error') or '-'}")
        for agent_id, cfg in server.configs.items():
            a = registry.get(agent_id)
            print(f"   {agent_id}: caps={a.capabilities} "
                  f"concurrency={a.max_concurrency} "
                  f"rate={a.rate_limit_per_min}/min budget=${a.budget_limit_usd} "
                  f"lang={a.languages}")

        print("\n== 2. 异步并发调度（真实 HTTP）==")
        scheduler = AsyncScheduler(registry=registry,
                                   metrics=MetricsCollector(), retries=2)
        dag = build_dag()
        report = await scheduler.run(dag, run_id="smoke_multiagent")
        print(f"   final_status = {report.final_status}")
        for tid in dag.tasks:
            t = dag.tasks[tid]
            r = report.results.get(tid)
            extra = ""
            if r is not None:
                extra = (f" cost=${r.usage.cost:.5f} "
                         f"tokens={r.usage.tokens_in}+{r.usage.tokens_out} "
                         f"dur={r.duration_ms}ms")
                if not r.success and r.error:
                    extra += f" err={r.error.code}"
            print(f"   {tid}: {t.status.value:<10}{extra}")

        print("\n== 3. 分配留痕（三级策略）==")
        for ass in report.assignments:
            risk = " RISK" if ass.risk else ""
            print(f"   {ass.task_id} -> {ass.agent_id} "
                  f"({ass.match_type}){risk}  {ass.reason}")

        print("\n== 4. 失败传播与剪枝 ==")
        for pr in report.prune_reports:
            print(f"   root={pr.root_failure['task_id']} "
                  f"reason={pr.root_failure['reason']} "
                  f"retries={pr.root_failure['retries']} "
                  f"pruned_final={pr.pruned_final}")
            for p in pr.pruned:
                print(f"     - {p['task_id']} ({p['state_at_cancel']}) "
                      f"reason={p['prune_reason']}")

        print("\n== 5. mock 观测（传输层视角）==")
        for aid in server.configs:
            print(f"   {aid}: calls={server.calls.get(aid, 0)} "
                  f"active_peak={server.active_peak.get(aid, 0)}")

        print("\n== 6. 治理层 ==")
        audit = Auditor().audit(report)
        print(f"   audit.verdict = {audit.verdict}")
        for issue in audit.issues:
            print(f"     - {issue}")
        cost = CostAccountant().account(report)
        print(f"   cost.total = ${cost.total_cost:.5f} "
              f"tokens = {cost.total_tokens_in}+{cost.total_tokens_out}")
        print(f"   cost.by_match_type = "
              f"{[(m.match_type, round(m.cost, 5)) for m in cost.by_match_type]}")
        print(f"   cost.failed = ${cost.failed_cost:.5f} "
              f"pruned = ${cost.pruned_cost:.5f}")
        learned = LearningEngine().learn(audit, cost)
        print(f"   learning rules = {[r.rule_id for r in learned.rules]}")

        # 7. 断言
        print("\n== 7. 断言 ==")
        a_translator = registry.get("translator")
        a_coder = registry.get("coder")
        a_analyst = registry.get("analyst")
        a_flaky = registry.get("flaky")
        results.append(check("能力采集入库",
                             a_translator.capabilities == ["translation"]
                             and a_coder.capabilities == ["code_gen", "code_review"]
                             and a_analyst.capabilities
                             == ["data_analysis", "report_writing"]))
        results.append(check("资源声明入库",
                             a_translator.max_concurrency == 2
                             and a_translator.budget_limit_usd == 5.0
                             and "中文" in a_translator.languages))
        results.append(check("整体状态 partial",
                             report.final_status == "partial"))
        status_map = {tid: dag.tasks[tid].status for tid in dag.tasks}
        results.append(check(
            "失败传播: t6a/b/c FAILED / t7 CANCELLED / 其余 SUCCESS",
            all(status_map[t] == TaskStatus.FAILED for t in ("t6a", "t6b", "t6c"))
            and status_map["t7"] == TaskStatus.CANCELLED
            and all(status_map[t] == TaskStatus.SUCCESS
                    for t in ("t1", "t2", "t3", "t4", "t5", "t8", "t9")),
            f"实际: { {k: v.value for k, v in status_map.items()} }",
        ))
        results.append(check("flaky 连续失败摘除",
                             a_flaky.status == "unavailable"
                             and a_flaky.consecutive_failures == 3))
        # 摘除后回退：同 model 新任务分配不到 flaky → 降级默认（分配是 registry
        # 逻辑，不走 HTTP；端到端摘除已由上面断言覆盖）
        fallback_assignment, _ = registry.assign(
            Task(id="t6d", desc="flaky 摘除后的回退验证",
                 required_resources=ResourceRequirement(model="flaky"))
        )
        results.append(check(
            "摘除后回退: 同 model 任务降级默认 agent",
            fallback_assignment.match_type == "degraded"
            and fallback_assignment.agent_id == "translator",
            f"实际: {fallback_assignment.agent_id} ({fallback_assignment.match_type})",
        ))
        ass_map = {a.task_id: a for a in report.assignments}
        results.append(check(
            "三级分配留痕齐全",
            ass_map["t1"].match_type == "exact" and ass_map["t1"].agent_id == "coder"            and ass_map["t2"].risk is True and ass_map["t2"].agent_id == "coder"
            and ass_map["t3"].match_type == "exact"
            and ass_map["t3"].agent_id == "translator"
            and ass_map["t4"].match_type == "capability"
            and ass_map["t4"].agent_id == "analyst"
            and ass_map["t5"].match_type == "degraded"
            and ass_map["t6a"].agent_id == "flaky"
            and ass_map["t8"].match_type == "degraded"
            and ass_map["t9"].match_type == "degraded",
        ))
        # analyst 第 1 次杂文 + 解析重试成功 → 恰好 2 次调用（解析重试不消耗任务重试）
        results.append(check("解析层兜底: analyst 杂文→重试成功（2 次调用）",
                             server.calls.get("analyst") == 2,
                             f"calls={server.calls.get('analyst')}"))
        results.append(check("per-agent 并发上限生效: translator peak ≤ 2",
                             server.active_peak.get("translator", 0) <= 2,
                             f"peak={server.active_peak.get('translator')}"))
        results.append(check("usage 真实回填（HTTP 响应 → 框架）",
                             all(
                                 report.results[t].usage.tokens_in > 0
                                 and report.results[t].usage.tokens_out > 0
                                 for t in ("t1", "t2", "t3", "t4", "t5", "t8", "t9")
                             )))
        results.append(check("审计发现问题（失败/剪枝/risk）",
                             len(audit.issues) > 0))
        results.append(check("成本核算 > 0", cost.total_cost > 0))
        results.append(check("学习规则提取非空", len(learned.rules) > 0))

    finally:
        await server.stop()

    ok = all(results)
    print("\n== 冒烟结论 ==")
    print("PASS：多 agent 全流程闭环（真实 HTTP 往返 + 三级分配 + 故障注入 + 治理）"
          if ok else f"FAIL：{results.count(False)} 项断言未通过")

    if "--visual" in sys.argv:
        try:
            from visualize_report import render_smoke_report

            agent_meta = {}
            for agent_id in server.configs:
                a = registry.get(agent_id)
                agent_meta[agent_id] = {
                    "capabilities": a.capabilities,
                    "max_concurrency": a.max_concurrency,
                    "rate_limit_per_min": a.rate_limit_per_min,
                    "budget_limit_usd": a.budget_limit_usd,
                    "languages": a.languages,
                    "status": a.status,
                    "consecutive_failures": a.consecutive_failures,
                }
            agent_stats = {
                aid: {"calls": server.calls.get(aid, 0),
                      "active_peak": server.active_peak.get(aid, 0)}
                for aid in server.configs
            }
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            html_path = os.path.join(repo_root, "reports",
                                     "smoke_multiagent_report.html")
            render_smoke_report(
                report=report, agent_meta=agent_meta, agent_stats=agent_stats,
                audit=audit, cost=cost, learned=learned, ok=ok,
                output_path=html_path,
            )
            print(f"\n== 可视化报告已生成 ==\n   {html_path}")
        except Exception as e:  # 可视化失败不影响冒烟结论
            print(f"   [warn] 报告渲染失败: {e}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
