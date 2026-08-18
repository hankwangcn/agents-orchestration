"""DeepSeek 真实端到端冒烟（需环境变量 $DEEPSEEK_API_KEY）。

覆盖链路：注册 → info_request 能力采集 → 异步并发调度执行 → 双层校验解析
→ 审计 → 成本核算 → 学习规则提取。

真实模型下同步验证三件事：
1. 协议模板（完整版 + few-shot）能被真实 LLM 理解并输出合法 Result JSON
2. 解析组件对真实格式漂移的兜底（杂文提取 / 重试）
3. 真实调用下成本 / token / 耗时回填情况

运行：.venv/bin/python scripts/smoke_deepseek.py
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
from orchestration.models import DAG, Task
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler


def build_dag() -> DAG:
    """3 任务：2 个并行 + 1 个汇总（验证拓扑推进与并发派发）。"""
    tasks = {
        "t1": Task(id="t1", desc="用不超过 60 字解释：什么是反向传播（backpropagation）"),
        "t2": Task(id="t2", desc="用不超过 60 字解释：什么是梯度下降（gradient descent）"),
        "t3": Task(
            id="t3",
            desc="结合前两个概念，用不超过 80 字说明反向传播与梯度下降的关系",
            deps=["t1", "t2"],
        ),
    }
    return DAG(tasks=tasks)


async def main() -> None:
    assert os.environ.get("DEEPSEEK_API_KEY"), "$DEEPSEEK_API_KEY 未设置"

    # 1. 注册 + 能力采集（真实 info_request → DeepSeek 回答协议问题）
    adapter = DeepSeekAdapter(model="deepseek-chat")
    registry = AgentRegistry()
    registry.register(adapter, agent_id="deepseek")
    print("== 1. info_request 能力采集 ==")
    collect_summary = registry.collect(agent_id="deepseek")
    for item in collect_summary:
        print(f"   [{item.get('scope')}] ok={item.get('ok')} "
              f"err={item.get('error') or '-'}")
    a = registry.get("deepseek")
    print(f"   能力标签: {a.capabilities}")
    print(f"   并发上限: {a.max_concurrency}  速率: {a.rate_limit_per_min}/min  "
          f"预算: {a.budget_limit_usd}")

    # 2. 并发调度执行
    print("\n== 2. 异步并发调度 ==")
    metrics = MetricsCollector()
    scheduler = AsyncScheduler(registry=registry, metrics=metrics)
    report = await scheduler.run(build_dag(), run_id="smoke_deepseek")
    print(f"   final_status = {report.final_status}")
    for tid, r in report.results.items():
        print(f"   {tid}: success={r.success} duration={r.duration_ms}ms "
              f"tokens={r.usage.tokens_in}+{r.usage.tokens_out} cost=${r.usage.cost:.4f}")
        print(f"       output: {str(r.output)[:80]}")
    for ass in report.assignments:
        print(f"   分配: {ass.task_id} -> {ass.agent_id} ({ass.match_type})")

    # 3. 治理层消费
    print("\n== 3. 治理层 ==")
    audit = Auditor().audit(report)
    print(f"   audit.verdict = {audit.verdict}")
    for issue in audit.issues:
        print(f"     - {issue}")
    cost = CostAccountant().account(report)
    print(f"   cost.total = ${cost.total_cost:.4f}  "
          f"tokens = {cost.total_tokens_in}+{cost.total_tokens_out}")
    print(f"   cost.by_match_type = {[(m.match_type, m.cost) for m in cost.by_match_type]}")
    print(f"   cost.failed = ${cost.failed_cost:.4f}  pruned = ${cost.pruned_cost:.4f}")
    rules = LearningEngine().learn(audit, cost)
    print(f"   learning rules = {[r.rule_id for r in rules.rules]}")

    print("\n== 冒烟结论 ==")
    ok = report.final_status in ("success",) and all(
        r.success for r in report.results.values()
    )
    print("PASS：真实 DeepSeek 全链路闭环" if ok else "FAIL：存在失败任务，见上方明细")


if __name__ == "__main__":
    asyncio.run(main())
