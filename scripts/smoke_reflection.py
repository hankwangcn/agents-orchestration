"""反思/判定冒烟（档三：真实 HTTP 往返，无需真实 LLM 与 API key）。

验证链路：注册 → info_request 能力采集（真实 HTTP 发现 judge 能力）→
提交带 goal 的 run（RunManager，真实 HTTP 执行）→ run 收尾判定
（DeepSeekAdapter 走真实 HTTP 打 mock judge 角色：协议装配 + output_schema
强校验 + 解析 + usage 真实回填）→ 判定结论进报告 → 喂学习层 JUD-1。

同时验证 advisory 边界与跳过分支：
- 判定不改任务状态、不阻断交付（final_status 仍按调度结果）
- 判定成本单列，不计入任务总成本
- 无 goal 的 run 跳过判定（skipped_reason=no_goal）

运行：.venv/bin/python scripts/smoke_reflection.py
（无需 API key——全部打到本地 mock 端点 scripts/mock_agents.py）
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.api.gateway import RunManager
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine
from orchestration.models import DAG, ResourceRequirement, Task, TaskStatus
from orchestration.reflection import Reflector
from orchestration.registry import AgentRegistry

from mock_agents import MockAgentServer, judge_configs

GOAL = "把抓取到的价格数据整理成一份比价报告"


def build_dag() -> DAG:
    return DAG(tasks={
        "t1": Task(id="t1", desc="抓取目标站点价格列表",
                   required_resources=ResourceRequirement(model="coder"),
                   required_capabilities=["code_gen"]),
        "t2": Task(id="t2", desc="把抓取结果整理成比价报告", deps=["t1"],
                   required_resources=ResourceRequirement(model="coder"),
                   required_capabilities=["code_gen"]),
    })


def build_registry(base_url: str) -> AgentRegistry:
    """注册执行 agent + 判定 agent（真实 HTTP，agent_id 即 mock 的 model 字段）。"""
    reg = AgentRegistry()
    for agent_id in ("coder", "judge"):
        reg.register(
            DeepSeekAdapter(model=agent_id, base_url=f"{base_url}/v1",
                            api_key="mock"),
            agent_id=agent_id,
        )
    return reg


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"   {'✅' if ok else '❌'} {name}" + (f"  [{detail}]" if detail else ""))
    return ok


async def main() -> int:
    server = MockAgentServer(judge_configs())
    await server.start()
    print(f"mock agents 已启动: {server.base_url}（角色: {', '.join(server.configs)}）")

    registry = build_registry(server.base_url)
    results: list[bool] = []

    try:
        print("\n== 1. 能力采集（真实 HTTP info_request）==")
        summary = await registry.aensure_fresh()
        judge_agent = registry.get("judge")
        print(f"   采集 {len(summary)} 条；judge.capabilities = {judge_agent.capabilities}")
        results.append(check("judge 能力被问出来（capability 采集）",
                             "judge" in judge_agent.capabilities))

        manager = RunManager(registry=registry, reflector=Reflector(registry))

        print("\n== 2. 提交带 goal 的 run（真实 HTTP 执行）==")
        dag = build_dag()
        run_id = await manager.submit(dag, goal=GOAL)
        report = await manager.wait(run_id)
        snap = manager.snapshot(run_id)
        print(f"   run_id={run_id} final_status={report.final_status} "
              f"goal={snap['goal']!r}")

        results.append(check("run 级目标随 run 可见",
                             snap["goal"] == GOAL))
        results.append(check("任务全部成功（判定不改交付状态）",
                             all(t.status == TaskStatus.SUCCESS
                                 for t in dag.tasks.values())
                             and report.final_status == "success"))

        print("\n== 3. 收尾判定（DeepSeekAdapter → mock judge，真实 HTTP）==")
        ref = report.reflection
        assert ref is not None
        print(f"   judged={ref['judged']} independent={ref['independent']} "
              f"judge_agent={ref['judge_agent']} achieved={ref['achieved']} "
              f"score={ref['score']} cost=${ref['cost']}")
        print(f"   reasons={ref['reasons']}")
        print(f"   gaps={ref['gaps']}")

        results.append(check("判定已产出结论", ref["judged"] is True))
        results.append(check("用独立判定 agent（judge 能力）",
                             ref["judge_agent"] == "judge"
                             and ref["independent"] is True))
        results.append(check("判定结论按 output_schema 强校验通过",
                             ref["achieved"] is False and ref["gaps"]))
        results.append(check("判定 usage 真实回填（>0）", ref["cost"] > 0,
                             f"cost={ref['cost']}"))
        task_cost = sum(r.usage.cost for r in report.results.values())
        results.append(check("判定成本单列，不计入任务总成本",
                             report.total_cost == round(task_cost, 4)
                             and ref["cost"] > 0,
                             f"total={report.total_cost} tasks={round(task_cost, 4)} "
                             f"judge={ref['cost']}"))

        print("\n== 4. 判定结论喂学习层 ==")
        audit = Auditor().audit(report)
        cost = CostAccountant(registry).account(report)
        learned = LearningEngine().learn(audit, cost, _to_report(ref))
        print(f"   learning rules = {[r.rule_id for r in learned.rules]}")
        jud = next((r for r in learned.rules if r.rule_id == "JUD-1"), None)
        results.append(check("JUD-1（目标未达成）规则触发", jud is not None))
        if jud:
            print(f"   JUD-1 severity={jud.severity} gaps={jud.evidence['gaps']}")

        print("\n== 5. 无 goal 的 run 跳过判定 ==")
        run_id2 = await manager.submit(build_dag())
        report2 = await manager.wait(run_id2)
        ref2 = report2.reflection
        print(f"   enabled={ref2['enabled']} skipped_reason={ref2['skipped_reason']}")
        results.append(check("无目标 → 跳过判定（记原因，不报错）",
                             ref2["enabled"] is False
                             and ref2["skipped_reason"] == "no_goal"))
        results.append(check("跳过的 run 正常收尾",
                             report2.final_status == "success"))

        print("\n== 6. mock 观测（传输层视角）==")
        for aid in server.configs:
            print(f"   {aid}: calls={server.calls.get(aid, 0)}")
        results.append(check("判定请求真的走了 HTTP（judge 收到调用）",
                             server.calls.get("judge", 0) >= 1,
                             f"calls={server.calls.get('judge')}"))

    finally:
        await server.stop()

    ok = all(results)
    print("\n== 冒烟结论 ==")
    print("PASS：反思/判定闭环（真实 HTTP 判定 agent + 目标基准 + advisory 边界）"
          if ok else f"FAIL：{results.count(False)} 项断言未通过")
    return 0 if ok else 1


def _to_report(ref: dict):
    """dict → ReflectionReport（学习层入参）。"""
    from orchestration.reflection import ReflectionReport

    return ReflectionReport.model_validate(ref)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
