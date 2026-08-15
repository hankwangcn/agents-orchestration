"""成本核算（架构 §3.2 治理层 OB 可观测性；阶段二）。

输入：ScheduleReport + AgentRegistry（预算声明对比）。
输出：CostReport——按 agent / 匹配类型归集成本，预算超支标记。

核算口径（架构 §5.4）：剪枝任务已发生的消耗照记总账，但单独归集暴露——
"已发生消耗不算浪费账目"，是审计对账的事实基础。
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .registry import AgentRegistry
from .scheduler import ScheduleReport


class AgentCost(BaseModel):
    """单个 agent 的成本归集。"""
    agent_id: str
    model: str = ""
    tasks: int = 0
    cost: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0
    match_types: list[str] = Field(default_factory=list)
    budget_limit_usd: float = 0.0
    over_budget: bool = False


class MatchTypeCost(BaseModel):
    """按分配方式归集（暴露降级成本）。"""
    match_type: str
    tasks: int = 0
    cost: float = 0.0


class CostReport(BaseModel):
    total_cost: float = 0.0
    total_tokens_in: int = 0
    total_tokens_out: int = 0
    avg_cost_per_task: float = 0.0

    by_agent: list[AgentCost] = Field(default_factory=list)
    by_match_type: list[MatchTypeCost] = Field(default_factory=list)

    failed_cost: float = 0.0
    """失败任务已消耗成本（暴露重试/失败的沉没成本）。"""
    pruned_cost: float = 0.0
    """剪枝任务已发生消耗（先成功后仍被剪的任务，§5.4 口径）。"""
    over_budget_agents: list[dict] = Field(default_factory=list)


class CostAccountant:
    """成本核算：只读归集，不修改任何状态。"""

    def __init__(self, registry: Optional[AgentRegistry] = None):
        self._registry = registry

    # ------------------------------------------------------------------

    def account(self, report: ScheduleReport) -> CostReport:
        out = CostReport()
        dag = report.dag
        results = report.results

        agent_costs: dict[str, AgentCost] = {}
        type_costs: dict[str, MatchTypeCost] = {}

        # 预算声明预载（注册表可选）
        budget: dict[str, float] = {}
        if self._registry is not None:
            for aid, agent in self._registry.agents.items():
                budget[aid] = agent.budget_limit_usd

        for tid, result in results.items():
            cost = result.usage.cost if result.usage else 0.0
            tin = result.usage.tokens_in if result.usage else 0
            tout = result.usage.tokens_out if result.usage else 0

            out.total_cost += cost
            out.total_tokens_in += tin
            out.total_tokens_out += tout

            # 失败成本归集
            if not result.success:
                out.failed_cost += cost

            # 归属（assignment 与 task 一一对应，按 task_id 反查）
            assignment = next(
                (a for a in report.assignments if a.task_id == tid), None
            )
            if assignment is None:
                continue
            aid = assignment.agent_id
            ac = agent_costs.setdefault(
                aid, AgentCost(agent_id=aid, budget_limit_usd=budget.get(aid, 0.0))
            )
            ac.tasks += 1
            ac.cost += cost
            ac.tokens_in += tin
            ac.tokens_out += tout
            if assignment.match_type not in ac.match_types:
                ac.match_types.append(assignment.match_type)

            tc = type_costs.setdefault(
                assignment.match_type, MatchTypeCost(match_type=assignment.match_type)
            )
            tc.tasks += 1
            tc.cost += cost

        # 剪枝已发生消耗：state_at_cancel=success 的任务（先成功后被剪）
        for pr in report.prune_reports:
            for p in pr.pruned:
                task = dag.tasks.get(p["task_id"])
                if task is None or task.result is None:
                    continue
                if p.get("state_at_cancel") == "success":
                    out.pruned_cost += (
                        task.result.usage.cost if task.result.usage else 0.0
                    )

        # 预算超支判定（声明预算 >0 才判定；0 = 未知不判）
        out.by_agent = sorted(agent_costs.values(), key=lambda a: a.cost, reverse=True)
        for ac in out.by_agent:
            if ac.budget_limit_usd > 0 and ac.cost > ac.budget_limit_usd:
                ac.over_budget = True
                out.over_budget_agents.append({
                    "agent_id": ac.agent_id,
                    "spent": round(ac.cost, 4),
                    "budget": ac.budget_limit_usd,
                    "overspend": round(ac.cost - ac.budget_limit_usd, 4),
                })

        out.by_match_type = sorted(
            type_costs.values(), key=lambda t: t.cost, reverse=True
        )
        task_n = len(results)
        out.avg_cost_per_task = (
            round(out.total_cost / task_n, 6) if task_n else 0.0
        )
        out.total_cost = round(out.total_cost, 4)
        out.failed_cost = round(out.failed_cost, 4)
        out.pruned_cost = round(out.pruned_cost, 4)
        return out
