"""任务分配 / 资源协调（架构 §3.2 **调度层「资源协调器」**；阶段三）。

三级分配策略，全程留痕（`Assignment`）：
1. exact      —— task.model 精确匹配（同 model 多实例轮询）
2. capability —— 无 exact 时按能力标签匹配（部分覆盖标记 risk）
3. degraded   —— 无匹配降级到默认通用 LLM（显式留痕，不静默消化）

多 agent 资源协调：连续失败达到阈值自动摘除（unavailable），不再参与分配。

**层职责边界**：本模块**只读** `AgentPool` 的画像并做决策，不采集数据——
采集 / 声明解析 / 刷新属规划层「资源统计器」（`agent_pool.AgentPool`）。
摘除是调度层动作（best-effort，不视为监控 agent 内部状态）。
"""
from __future__ import annotations

from typing import Optional

from .agent_pool import AgentPool, RegisteredAgent, RegistryError
from .models import Assignment, Task


class Allocator:
    """资源协调器：为任务挑 agent（三级策略）+ 维护 agent 健康度/摘除。"""

    def __init__(
        self,
        pool: AgentPool,
        max_consecutive_failures: int = 3,
    ):
        self._pool = pool
        self.max_consecutive_failures = max_consecutive_failures
        self._rr_counter: dict[str, int] = {}  # model → 轮询游标

    # ---------- 分配（三级策略 + 多实例轮询） ----------

    def assign(self, task: Task) -> tuple[Assignment, object]:
        """为任务分配 agent。

        1. exact：task.model 精确匹配（available 实例，同 model 轮询）
        2. capability：按 required_capabilities 匹配（部分覆盖标记 risk）
        3. degraded：降级默认通用 LLM（显式留痕）
        """
        agents = self._pool.agents
        model = task.required_resources.model
        req_caps = [c for c in (task.required_capabilities or []) if c]

        # 1. 精确 model 匹配
        pool = [a for a in agents.values() if a.model == model and a.available]
        if pool:
            agent = self._round_robin(pool, model)
            # 能力声明未覆盖任务需求 → 标记风险（审计重点盯），但不降级
            risk = bool(req_caps) and not (set(req_caps) & set(agent.capabilities))
            return (
                Assignment(
                    task_id=task.id,
                    agent_id=agent.agent_id,
                    match_type="exact",
                    risk=risk,
                    reason=(
                        f"能力声明未覆盖需求 {req_caps}" if risk
                        else f"覆盖能力：{sorted(set(req_caps) & set(agent.capabilities))}"
                    ),
                ),
                agent.adapter,
            )

        # 2. 能力匹配：需求覆盖最多的 available agent
        if req_caps:
            best: Optional[RegisteredAgent] = None
            best_cover = 0
            for a in agents.values():
                if not a.available:
                    continue
                cover = len(set(req_caps) & set(a.capabilities))
                if cover > best_cover:
                    best, best_cover = a, cover
            if best is not None and best_cover > 0:
                partial = best_cover < len(set(req_caps))
                return (
                    Assignment(
                        task_id=task.id,
                        agent_id=best.agent_id,
                        match_type="capability",
                        risk=partial,
                        reason=(
                            f"能力覆盖 {best_cover}/{len(set(req_caps))}：{sorted(set(req_caps) & set(best.capabilities))}"
                            + ("（部分覆盖）" if partial else "")
                        ),
                    ),
                    best.adapter,
                )

        # 3. 降级：默认通用 LLM 优先；默认不可用则任意可用 agent 兜底
        #    （默认 agent 被摘除不应导致系统瘫痪）；全部不可用才抛错
        fallback: Optional[RegisteredAgent] = None
        default_id = self._pool.default_id
        if default_id is not None:
            d = agents.get(default_id)
            if d is not None and d.available:
                fallback = d
        if fallback is None:
            fallback = next((a for a in agents.values() if a.available), None)
        if fallback is None:
            raise RegistryError(
                f"任务 {task.id} 无任何可用 agent："
                f"model={model}, caps={req_caps}"
            )
        return (
            Assignment(
                task_id=task.id,
                agent_id=fallback.agent_id,
                match_type="degraded",
                risk=False,
                reason=f"无匹配 agent（model={model}, caps={req_caps}），降级默认",
            ),
            fallback.adapter,
        )

    def _round_robin(self, pool: list[RegisteredAgent], model: str) -> RegisteredAgent:
        """同 model 多实例轮询分流（多 agent 资源协调）。"""
        idx = self._rr_counter.get(model, 0)
        agent = pool[idx % len(pool)]
        self._rr_counter[model] = idx + 1
        return agent

    # ---------- 健康度维护（故障摘除 / 恢复） ----------

    def record_success(self, agent_id: str) -> None:
        self._pool.get(agent_id).consecutive_failures = 0

    def record_failure(self, agent_id: str) -> None:
        """连续失败达到阈值自动摘除（best-effort，调度器每次执行后调用）。"""
        agent = self._pool.get(agent_id)
        agent.consecutive_failures += 1
        if agent.consecutive_failures >= self.max_consecutive_failures:
            agent.status = "unavailable"

    def mark_unavailable(self, agent_id: str) -> None:
        self._pool.get(agent_id).status = "unavailable"

    def mark_available(self, agent_id: str) -> None:
        agent = self._pool.get(agent_id)
        agent.status = "available"
        agent.consecutive_failures = 0
