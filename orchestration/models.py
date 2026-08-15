"""核心数据模型：Task / Result（结果契约）/ DAG（含图算法）。

对应 docs/architecture.md §4 数据模型。
Result 是框架一切逻辑的枢纽——审计、资源统计、失败处理、自我学习全部依赖此契约。
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class SideEffects(str, Enum):
    NONE = "none"
    EXTERNAL_API = "external_api"
    FILE_WRITE = "file_write"


class ResourceRequirement(BaseModel):
    """任务资源需求（架构 §4.1 required_resources）。"""
    model: str = "deepseek-chat"
    budget: float = 1.0
    timeout: int = 300


class Usage(BaseModel):
    tokens_in: int = 0
    tokens_out: int = 0
    cost: float = 0.0


class ErrorInfo(BaseModel):
    code: str
    message: str = ""


class Result(BaseModel):
    """结果契约（架构 §4.2）：agent 响应的统一格式，框架一切逻辑的枢纽。

    request_id 用于对账（消息协议 §7.7 强制回带）；简化版模板下可缺失，
    由框架补空、对账降级为 task_id / 单通道顺序。
    """
    request_id: Optional[str] = None
    task_id: str
    success: bool
    output: Any = None
    error: Optional[ErrorInfo] = None
    usage: Usage = Field(default_factory=Usage)
    duration_ms: int = 0
    retries: int = 0
    side_effect_report: str = ""


class Task(BaseModel):
    """任务模型（架构 §4.1）。

    required_capabilities：任务的能力需求标签（拆解层声明），
    分配层据此匹配 agent（阶段三）；为空时只按 model 匹配。
    """
    id: str
    desc: str
    deps: list[str] = Field(default_factory=list)
    output_schema: Optional[dict] = None
    required_resources: ResourceRequirement = Field(default_factory=ResourceRequirement)
    required_capabilities: list[str] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    side_effects: SideEffects = SideEffects.NONE
    result: Optional[Result] = None

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.SKIPPED,
        )


class PruneReport(BaseModel):
    """剪枝取消报告（架构 §5.4）：同时喂给审计与自我学习。"""
    root_failure: dict
    pruned: list[dict]
    pruned_final: bool


class Assignment(BaseModel):
    """一次任务分配的留痕（审计/自我学习素材，阶段二消费）。

    阶段三起由注册表产出，随 ScheduleReport 交付。
    """
    task_id: str
    agent_id: str
    match_type: str  # exact | capability | degraded
    reason: str = ""
    risk: bool = False  # 能力不足/声明未覆盖 → 审计重点盯


class DAG(BaseModel):
    """依赖 DAG（架构 §4.3）：节点 = 任务，边 = 数据流依赖。

    调度器只认拓扑序（in-degree=0 可派发）；失败传播与死任务剪枝
    均在此做图算法（架构 §5.2）。DAG 是数据流依赖，不是运行时状态。
    """
    tasks: dict[str, Task] = Field(default_factory=dict)

    # ---------- 图基础 ----------

    def descendants(self, task_id: str) -> set[str]:
        """T 的全部后代（含间接下游）——输入链断裂所波及的任务。"""
        result: set[str] = set()
        stack = [t for t in self.tasks if task_id in self.tasks[t].deps]
        while stack:
            cur = stack.pop()
            if cur in result:
                continue
            result.add(cur)
            stack.extend(t for t in self.tasks if cur in self.tasks[t].deps)
        return result

    def final_tasks(self) -> set[str]:
        """最终交付任务：出度为 0（产出无人继续消费，即交付点）。"""
        consumed = {d for t in self.tasks.values() for d in t.deps}
        return {tid for tid in self.tasks if tid not in consumed}

    def reverse_reachable(self, roots: set[str]) -> set[str]:
        """反向可达：从 roots 沿依赖边反向遍历，返回全部上游。

        剪枝判据（架构 §5.2）："我的产出还有没有人要？"
        反向走得到 = 产出仍被最终交付消费 = 保留。
        """
        reachable: set[str] = set()
        stack = list(roots)
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(self.tasks[cur].deps)
        return reachable

    def ready_tasks(self) -> list[str]:
        """拓扑序可派发：pending 且所有依赖已 success。"""
        return [
            tid
            for tid, t in self.tasks.items()
            if t.status == TaskStatus.PENDING
            and all(self.tasks[d].status == TaskStatus.SUCCESS for d in t.deps)
        ]

    def all_terminal(self) -> bool:
        return all(t.is_terminal for t in self.tasks.values())

    # ---------- 失败处理：反向可达性剪枝（架构 §5.2） ----------

    def prune_after_failure(self, failed_task_id: str) -> PruneReport:
        """任务 T 失败（重试耗尽）后的剪枝算法。

        统一规则：以"仍存活的 final task"为根做反向可达——
        可达者保留，不可达者全部取消。存活 final 为空（交付点全灭）
        即整棵 DAG 取消。
        """
        failed = self.tasks[failed_task_id]
        assert failed.status == TaskStatus.FAILED

        # 1. T 及其全部后代（输入链断裂）必然取消
        to_cancel = self.descendants(failed_task_id) | {failed_task_id}

        # 2. 存活 final = 最终交付点中未被波及者
        finals = self.final_tasks()
        alive_finals = finals - to_cancel

        # 3. 反向可达剪枝：可达保留，不可达取消
        survivors = self.reverse_reachable(alive_finals) if alive_finals else set()
        cancelled_ids = set(self.tasks) - survivors
        pruned_final = not alive_finals

        pruned: list[dict] = []
        for tid in cancelled_ids:
            if tid == failed_task_id:
                continue  # 根失败单独记录
            t = self.tasks[tid]
            reason = (
                "downstream_chain" if tid in to_cancel else "unreachable_from_final"
            )
            pruned.append(
                {
                    "task_id": tid,
                    "state_at_cancel": t.status.value,
                    "prune_reason": reason,
                }
            )
            t.status = TaskStatus.CANCELLED

        failed_res = failed.result
        report = PruneReport(
            root_failure={
                "task_id": failed_task_id,
                "reason": (
                    failed_res.error.code
                    if failed_res and failed_res.error
                    else "unknown"
                ),
                "retries": failed_res.retries if failed_res else 0,
            },
            pruned=pruned,
            pruned_final=pruned_final,
        )
        return report


class ScheduleReport(BaseModel):
    """一次 DAG 调度的收尾报告（结果 + 剪枝报告 + 分配留痕 + 成本）。

    阶段一同步调度器与阶段四异步调度器共用此契约；
    审计器 / 成本核算 / 学习引擎统一消费它。
    final_status：success | partial | failed | cancelled（阶段四外部取消）。
    """
    dag: DAG
    results: dict[str, Result] = Field(default_factory=dict)
    prune_reports: list[PruneReport] = Field(default_factory=list)
    assignments: list[Assignment] = Field(default_factory=list)
    total_cost: float = 0.0
    final_status: str = "success"
