"""自我学习（架构 §3.2 学习层；阶段二）。

输入：AuditReport + CostReport（审计/成本核算产出的事实）+ 可选
ReflectionReport（治理层反思/判定结论——目标是否达成）。
输出：LearningReport——启发式规则提取（失败模式 / 降级 / 风险分配 / 预算超支 /
剪枝质量 / 目标达成度）。

定位：把审计数字与判定结论转成"可执行的拆解/分配优化建议"，规则带证据与
动作建议，经经验库（lessons.py）落盘并回馈拆解提示词——闭环出口。

**证据强度分级（客观性分层）**：规则按来源分两级，禁止同级呈现——
- ``objective``：确定性事实（审计对账 / 分配留痕 / 成本核算 / 剪枝统计），
  只读、纯函数、结论可重复；**只有这一级可以作为提示词的硬性指导**；
- ``judgment``：LLM 判定结论（JUD-*），非确定、有成本、不可重放；只作
  参考随附，不得伪装成事实。

纯规则引擎，不做 LLM 复盘——可观测、可测试。
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .audit import AuditReport
from .cost import CostReport
from .reflection import ReflectionReport

# 客观（确定性）来源的规则类别：审计事实对账 / 分配留痕 / 成本核算
OBJECTIVE_CATEGORIES: frozenset[str] = frozenset({
    "reconciliation",       # REC-1 / INT-1：框架自身断链、崩溃点副作用
    "failure_pattern",      # FP-*：错误码频次
    "degraded_assignment",  # DEG-1：降级占比
    "capability_risk",      # CAP-1：风险分配失败率
    "budget_overrun",       # BUG-*：预算声明对账
    "pruning_quality",      # PRU-1：剪枝统计
})

# 判定（LLM）来源的规则类别：非确定，有成本，不可重放
JUDGMENT_CATEGORIES: frozenset[str] = frozenset({
    "goal_mismatch",  # JUD-1：目标未达成
    "reflection",     # JUD-2：判定未产出结论
})


def tier_of(category: str) -> str:
    """规则类别 → 证据强度分级（objective | judgment）。"""
    return "judgment" if category in JUDGMENT_CATEGORIES else "objective"


class LearningRule(BaseModel):
    rule_id: str
    severity: str  # high | medium | low
    category: str  # failure_pattern | degraded_assignment | capability_risk
    #              | budget_overrun | pruning_quality | reconciliation
    #              | goal_mismatch | reflection
    tier: str = ""
    """证据强度分级：objective（确定性事实）| judgment（LLM 判定）。"""
    message: str
    evidence: dict = Field(default_factory=dict)
    action: str = ""
    """建议动作（喂拆解优化/分配调整）。"""

    @property
    def objective(self) -> bool:
        return self.tier == "objective"


class LearningReport(BaseModel):
    rules: list[LearningRule] = Field(default_factory=list)
    rule_count: int = 0

    def add(self, rule: LearningRule) -> None:
        # 分级统一在唯一入口补齐：判定类规则不得与确定性事实同级呈现
        if not rule.tier:
            rule.tier = tier_of(rule.category)
        self.rules.append(rule)
        self.rule_count = len(self.rules)


class LearningEngine:
    """从审计/成本报告提取规则。阈值可配置，便于测试与调参。"""

    def __init__(
        self,
        degraded_ratio_threshold: float = 0.2,
        failure_pattern_min: int = 2,
        risk_failure_ratio_threshold: float = 0.3,
    ):
        self.degraded_ratio_threshold = degraded_ratio_threshold
        self.failure_pattern_min = failure_pattern_min
        self.risk_failure_ratio_threshold = risk_failure_ratio_threshold

    # ------------------------------------------------------------------

    def learn(
        self,
        audit: AuditReport,
        cost: CostReport,
        reflection: Optional[ReflectionReport] = None,
    ) -> LearningReport:
        out = LearningReport()
        self._learn_reconciliation(out, audit)
        self._learn_failure_patterns(out, audit)
        self._learn_degraded(out, audit)
        self._learn_capability_risk(out, audit)
        self._learn_budget(out, cost)
        self._learn_pruning(out, audit)
        self._learn_interrupted(out, audit)
        self._learn_reflection(out, reflection)
        return out

    # ------------------------------------------------------------------

    def _learn_reconciliation(self, out: LearningReport, audit: AuditReport) -> None:
        """正确性硬问题：断链/矛盾——优先级最高，先暴露再谈优化。"""
        if audit.missing_results or audit.status_result_mismatches:
            out.add(LearningRule(
                rule_id="REC-1",
                severity="high",
                category="reconciliation",
                message=(
                    "结果契约对账失败：存在缺契约或状态矛盾的任务，"
                    "解析组件/协议链路需排查"
                ),
                evidence={
                    "missing": audit.missing_results,
                    "mismatches": audit.status_result_mismatches,
                },
                action="检查 validation.py 解析兜底与适配器模板装配",
            ))

    def _learn_failure_patterns(self, out: LearningReport, audit: AuditReport) -> None:
        """高频错误码 → 沉淀"避免该失败"的拆解规则。"""
        for pat in audit.error_patterns:
            if pat["count"] < self.failure_pattern_min:
                continue
            code = pat["code"]
            out.add(LearningRule(
                rule_id=f"FP-{code[:16]}",
                severity="high" if pat["count"] >= 3 else "medium",
                category="failure_pattern",
                message=f"错误码 {code} 出现 {pat['count']} 次："
                        f"该类型任务反复失败，拆解时需规避或换 agent",
                evidence={"code": code, "count": pat["count"],
                          "task_ids": pat["task_ids"]},
                action=(
                    "拆解 prompt 中对该类任务标注更高能力要求，"
                    "或分配时优先匹配专用 agent"
                ),
            ))

    def _learn_degraded(self, out: LearningReport, audit: AuditReport) -> None:
        """降级占比过高 → 拆解 model 建议与注册表脱节。"""
        total = sum(audit.assignments_by_type.values())
        if total == 0:
            return
        degraded_n = len(audit.degraded_tasks)
        ratio = degraded_n / total
        if degraded_n and ratio >= self.degraded_ratio_threshold:
            out.add(LearningRule(
                rule_id="DEG-1",
                severity="medium",
                category="degraded_assignment",
                message=(
                    f"{degraded_n}/{total} 任务降级分配（{ratio:.0%}），"
                    "拆解建议的 model 与注册表匹配度差"
                ),
                evidence={"degraded": degraded_n, "total": total, "ratio": ratio},
                action="核对拆解 prompt 的 model 建议逻辑，或注册表补充对应模型 agent",
            ))

    def _learn_capability_risk(self, out: LearningReport, audit: AuditReport) -> None:
        """风险分配（能力声明未覆盖）且失败率高 → 声明与真实能力脱节。"""
        if not audit.risky_tasks:
            return
        risky_n = len(audit.risky_tasks)
        risky_failed = sum(
            1 for r in audit.risky_tasks if r.get("task_status") == "failed"
        )
        if risky_failed and risky_failed / risky_n >= self.risk_failure_ratio_threshold:
            out.add(LearningRule(
                rule_id="CAP-1",
                severity="high",
                category="capability_risk",
                message=(
                    f"能力声明未覆盖的任务失败率 {risky_failed}/{risky_n}："
                    "agent 能力声明与实际任务需求脱节"
                ),
                evidence={"risky": risky_n, "failed": risky_failed,
                          "tasks": [r["task_id"] for r in audit.risky_tasks]},
                action="重新采集 capability 声明（info_request）或拆解时标注能力标签",
            ))

    def _learn_budget(self, out: LearningReport, cost: CostReport) -> None:
        """预算超支 → 声明预算与实际成本脱节。"""
        for ob in cost.over_budget_agents:
            out.add(LearningRule(
                rule_id=f"BUG-{ob['agent_id']}",
                severity="medium",
                category="budget_overrun",
                message=(
                    f"agent {ob['agent_id']} 超支 {ob['overspend']}$"
                    f"（预算 {ob['budget']}$，实花 {ob['spent']}$）"
                ),
                evidence=ob,
                action="更新该 agent 预算声明，或拆解时收紧任务 budget 上限",
            ))

    def _learn_interrupted(self, out: LearningReport, audit: AuditReport) -> None:
        """中断任务（断点恢复后待人工）→ 检查崩溃原因与副作用声明质量。"""
        if not audit.interrupted_tasks:
            return
        out.add(LearningRule(
            rule_id="INT-1",
            severity="medium",
            category="reconciliation",
            message=(
                f"{len(audit.interrupted_tasks)} 个任务断点恢复后中断待人工确认："
                "进程崩溃点存在副作用任务（不自动重派，避免副作用执行两次）"
            ),
            evidence={
                "tasks": [
                    {"task_id": t["task_id"], "side_effects": t["side_effects"]}
                    for t in audit.interrupted_tasks
                ]
            },
            action=(
                "排查进程崩溃原因（StateStore 事件驱动写入是否生效）；"
                "人工 resolve（complete/cancel/retry）后重新 resume"
            ),
        ))

    def _learn_pruning(self, out: LearningReport, audit: AuditReport) -> None:
        """剪枝（含 final 被剪）→ 拆解并行度/依赖质量复盘。"""
        if audit.prune_events == 0:
            return
        out.add(LearningRule(
            rule_id="PRU-1",
            severity="high" if audit.pruned_final_events else "medium",
            category="pruning_quality",
            message=(
                f"{audit.prune_events} 次失败传播触发剪枝，"
                f"共剪 {audit.pruned_task_count} 个任务"
                + ("，且波及最终交付（整棵失败）" if audit.pruned_final_events else "")
                + "：拆解存在无意义并行或依赖过深"
            ),
            evidence={
                "prune_events": audit.prune_events,
                "pruned": audit.pruned_task_count,
                "pruned_final": audit.pruned_final_events,
                "roots": [r["task_id"] for r in audit.root_failures],
            },
            action=(
                "复盘拆解 DAG：减少不必要并行、合并低价值下游；"
                "根失败任务可降级/换 agent 重试"
            ),
        ))

    def _learn_reflection(
        self,
        out: LearningReport,
        reflection: Optional[ReflectionReport],
    ) -> None:
        """治理层判定结论 → 目标达成度规则（advisory，不改状态）。

        - JUD-1：判定认为**目标未达成**——最上位的信号（过程全绿但交付没达
          成目标，是拆解口径问题，不是执行问题）
        - JUD-2：判定了但没拿到结论（判定链路故障 / 超时）——低优先级，
          只提示判定能力本身需要修
        """
        if reflection is None or not reflection.enabled:
            return
        if reflection.achieved is False:
            out.add(LearningRule(
                rule_id="JUD-1",
                severity="high",
                category="goal_mismatch",
                message=(
                    "判定认为最终交付未达成原始目标"
                    + ("（非独立判定，可靠性打折）" if not reflection.independent else "")
                ),
                evidence={
                    "goal": reflection.goal,
                    "score": reflection.score,
                    "reasons": reflection.reasons,
                    "gaps": reflection.gaps,
                    "judge_agent": reflection.judge_agent,
                    "independent": reflection.independent,
                },
                action=(
                    "复盘拆解：目标里的交付物是否被拆成任务、依赖链是否覆盖到最终交付；"
                    "gaps 中的缺项应成为新的交付任务"
                ),
            ))
        elif reflection.error_code:
            out.add(LearningRule(
                rule_id="JUD-2",
                severity="low",
                category="reflection",
                message=(
                    f"判定未产出结论（{reflection.error_code}）："
                    "判定链路故障，结果达成度未知"
                ),
                evidence={
                    "judge_agent": reflection.judge_agent,
                    "error_code": reflection.error_code,
                    "error_message": reflection.error_message,
                },
                action="排查判定 agent 可用性 / 协议响应合规性，或放宽判定超时",
            ))
