"""自我学习（架构 §3.2 学习层；阶段二）。

输入：AuditReport + CostReport（审计/成本核算产出的事实）。
输出：LearningReport——启发式规则提取（失败模式 / 降级 / 风险分配 / 预算超支 / 剪枝质量）。

定位：把审计数字转成"可执行的拆解/分配优化建议"，规则带证据与动作建议，
供阶段四及后续拆解优化闭环消费。纯规则引擎，不做 LLM 复盘——可观测、可测试。
"""
from __future__ import annotations

from pydantic import BaseModel, Field

from .audit import AuditReport
from .cost import CostReport


class LearningRule(BaseModel):
    rule_id: str
    severity: str  # high | medium | low
    category: str  # failure_pattern | degraded_assignment | capability_risk
    #              | budget_overrun | pruning_quality | reconciliation
    message: str
    evidence: dict = Field(default_factory=dict)
    action: str = ""
    """建议动作（喂拆解优化/分配调整）。"""


class LearningReport(BaseModel):
    rules: list[LearningRule] = Field(default_factory=list)
    rule_count: int = 0

    def add(self, rule: LearningRule) -> None:
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

    def learn(self, audit: AuditReport, cost: CostReport) -> LearningReport:
        out = LearningReport()
        self._learn_reconciliation(out, audit)
        self._learn_failure_patterns(out, audit)
        self._learn_degraded(out, audit)
        self._learn_capability_risk(out, audit)
        self._learn_budget(out, cost)
        self._learn_pruning(out, audit)
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
                    "解析层/协议链路需排查"
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
