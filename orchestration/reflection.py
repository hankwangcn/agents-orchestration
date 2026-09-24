"""反思 / 判定模块（架构 §3.2 **治理层**）：产出是否达成原始目标。

定位：框架此前只在**契约/结构**层面判定"结果符合预期"（解析组件 + 审计对账），
产出**内容**正确性无人判定——格式完美但内容错误的结果会被判 SUCCESS。
本模块补上语义判定这一环。

判定基准（唯一有效）：**用户原始目标（goal）**。
- 子任务描述是框架自己的拆解产物，拿产出跟它比对 = 自己出题自己判（自证循环）；
- 子任务结果只作**证据**随附（让判定者知道过程中发生了什么），不作基准。

判定者（判定能力也是一种"能力"）：
- 优先用注册表中声明 `judge`/`reviewer` 能力的 **independent** agent
  （能力由 info_request 采集，与其它 agent 同构；可指向异构模型——
  同一个模型自己判自己不可信）；
- 无此类 agent 时，按 `allow_self_judge` 决定：降级为自判（在报告中标记
  `independent=False`，**明确这是可靠性打折的判定**）或直接跳过。

形态（复用既有机制，零新增协议）：
- 判定 = 一次普通的 `task_request`（desc 放判定指令、inputs 放基准与证据、
  output_schema 放判定结构），走适配器基类的协议装配 + 双层校验解析 +
  解析重试——**消息类型不新增、agent 零变更**；
- 判定输出受 output_schema 框架侧强校验（与任务产出同等严格）。

动作边界（**advisory，不阻断**）：
- 结论写进 ScheduleReport.reflection（报告可见）+ 喂学习层（JUD-* 规则）；
- **不改任务状态、不自动重跑、不阻断交付**——是否采纳由审计/人工定夺
  （副作用任务重派 = 副作用执行两次，是既有红线）。

审计边界（不得混淆）：审计是**只读、纯函数、结论可重复（可重放）**的事实对账；判定是
**非确定、有成本**的语义结论。两者独立留痕，不合并——合并会破坏审计
结论可重复这一属性。判定产出不再被判定（递归边界）。调用次数与成本单列。
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

from pydantic import BaseModel, Field

from .adapters.base import AgentAdapter
from .agent_pool import RegisteredAgent, RegistryError
from .models import Assignment, ResourceRequirement, Result, ScheduleReport, Task
from .registry import AgentRegistry
from .timeouts import call_with_timeout

# 声明了任一标签即视为"判定 agent"（能力是问出来的——同 capability 采集口径）
JUDGE_CAPABILITIES: tuple[str, ...] = ("judge", "reviewer", "reflection")

REFLECTION_TASK_ID = "__reflection__"

# 判定结构（output_schema 框架侧强校验——判定输出与任务产出一视同仁）
VERDICT_SCHEMA: dict = {
    "achieved": "boolean",
    "score": "number",
    "reasons": ["string"],
    "gaps": ["string"],
}

REFLECTION_PROMPT = """你是结果判定组件。你只做一件事：判断"最终交付"是否达成了"原始目标"。

INPUT 里是你的判定素材（JSON）：
- goal：用户的原始目标——**唯一判定基准**
- deliverables：最终交付任务的产出
- evidence：过程任务的结果摘要（含失败 / 被剪枝的情况）
- facts：本次运行的事实（终态、成本、状态计数、剪枝统计）

判定要求：
1. 基准只能是 goal。**不要把某个子任务的描述当作验收标准**——子任务是
   拆分产物，用它当基准等于自己出题自己判。
2. deliverables 是待验对象，evidence 只是理解过程的证据。
3. 只判断"交付是否达成目标"；不评价过程好坏，不提执行建议。
4. 目标未达成时，在 gaps 里逐条指出还差什么、缺什么。
5. 证据不足或信息矛盾时给低 score，并在 reasons 里说明不确定性，不要编造。

只输出一个 JSON 对象（放在代码块内，不要其他内容）：
{"achieved": true, "score": 0.9, "reasons": ["..."], "gaps": []}
"""

# 正确判定示例：解析失败重试时随修正提示给出（口径同协议 §7.3）
REFLECTION_EXAMPLE: dict = {
    "achieved": False,
    "score": 0.3,
    "reasons": ["只完成了数据抓取，最终报告未生成"],
    "gaps": ["缺少最终比价报告"],
}


class ReflectionReport(BaseModel):
    """一次判定的结论（advisory）。

    enabled=False 表示本次未做判定（无目标 / 无判定 agent）——`skipped_reason`
    说明原因；`judged=False` 且 `error_code` 非空表示判定了但未拿到结论。
    """

    enabled: bool = True
    skipped_reason: str = ""
    goal: str = ""
    judged: bool = False
    judge_agent: str = ""
    independent: bool = True
    """是否用了独立判定 agent（False = 降级自判，可靠性打折）。"""
    achieved: Optional[bool] = None
    score: Optional[float] = None
    reasons: list[str] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)
    error_code: str = ""
    error_message: str = ""
    request_id: str = ""
    cost: float = 0.0
    duration_ms: int = 0

    @property
    def ok(self) -> bool:
        """是否拿到了可用结论。"""
        return self.judged and self.achieved is not None


class Reflector:
    """运行级判定器：最终交付 × 原始目标 → ReflectionReport。

    registry：只读用途（挑判定 agent / 判定失败时降级分配），与审计器同构。
    max_chars：单个产出/摘要的截断上限（判定请求要控规模，不能把交付全文塞进去）。
    allow_self_judge：无独立判定 agent 时是否降级自判（默认允许，但报告里
    标记 independent=False）。
    """

    def __init__(
        self,
        registry: AgentRegistry,
        timeout_seconds: float = 120.0,
        max_chars: int = 4000,
        allow_self_judge: bool = True,
    ):
        self._registry = registry
        self.timeout_seconds = timeout_seconds
        self.max_chars = max_chars
        self.allow_self_judge = allow_self_judge

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    async def areflect(
        self,
        goal: str,
        report: ScheduleReport,
        run_id: str = "",
    ) -> ReflectionReport:
        """异步判定（网关 run 收尾后调用；不阻塞事件循环）。"""
        out = ReflectionReport(goal=goal or "")
        if not (goal or "").strip():
            out.enabled = False
            out.skipped_reason = "no_goal"  # 未提交目标 → 无基准可判，跳过
            return out

        picked = self._pick(goal, run_id)
        if isinstance(picked, ReflectionReport):
            return picked  # 无判定者（或不允许自判）→ 已带 skipped_reason
        out, task, assignment, adapter = picked

        inputs = self._build_inputs(goal, report)
        try:
            call = adapter.arun_task(task, request_id=out.request_id, inputs=inputs)
            if self.timeout_seconds and self.timeout_seconds > 0:
                result = await asyncio.wait_for(call, timeout=self.timeout_seconds)
            else:
                result = await call
        except asyncio.TimeoutError:
            out.error_code = "timeout"
            out.error_message = f"判定调用超过 {self.timeout_seconds}s（框架侧 wall-clock）"
            return out
        except Exception as e:  # 判定是 advisory：任何异常都不该影响 run 收尾
            out.error_code = "judge_error"
            out.error_message = str(e)
            return out
        self._apply(out, result)
        return out

    def reflect(
        self,
        goal: str,
        report: ScheduleReport,
        run_id: str = "",
    ) -> ReflectionReport:
        """同步判定（同步调度 / 库内直调用）。逻辑与 areflect 一致。"""
        out = ReflectionReport(goal=goal or "")
        if not (goal or "").strip():
            out.enabled = False
            out.skipped_reason = "no_goal"
            return out

        picked = self._pick(goal, run_id)
        if isinstance(picked, ReflectionReport):
            return picked
        out, task, assignment, adapter = picked

        inputs = self._build_inputs(goal, report)

        def _call() -> Result:
            return adapter.run_task(task, request_id=out.request_id, inputs=inputs)

        try:
            if self.timeout_seconds and self.timeout_seconds > 0:
                result = call_with_timeout(_call, self.timeout_seconds)
            else:
                result = _call()
        except TimeoutError as e:
            out.error_code = "timeout"
            out.error_message = str(e)
            return out
        except Exception as e:
            out.error_code = "judge_error"
            out.error_message = str(e)
            return out
        self._apply(out, result)
        return out

    # ------------------------------------------------------------------
    # 判定者选择
    # ------------------------------------------------------------------

    def _pick(
        self,
        goal: str,
        run_id: str,
    ) -> "tuple[ReflectionReport, Task, Assignment, AgentAdapter] | ReflectionReport":
        """挑判定 agent。返回 (报告, 任务, 分配, 适配器) 或"跳过"的报告。"""
        out = ReflectionReport(goal=goal)
        out.request_id = f"reflection:{run_id or 'run'}"

        task = Task(
            id=REFLECTION_TASK_ID,
            desc=REFLECTION_PROMPT,
            required_capabilities=["judge"],
            output_schema=VERDICT_SCHEMA,
            required_resources=ResourceRequirement(timeout=int(self.timeout_seconds)),
        )

        judge = self._judge_agent()
        if judge is not None:
            assignment = Assignment(
                task_id=task.id,
                agent_id=judge.agent_id,
                match_type="capability",
                reason=f"判定能力匹配：{sorted(set(judge.capabilities) & set(JUDGE_CAPABILITIES))}",
            )
            out.judge_agent = judge.agent_id
            out.judged = True  # 已选定判定者 → 判定已发起（结论可能失败）
            return out, task, assignment, judge.adapter

        if not self.allow_self_judge:
            out.enabled = False
            out.skipped_reason = "no_judge_agent"
            return out
        # 降级自判：走正常分配（默认通用 agent），报告中标记 non-independent
        try:
            assignment, adapter = self._registry.assign(task)
        except RegistryError as e:
            out.enabled = False
            out.skipped_reason = f"no_agent: {e}"
            return out
        out.independent = False
        out.judge_agent = assignment.agent_id
        out.judged = True
        return out, task, assignment, adapter

    def _judge_agent(self) -> Optional[RegisteredAgent]:
        """注册表中声明了判定能力的可用 agent（能力是问出来的）。"""
        for a in self._registry.agents.values():
            if a.available and set(a.capabilities) & set(JUDGE_CAPABILITIES):
                return a
        return None

    # ------------------------------------------------------------------
    # 素材组装
    # ------------------------------------------------------------------

    def _build_inputs(self, goal: str, report: ScheduleReport) -> dict:
        """基准（goal）+ 待验（交付产出）+ 证据（过程摘要）+ 事实。"""
        dag = report.dag
        finals = dag.final_tasks()
        deliverables: list[dict] = []
        evidence: list[dict] = []
        for tid, t in dag.tasks.items():
            res = t.result
            if tid in finals:
                deliverables.append({
                    "task_id": tid,
                    "desc": t.desc,
                    "output": self._clip(res.output if res else None),
                })
            else:
                evidence.append({
                    "task_id": tid,
                    "status": t.status.value,
                    "desc": t.desc,
                    "summary": self._summarize(res),
                })
        counts: dict[str, int] = {}
        for t in dag.tasks.values():
            counts[t.status.value] = counts.get(t.status.value, 0) + 1
        return {
            "goal": goal,
            "deliverables": deliverables,
            "evidence": evidence,
            "facts": {
                "final_status": report.final_status,
                "total_cost": report.total_cost,
                "status_counts": counts,
                "pruned_task_count": sum(len(p.pruned) for p in report.prune_reports),
                "pruned_final": any(p.pruned_final for p in report.prune_reports),
            },
        }

    def _clip(self, value: Any) -> Any:
        """截断单个产出（判定请求要控规模；超限以省略标记说明）。"""
        if value is None:
            return None
        text = (
            value if isinstance(value, str)
            else json.dumps(value, ensure_ascii=False, default=str)
        )
        if len(text) <= self.max_chars:
            return value
        return text[: self.max_chars] + f"...(已截断 {len(text) - self.max_chars} 字符)"

    def _summarize(self, res: Optional[Result]) -> str:
        """过程任务的单行摘要（失败给错误码，成功给截断产出）。"""
        if res is None:
            return ""
        if not res.success:
            code = res.error.code if res.error else "unknown"
            msg = res.error.message if res.error else ""
            return f"失败（{code}）{msg}"[: self.max_chars]
        text = (
            res.output if isinstance(res.output, str)
            else json.dumps(res.output, ensure_ascii=False, default=str)
        )
        return text[: self.max_chars]

    # ------------------------------------------------------------------
    # 结论落地
    # ------------------------------------------------------------------

    def _apply(self, out: ReflectionReport, result: Result) -> None:
        """把判定调用的 Result 收敛成 ReflectionReport（不改任何任务状态）。"""
        out.judged = True
        out.request_id = result.request_id or out.request_id
        out.cost = result.usage.cost
        out.duration_ms = result.duration_ms
        if not result.success:
            out.error_code = result.error.code if result.error else "judge_failed"
            out.error_message = result.error.message if result.error else ""
            return
        verdict = result.output if isinstance(result.output, dict) else {}
        achieved = verdict.get("achieved")
        out.achieved = achieved if isinstance(achieved, bool) else None
        score = verdict.get("score")
        out.score = (
            float(score)
            if isinstance(score, (int, float)) and not isinstance(score, bool)
            else None
        )
        out.reasons = [r for r in (verdict.get("reasons") or []) if isinstance(r, str)]
        out.gaps = [g for g in (verdict.get("gaps") or []) if isinstance(g, str)]
