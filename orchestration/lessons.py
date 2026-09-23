"""经验库与提示词顾问（架构 §3.2 学习层闭环）。

学习层此前"能跑但没人调、算完即扔"：规则提取成立，但既不落盘也无消费方
（§3.2 承诺的"规则库/经验库"零实现）。本模块补齐闭环的两端：

**① 经验库（跨 run 记忆）**
`Lesson` = 单条规则在**跨 run**尺度上的聚合视图：命中次数、贡献 run 数、最高
severity、最近一次的证据。`build_digest()` 把 StateStore 里累积的行聚合成
`LessonDigest`——单 run 内 `failure_pattern_min=2` 意义有限，跨 run 复现才是
"客观支撑"。落盘由 StateStore（`learning_lessons` 表）负责。

**② 消费方 = 拆解提示词（闭环出口）**
`PromptAdvisor.guidance()` 产出一段**可追溯、有数值、有上限**的提示词指导块，
注入拆解引擎（`decomposer.Decomposer(guidance_provider=...)`）：

- **注册表客观事实**（直接测量，无需阈值）：实际可用模型列表、已注册能力标签。
  直击 DEG-1（建议的 model 无可用 agent → 降级）与 CAP-1（能力标签不存在 →
  风险分配）——这两类返工可以在拆解期就避免。
- **经验库规则**（需跨 run 复现，`min_occurrences` 门槛）：每条都带
  "既往 N 次运行命中 M 次"的数值支撑，不给无证据的建议。
- **判定结论**（LLM 来源，非确定）**单独成节**并显式标注——客观与判定
  **禁止同级呈现**（否则把非确定结论伪装成事实）。

红线：本模块**只读**——不采集、不改状态、不阻断；提示词指导是 advisory，
拆解引擎照旧受 Schema 七关校验约束。生成失败一律退化为"无指导块"，
绝不把拆解链路拖垮。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

from pydantic import BaseModel, Field

from .learning import (
    JUDGMENT_CATEGORIES,
    OBJECTIVE_CATEGORIES,
    LearningReport,
    tier_of,
)
from .registry import AgentRegistry

# 对"怎么拆"有直接指导意义的类别（其余类别是框架自身问题，不该进提示词）
PROMPT_CATEGORIES: frozenset[str] = (
    frozenset({"failure_pattern", "degraded_assignment", "capability_risk",
               "budget_overrun", "pruning_quality", "goal_mismatch"})
)

_SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1}


class Lesson(BaseModel):
    """一条规则在跨 run 尺度上的聚合（经验库视图）。"""

    rule_id: str
    tier: str  # objective | judgment
    severity: str  # high | medium | low
    category: str
    message: str
    action: str = ""
    evidence: dict = Field(default_factory=dict)
    occurrences: int = 0
    """命中次数（跨 run 累加）。"""
    runs: int = 0
    """贡献该规则的 run 数——复现证据强度。"""
    last_run_id: str = ""
    last_seen: str = ""

    @property
    def objective(self) -> bool:
        return self.tier == "objective"


class LessonDigest(BaseModel):
    """经验库聚合视图（跨 run）。"""

    lessons: list[Lesson] = Field(default_factory=list)
    runs_considered: int = 0
    """贡献过经验的 run 数（经验库规模）。"""
    generated_at: str = ""

    def of_tier(self, tier: str) -> list[Lesson]:
        return [ls for ls in self.lessons if ls.tier == tier]


def build_digest(rows: Iterable[dict]) -> LessonDigest:
    """把经验库原始行聚合成 digest。

    rows：StateStore.load_lessons() 的输出，每行
    {run_id, rule_id, tier, severity, category, message, action, evidence,
     created_at}。
    """
    rows = list(rows)
    by_rule: dict[str, Lesson] = {}
    run_ids: set[str] = set()
    latest: dict[str, str] = {}
    for r in rows:
        rid = r.get("rule_id") or ""
        if not rid:
            continue
        run_id = r.get("run_id") or ""
        created = r.get("created_at") or ""
        if run_id:
            run_ids.add(run_id)
        seen = latest.get(rid)
        lesson = by_rule.get(rid)
        if lesson is None:
            lesson = Lesson(
                rule_id=rid,
                tier=r.get("tier") or "objective",
                severity=r.get("severity") or "low",
                category=r.get("category") or "",
                message=r.get("message") or "",
                action=r.get("action") or "",
                evidence=r.get("evidence") or {},
                last_run_id=run_id,
                last_seen=created,
            )
            by_rule[rid] = lesson
            latest[rid] = created
        elif created >= seen:  # 取最近一次的文本与证据（ISO 时间串可直接比较）
            lesson.severity = r.get("severity") or lesson.severity
            lesson.message = r.get("message") or lesson.message
            lesson.action = r.get("action") or lesson.action
            lesson.evidence = r.get("evidence") or lesson.evidence
            lesson.last_run_id = run_id
            lesson.last_seen = created
            latest[rid] = created
        # 最高 severity 优先（跨 run 取严重者）
        if _sev(lesson.severity) < _sev(r.get("severity") or ""):
            lesson.severity = r.get("severity")

    # 复现证据：occurrences = 行数；runs = 该规则贡献的 run 数
    counts: dict[str, int] = {}
    rule_runs: dict[str, set[str]] = {}
    for r in rows:
        rid = r.get("rule_id") or ""
        if not rid or rid not in by_rule:
            continue
        counts[rid] = counts.get(rid, 0) + 1
        if r.get("run_id"):
            rule_runs.setdefault(rid, set()).add(r["run_id"])
    for rid, lesson in by_rule.items():
        lesson.occurrences = counts.get(rid, 0)
        lesson.runs = len(rule_runs.get(rid, ()))

    lessons = sorted(
        by_rule.values(),
        key=lambda ls: (-_sev(ls.severity), -ls.occurrences, ls.rule_id),
    )
    return LessonDigest(
        lessons=lessons,
        runs_considered=len(run_ids),
        generated_at=_now(),
    )


class PromptAdvisor:
    """把经验库 + 注册表事实转成拆解提示词的指导块（学习层闭环出口）。

    min_occurrences：规则进入提示词所需的**跨 run 复现**次数（默认 2）——
    "客观支撑"即复现证据，单次偶发不写成指导。
    max_items：指导块条数上限（提示词要控规模，不能把经验库全文塞进去）。
    include_judgment：是否附上判定结论（非确定来源，单独成节并标注）。
    """

    OBJECTIVE_HEADER = "【历史经验（学习层自动生成，来自既往运行的事实统计）】"
    JUDGMENT_HEADER = "【判定结论（非确定来源，仅供参考，不要当作硬性验收标准）】"

    def __init__(
        self,
        registry: Optional[AgentRegistry] = None,
        lesson_store: Optional[object] = None,
        min_occurrences: int = 2,
        max_items: int = 8,
        include_judgment: bool = True,
    ):
        self._registry = registry
        self._store = lesson_store
        self.min_occurrences = min_occurrences
        self.max_items = max_items
        self.include_judgment = include_judgment

    # ------------------------------------------------------------------

    def digest(self) -> LessonDigest:
        """读经验库（无存储 / 读失败 → 空 digest，闭环退化为仅注册表事实）。"""
        if self._store is None:
            return LessonDigest(generated_at=_now())
        try:
            rows = self._store.load_lessons()
        except Exception:
            return LessonDigest(generated_at=_now())
        return build_digest(rows)

    def registry_facts(self) -> list[str]:
        """注册表客观事实（直接测量，无需阈值）——防降级/防能力风险。"""
        lines: list[str] = []
        if self._registry is None:
            return lines
        agents = [a for a in self._registry.agents.values() if a.available]
        if not agents:
            return lines
        models = sorted({a.model for a in agents if a.model})
        caps = sorted({c for a in agents for c in (a.capabilities or []) if c})
        if models:
            lines.append(
                f"- 当前可用模型（注册表实测）：{', '.join(models)}。"
                "model 字段请从其中选择——建议不存在的模型会导致降级分配"
            )
        if caps:
            lines.append(
                f"- 当前已注册能力标签：{', '.join(caps)}。"
                "required_capabilities 只使用这些标签，否则会被判为能力风险"
            )
        return lines

    def guidance(self) -> str:
        """生成提示词指导块；无任何客观素材时返回空串（不污染提示词）。"""
        objective_lines = self._lesson_lines("objective")
        judgment_lines = (
            self._lesson_lines("judgment") if self.include_judgment else []
        )
        blocks: list[str] = []
        facts = self.registry_facts()
        if facts or objective_lines:
            blocks.append(
                "\n".join([self.OBJECTIVE_HEADER] + facts + objective_lines)
            )
        if judgment_lines:
            blocks.append("\n".join([self.JUDGMENT_HEADER] + judgment_lines))
        if not blocks:
            return ""
        blocks.append(
            "以上是既往运行的事实统计与注册表现状，用于避免重复返工；"
            "它们不改变下面的输出格式与硬性要求。"
        )
        return "\n\n".join(blocks)

    # ------------------------------------------------------------------

    def _lesson_lines(self, tier: str) -> list[str]:
        """按复现门槛与条数上限，取该 tier 的规则转成提示词条目。"""
        digest = self.digest()
        picked = [
            ls for ls in digest.lessons
            if ls.tier == tier
            and ls.category in PROMPT_CATEGORIES
            and ls.occurrences >= self.min_occurrences
        ]
        lines: list[str] = []
        for ls in picked[: self.max_items]:
            lines.append(
                f"- [{_tier_label(tier)}] 既往 {ls.runs} 次运行命中 "
                f"{ls.occurrences} 次：{_clip(ls.message, 140)}"
                + (f" → 建议：{_clip(ls.action, 100)}" if ls.action else "")
            )
        return lines


def _sev(value: str) -> int:
    return _SEVERITY_ORDER.get(value, 0)


def _tier_label(tier: str) -> str:
    return "客观" if tier == "objective" else "判定"


def _clip(text: str, limit: int) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "JUDGMENT_CATEGORIES",
    "Lesson",
    "LessonDigest",
    "OBJECTIVE_CATEGORIES",
    "PROMPT_CATEGORIES",
    "PromptAdvisor",
    "build_digest",
    "tier_of",
    "LearningReport",
]
