"""人读叙述摘要（LLM 产出；**显式生成、非确定、单独留痕**）。

定位：运行存档的**确定性人读投影**（`report_view`）讲事实、可复跑；叙述摘要
讲"一段话说清得失"——这是 LLM 产出，**非确定、有成本**。因此它：

- 只**显式触发**（`POST /api/runs/{id}/narrative`），绝不默认生成；
- 单独成节并**标注来源**（model / 生成时间），**不混入确定性报告**；
- 与审计（确定性、可复跑）分离留痕——同"判定 vs 审计"的口径。

形态：框架内部 LLM 调用，**不走消息协议**（同拆解引擎）——复用底层聊天入口，
输入 = 从运行存档投影出的结构化事实（**不是**原始存档全量），输出 = 一段面向
人的总结。生成失败收敛为异常，由调用方决定呈现，不影响运行存档本身。
"""
from __future__ import annotations

import asyncio
import json
from typing import Callable, Optional

NARRATIVE_PROMPT_VERSION = "v1"

NARRATIVE_PROMPT = """你是交付复盘助手。根据下面这段"运行存档事实"，写一段面向人的总结（2-4 句，中文）。

要求：
1. 只陈述事实与得失（做成了什么、发生了什么问题、花了多少），不编造未提供的信息；
2. 不给出后续建议——建议由学习层规则单独给出；
3. 直接输出总结正文，不要标题、不要 JSON、不要 Markdown 标记。

运行存档事实（JSON）：
{facts}
"""


class NarrativeError(Exception):
    """叙述生成失败（LLM 不可用 / 返回空）。"""


class Narrator:
    """把运行存档投影为一段人读总结（框架内部 LLM 调用，非确定）。

    llm_call：接收完整提示词文本、返回模型原始响应（复用适配器裸聊天入口）。
    max_chars：事实摘要的截断上限（提示词要控规模）。
    """

    def __init__(
        self,
        llm_call: Callable[[str], str],
        model: str = "deepseek-chat",
        max_chars: int = 6000,
    ):
        self._llm_call = llm_call
        self.model = model
        self.max_chars = max_chars
        self.last_prompt = ""

    def narrate(self, view: dict) -> dict:
        """同步生成（库内直调用）。返回 {text, model, created_at}。"""
        facts = _facts(view, self.max_chars)
        prompt = NARRATIVE_PROMPT.format(facts=facts)
        self.last_prompt = prompt
        try:
            raw = self._llm_call(prompt)
        except Exception as e:  # 网络/鉴权等 → 收敛为叙述失败
            raise NarrativeError(f"叙述调用失败：{e}") from e
        text = (raw or "").strip()
        if not text:
            raise NarrativeError("叙述调用返回空内容")
        return {"text": text, "model": self.model, "created_at": _now()}

    async def anarrate(self, view: dict) -> dict:
        """异步生成（网关：丢线程池避免阻塞事件循环）。"""
        return await asyncio.to_thread(self.narrate, view)


def _facts(view: dict, max_chars: int) -> str:
    """从运行存档视图提取紧凑事实（控规模；不塞原始产出全量）。"""
    a = view.get("archive") or {}
    s = view.get("summary") or {}
    gov = view.get("governance") or {}
    learning = view.get("learning") or {}
    audit = gov.get("audit") or {}
    ref = gov.get("reflection") or {}

    facts = {
        "goal": view.get("goal") or "（未提交原始目标）",
        "final_status": a.get("final_status"),
        "total_cost": a.get("total_cost"),
        "duration_ms": a.get("duration_ms"),
        "tasks": {
            "total": s.get("tasks_total"),
            "success": s.get("success"),
            "failed": s.get("failed"),
            "cancelled": s.get("cancelled"),
            "skipped": s.get("skipped"),
            "success_rate": s.get("success_rate"),
        },
        "failures": [
            {"task_id": t.get("task_id"), "error": t.get("error_code")}
            for t in view.get("tasks") or []
            if t.get("status") == "failed"
        ],
        "pruned": [
            p.get("root_failure", {}).get("task_id")
            for p in view.get("prunes") or []
        ],
        "audit_verdict": audit.get("verdict"),
        "audit_issues": audit.get("issues") or [],
        "judgment": {
            "judged": bool(ref.get("enabled")),
            "achieved": ref.get("achieved"),
            "gaps": ref.get("gaps") or [],
        } if ref else None,
        "learning_rules": [
            {"rule_id": r.get("rule_id"), "message": r.get("message")}
            for r in learning.get("rules") or []
        ],
    }
    text = json.dumps(facts, ensure_ascii=False, default=str)
    if len(text) > max_chars:
        text = text[:max_chars] + f"...(已截断 {len(text) - max_chars} 字符)"
    return text


def make_default_narrator(
    model: str = "deepseek-chat",
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Narrator:
    """默认叙述器：复用 DeepSeek 适配器裸聊天入口（同一套端点与鉴权）。

    api_key 缺省回落到环境变量 DEEPSEEK_API_KEY；两者皆无则抛 ValueError——
    由调用方决定是否启用叙述能力。
    """
    from .adapters.deepseek import DeepSeekAdapter

    kwargs: dict = {"model": model}
    if api_key:
        kwargs["api_key"] = api_key
    if base_url:
        kwargs["base_url"] = base_url
    adapter = DeepSeekAdapter(**kwargs)
    return Narrator(adapter.chat, model=model)


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "NARRATIVE_PROMPT_VERSION",
    "NarrativeError",
    "Narrator",
    "make_default_narrator",
]
