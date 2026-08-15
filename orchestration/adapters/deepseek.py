"""DeepSeek 适配器（当前唯一的模型适配器；后续按 adapters/ 扩展）。

DeepSeek API 为 OpenAI 兼容协议——复用 openai SDK，仅 base_url 指向
https://api.deepseek.com。实现 _call_llm 即完成协议接入——协议装配与
双层校验解析由 AgentAdapter 基类统一提供。

api_key 默认从环境变量 DEEPSEEK_API_KEY 读取（构造时未显式传入）。

真实元数据回填（冒烟实测发现）：真实 LLM 不会自报 usage（协议 §4.2 的
agent 自报仅作兜底），真实 token / 耗时只能从 API 响应捕获——
_call_llm/_acall_llm 用 contextvars 记录（协程隔离，多协程共享实例无竞态），
_post_process 在解析后回填 Result.usage 与 duration_ms。

阶段四：_acall_llm 用 AsyncOpenAI 真异步——并发调度下网络等待不阻塞事件循环。
"""
from __future__ import annotations

import contextvars
import os
import time

from ..models import Result, Usage
from .base import AgentAdapter

# deepseek-chat 单价（USD / 1M tokens，2026-08 官方定价；如有调整改此处即可）
_PRICE_IN = 0.27
_PRICE_OUT = 1.10

# 协程级元数据：每次 _call_llm/_acall_llm 记录，_post_process 消费
_meta_ctx: contextvars.ContextVar[dict] = contextvars.ContextVar(
    "deepseek_llm_meta", default={}
)


class DeepSeekAdapter(AgentAdapter):
    def __init__(
        self,
        model: str = "deepseek-chat",
        api_key: str | None = None,
        base_url: str = "https://api.deepseek.com",
        template_mode: str = "full",
        temperature: float = 0.2,
        max_tokens: int = 4096,
    ):
        super().__init__(model=model, template_mode=template_mode)
        # 延迟导入：测试环境无 openai 包时不影响其余模块
        from openai import AsyncOpenAI, OpenAI

        key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        if not key:
            raise ValueError(
                "未提供 api_key 且环境变量 DEEPSEEK_API_KEY 未设置"
            )
        self._client = OpenAI(api_key=key, base_url=base_url)
        self._aclient = AsyncOpenAI(api_key=key, base_url=base_url)
        self.temperature = temperature
        self.max_tokens = max_tokens

    # -- 模型调用 + 真实元数据捕获 --------------------------------------

    def _call_llm(self, messages: list[dict]) -> str:
        t0 = time.monotonic()
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        _meta_ctx.set(_extract_meta(resp, t0))
        return resp.choices[0].message.content or ""

    async def _acall_llm(self, messages: list[dict]) -> str:
        t0 = time.monotonic()
        resp = await self._aclient.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        _meta_ctx.set(_extract_meta(resp, t0))
        return resp.choices[0].message.content or ""

    def _post_process(self, result: Result) -> Result:
        """回填真实 token / 耗时 / 成本（agent 自报值为 0 时覆盖）。

        真实 API 值优先：LLM 自报的 usage 不可信（冒烟实测全 0）。
        """
        meta = _meta_ctx.get()
        if not meta:
            return result
        tokens_in = meta.get("tokens_in") or 0
        tokens_out = meta.get("tokens_out") or 0
        if tokens_in or tokens_out:
            result.usage = Usage(
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                cost=round(
                    tokens_in / 1_000_000 * _PRICE_IN
                    + tokens_out / 1_000_000 * _PRICE_OUT,
                    6,
                ),
            )
        if result.duration_ms == 0 and meta.get("duration_ms"):
            result.duration_ms = meta["duration_ms"]
        return result

    async def achat(self, prompt: str) -> str:
        """异步裸聊天入口：供拆解引擎等框架内部组件复用同一适配器。"""
        resp = await self._aclient.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        return resp.choices[0].message.content or ""

    def chat(self, prompt: str) -> str:
        """裸聊天入口：供拆解引擎（框架内部组件）复用同一适配器。"""
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )
        return resp.choices[0].message.content or ""


def _extract_meta(resp: object, t0: float) -> dict:
    """从 OpenAI 兼容响应提取 token / 耗时。"""
    usage = getattr(resp, "usage", None)
    tokens_in = getattr(usage, "prompt_tokens", None) if usage else None
    tokens_out = getattr(usage, "completion_tokens", None) if usage else None
    return {
        "tokens_in": int(tokens_in) if tokens_in else 0,
        "tokens_out": int(tokens_out) if tokens_out else 0,
        # 真实调用耗时至少 1ms；0 视为无意义（mock/异常路径）
        "duration_ms": max(1, int((time.monotonic() - t0) * 1000)),
    }
