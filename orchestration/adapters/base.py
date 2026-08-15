"""Agent 适配器抽象基类（架构 §3.2 执行层）。

职责：装配协议提示词（模板 + 请求 JSON）→ 调用模型 → 双层校验解析响应。
实现者只需提供 _call_llm（模型调用），协议装配与解析完全复用。

同步 / 异步双路径（阶段四并发调度）：
- run_task / run_info / cancel —— 同步路径（串行调度器 / 同步环境）
- arun_task / arun_info / acancel —— 异步路径（AsyncScheduler 使用）。
  默认实现把同步 _call_llm 丢到线程池（asyncio.to_thread），
  任何只有同步实现的 adapter 无需改动即可被并发调度；
  追求真异步的 adapter（如 DeepSeekAdapter）override _acall_llm 即可全异步。

取消契约（D5）：cancel 为 best-effort，不保证 agent 一定停止——
晚到结果由调度器丢弃（架构 §5.3）。
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional

from ..models import Result, Task
from ..protocol import (
    build_task_request,
    build_info_request,
    render,
)
from ..validation import aparse_result, parse_result


class AgentAdapter(ABC):
    """对接任意 agent：装配协议 → 调用 → 解析。

    template_mode: "full"（默认）/ "simple"（低能力模型，协议 §7.8）。
    """

    def __init__(self, model: str = "deepseek-chat", template_mode: str = "full"):
        self.model = model
        self.template_mode = template_mode

    # -- 实现者必须提供 ------------------------------------------------

    @abstractmethod
    def _call_llm(self, messages: list[dict]) -> str:
        """调用底层模型，返回原始响应文本。"""

    async def _acall_llm(self, messages: list[dict]) -> str:
        """异步调用底层模型。默认：同步实现丢线程池（不阻塞事件循环）。

        追求真异步的 adapter（如 DeepSeekAdapter 用 AsyncOpenAI）override 此方法。
        """
        return await asyncio.to_thread(self._call_llm, messages)

    # -- 可选钩子 -------------------------------------------------------

    def _post_process(self, result: Result) -> Result:
        """解析完成后的钩子：adapter 可用真实 API 元数据回填 Result。

        真实 LLM 不会自报 usage（协议 §4.2 的 agent 自报仅作兜底），
        真实 token / 耗时只能由 adapter 层从 API 响应捕获——
        DeepSeekAdapter 据此回填 usage.tokens_* / duration_ms。

        并发安全：asyncio 场景下多个协程共享同一 adapter 实例，
        实现者应用 contextvars 存储元数据（协程隔离），不要用实例字段。
        """
        return result

    # -- 框架侧公共逻辑（协议装配 + 双层校验解析） ----------------------

    def run_task(
        self,
        task: Task,
        request_id: str,
        inputs: Optional[dict] = None,
    ) -> Result:
        """派发任务执行：模板 + task_request + 上游数据 → 解析 Result。"""
        request = build_task_request(
            request_id=request_id,
            task_id=task.id,
            task_desc=task.desc,
            inputs=inputs,
            output_schema=task.output_schema,
            constraints={
                "timeout_seconds": task.required_resources.timeout,
                "budget_usd": task.required_resources.budget,
                "side_effects": task.side_effects.value,
            },
        )
        messages = self._assemble(request, inputs)

        def retry(correction: str) -> str:
            msgs = messages + [
                {"role": "assistant", "content": last_text},
                {"role": "user", "content": correction},
            ]
            return self._call_llm(msgs)

        last_text = self._call_llm(messages)
        return self._post_process(parse_result(
            last_text,
            request_id=request_id,
            task_id=task.id,
            template_mode=self.template_mode,
            response_kind="run",
            retry_fn=retry,
        ))

    def run_info(
        self,
        scope: str,
        questions: list[str],
        request_id: str,
    ) -> Result:
        """信息请求（协议 §2 / §4.2）：收集 agent 能力 / 资源 / 约束。"""
        request = build_info_request(
            request_id=request_id,
            scope=scope,
            questions=questions,
        )
        messages = self._assemble(request)

        def retry(correction: str) -> str:
            msgs = messages + [
                {"role": "assistant", "content": last_text},
                {"role": "user", "content": correction},
            ]
            return self._call_llm(msgs)

        last_text = self._call_llm(messages)
        return self._post_process(parse_result(
            last_text,
            request_id=request_id,
            template_mode=self.template_mode,
            response_kind="info",
            retry_fn=retry,
        ))

    def cancel(self, task_id: str, request_id: str) -> Result:
        """取消请求（task_request 的 action=cancel 变体，best-effort）。"""
        request = build_task_request(
            request_id=request_id,
            task_id=task_id,
            action="cancel",
        )
        messages = self._assemble(request)
        text = self._call_llm(messages)
        return self._post_process(parse_result(
            text,
            request_id=request_id,
            task_id=task_id,
            template_mode=self.template_mode,
            response_kind="cancel",
        ))

    # -- 异步路径（阶段四 AsyncScheduler 使用） ---------------------------

    async def arun_task(
        self,
        task: Task,
        request_id: str,
        inputs: Optional[dict] = None,
    ) -> Result:
        """异步派发任务执行。逻辑与 run_task 完全一致，走 _acall_llm。"""
        request = build_task_request(
            request_id=request_id,
            task_id=task.id,
            task_desc=task.desc,
            inputs=inputs,
            output_schema=task.output_schema,
            constraints={
                "timeout_seconds": task.required_resources.timeout,
                "budget_usd": task.required_resources.budget,
                "side_effects": task.side_effects.value,
            },
        )
        messages = self._assemble(request, inputs)

        async def retry(correction: str) -> str:
            msgs = messages + [
                {"role": "assistant", "content": last_text},
                {"role": "user", "content": correction},
            ]
            return await self._acall_llm(msgs)

        last_text = await self._acall_llm(messages)
        return self._post_process(await aparse_result(
            last_text,
            request_id=request_id,
            task_id=task.id,
            template_mode=self.template_mode,
            response_kind="run",
            aretry_fn=retry,
        ))

    async def arun_info(
        self,
        scope: str,
        questions: list[str],
        request_id: str,
    ) -> Result:
        """异步信息请求（资源统计 / 能力收集）。"""
        request = build_info_request(
            request_id=request_id,
            scope=scope,
            questions=questions,
        )
        messages = self._assemble(request)

        async def retry(correction: str) -> str:
            msgs = messages + [
                {"role": "assistant", "content": last_text},
                {"role": "user", "content": correction},
            ]
            return await self._acall_llm(msgs)

        last_text = await self._acall_llm(messages)
        return self._post_process(await aparse_result(
            last_text,
            request_id=request_id,
            template_mode=self.template_mode,
            response_kind="info",
            aretry_fn=retry,
        ))

    async def acancel(self, task_id: str, request_id: str) -> Result:
        """异步取消请求（best-effort）。"""
        request = build_task_request(
            request_id=request_id,
            task_id=task_id,
            action="cancel",
        )
        messages = self._assemble(request)
        text = await self._acall_llm(messages)
        return self._post_process(await aparse_result(
            text,
            request_id=request_id,
            task_id=task_id,
            template_mode=self.template_mode,
            response_kind="cancel",
        ))

    # ------------------------------------------------------------------

    def _assemble(
        self,
        request: dict,
        inputs: Optional[dict] = None,
    ) -> list[dict]:
        """协议装配（§7.6）：模板 + === REQUEST === + === INPUT ===。"""
        user_content = render(request, template_mode=self.template_mode, inputs=inputs)
        return [
            {"role": "system", "content": "You are an agent that responds strictly "
                                          "according to the protocol in the user message."},
            {"role": "user", "content": user_content},
        ]
