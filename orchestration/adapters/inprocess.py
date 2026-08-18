"""进程内适配器：同进程 Python agent 免 HTTP 接入（架构 §3.2 / 协议 §8）。

场景：agent 就是本项目进程里的一个 Python 函数/可调用对象——
再为它起 HTTP 服务是纯开销（序列化 + 网络往返只为调一个本地函数）。
InProcessAdapter 把"调用"直接接到本地函数上，协议装配 / 双层校验解析 /
解析重试 / 成本回填全部复用 AgentAdapter 基类，传输层零成本。

与 HTTP adapter（DeepSeekAdapter）完全对等：
- 注册进 AgentRegistry 后，info_request 能力采集、三级分配、
  per-agent 并发上限、连续失败摘除…… 全部照常工作；
- 唯一的区别是没有网络往返——协议约束（输出合法 JSON）依然由
  基类解析层强制，稳定性兜底一个不少。

两种返回形态（fn 二选一）：
- 返回 str  —— 按协议原样进入解析层（推荐：与真实 agent 行为一致，
  能暴露格式漂移，冒烟/测试更逼真）；
- 返回 dict —— 自动 JSON 序列化后再进解析层（便捷形态）。

异步：同步 fn 经基类默认 _acall_llm 丢线程池，AsyncScheduler 可直接使用。
"""
from __future__ import annotations

import json
from typing import Callable, Union

from .base import AgentAdapter

# fn 接收协议装配后的消息列表，返回原始响应文本或 dict（自动序列化）
AgentFn = Callable[[list[dict]], Union[str, dict]]


class InProcessAdapter(AgentAdapter):
    """对接同进程 Python 函数（免 HTTP 传输）。

    fn 签名：fn(messages) -> str | dict
    - messages：协议装配后的消息列表（system + user，含模板/请求 JSON/输入）
    - 返回 str 原样进解析层；返回 dict 自动转 JSON 文本（便捷形态）
    """

    def __init__(
        self,
        fn: AgentFn,
        model: str = "inprocess",
        template_mode: str = "full",
    ):
        super().__init__(model=model, template_mode=template_mode)
        self._fn = fn

    def _call_llm(self, messages: list[dict]) -> str:
        """直接调用本地函数——协议之外的唯一实现点（基类承担其余全部）。"""
        out = self._fn(messages)
        if isinstance(out, str):
            return out
        return json.dumps(out, ensure_ascii=False)
