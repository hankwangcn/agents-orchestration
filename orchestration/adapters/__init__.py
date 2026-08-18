"""Agent 适配器：协议内容与传输解耦（架构 §3.2 / 协议 §8）。

- deepseek：OpenAI 兼容 HTTP 端点（默认接入形态，配置即接入零代码）
- inprocess：同进程 Python 函数（免 HTTP，传输零成本）
实现者只需提供 _call_llm——协议装配 / 双层校验解析 / 重试 / 成本回填
全部由 AgentAdapter 基类统一提供。
"""
from .base import AgentAdapter
from .deepseek import DeepSeekAdapter
from .inprocess import InProcessAdapter

__all__ = ["AgentAdapter", "DeepSeekAdapter", "InProcessAdapter"]
