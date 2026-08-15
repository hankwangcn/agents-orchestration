"""DeepSeek 适配器测试（mock API 响应，不真实调用）。

覆盖：真实元数据回填（usage/token/耗时）——冒烟实测发现真实 LLM
不自报 usage，由 adapter 从 API 响应捕获（协议 §4.2 兜底）。
"""
import json
from types import SimpleNamespace

import pytest

from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.models import Task


def _resp(content: str, prompt_tokens: int = 0, completion_tokens: int = 0):
    """构造 OpenAI 兼容响应对象。"""
    msg = SimpleNamespace(content=content)
    choice = SimpleNamespace(message=msg)
    usage = SimpleNamespace(
        prompt_tokens=prompt_tokens, completion_tokens=completion_tokens
    )
    return SimpleNamespace(choices=[choice], usage=usage)


def _result_json(request_id: str, task_id: str = "t1"):
    return json.dumps({
        "request_id": request_id,
        "task_id": task_id,
        "success": True,
        "output": {"explanation": "ok"},
        "usage": {"tokens_in": 0, "tokens_out": 0, "cost": 0},
    }, ensure_ascii=False)


def _make_adapter(monkeypatch, content: str, pin: int = 0, pout: int = 0):
    a = DeepSeekAdapter(model="deepseek-chat", api_key="test-key")
    monkeypatch.setattr(
        a._client.chat.completions, "create",
        lambda **kw: _resp(content, prompt_tokens=pin, completion_tokens=pout),
    )
    return a


def test_usage_backfilled_from_api(monkeypatch):
    """agent 自报 0 → 真实 API usage 回填 + 按单价算成本。"""
    a = _make_adapter(monkeypatch, _result_json("req-1"),
                      pin=1000, pout=200)
    task = Task(id="t1", desc="测试任务")
    result = a.run_task(task, request_id="req-1")
    assert result.success
    assert result.usage.tokens_in == 1000
    assert result.usage.tokens_out == 200
    # deepseek-chat 单价：$0.27/1M in + $1.10/1M out
    assert result.usage.cost == pytest.approx(
        1000 / 1e6 * 0.27 + 200 / 1e6 * 1.10, abs=1e-6
    )


def test_duration_backfilled(monkeypatch):
    """duration_ms 为 0 时用真实耗时回填。"""
    a = _make_adapter(monkeypatch, _result_json("req-2"),
                      pin=10, pout=5)
    task = Task(id="t1", desc="测试任务")
    result = a.run_task(task, request_id="req-2")
    assert result.duration_ms > 0


def test_agent_reported_usage_overwritten_by_api(monkeypatch):
    """agent 自报了非零 usage 也以真实 API 值为准（自报不可信）。"""
    body = json.dumps({
        "request_id": "req-3",
        "task_id": "t1",
        "success": True,
        "output": {"explanation": "ok"},
        "usage": {"tokens_in": 999, "tokens_out": 999, "cost": 9.9},
    }, ensure_ascii=False)
    a = _make_adapter(monkeypatch, body, pin=50, pout=10)
    result = a.run_task(Task(id="t1", desc="x"), request_id="req-3")
    assert result.usage.tokens_in == 50
    assert result.usage.tokens_out == 10
    assert result.usage.cost == pytest.approx(
        50 / 1e6 * 0.27 + 10 / 1e6 * 1.10, abs=1e-6
    )


def test_missing_key_raises(monkeypatch):
    """无 key 且环境变量缺失 → 明确报错。"""
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        DeepSeekAdapter(api_key=None)


def test_api_error_propagates(monkeypatch):
    """网络/API 异常向上抛（由调度器重试层处理）。"""
    a = DeepSeekAdapter(model="deepseek-chat", api_key="test-key")

    def boom(**kw):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(a._client.chat.completions, "create", boom)
    with pytest.raises(RuntimeError):
        a.run_task(Task(id="t1", desc="x"), request_id="req-4")
