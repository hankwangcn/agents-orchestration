"""进程内适配器测试：同进程 Python agent 免 HTTP（架构 §3.2 / 协议 §8）。

覆盖：str / dict 两种返回形态、解析重试（杂文→附正确示例）、
info_request 能力采集入库、AsyncScheduler 免 HTTP 全流程、异常传播。
"""
from __future__ import annotations

import json

import pytest

from orchestration.adapters.inprocess import InProcessAdapter
from orchestration.models import DAG, Task
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler


def extract_request(messages: list[dict]) -> dict:
    """从协议装配后的消息中提取 REQUEST JSON（测试辅助）。

    解析重试时基类会追加（assistant 原文 + user 修正提示），
    故遍历所有 user 消息找 === REQUEST === 标记。
    """
    for m in messages:
        if m["role"] == "user" and "=== REQUEST ===" in m["content"]:
            block = m["content"].split("=== REQUEST ===\n", 1)[1]
            if "\n=== INPUT ===" in block:
                block = block.split("\n=== INPUT ===", 1)[0]
            return json.loads(block.strip())
    raise AssertionError("消息中无 === REQUEST === 标记")


def run(sched: AsyncScheduler, dag: DAG, **kw):
    return __import__("asyncio").run(sched.run(dag, **kw))


# ---------------------------------------------------------------------------
# 基础：两种返回形态
# ---------------------------------------------------------------------------

def test_run_task_str_response():
    """fn 返回合法 JSON 字符串 → 走完整协议解析，request_id 回带。"""
    def agent(messages):
        req = extract_request(messages)
        assert req["type"] == "task_request"
        return json.dumps({
            "request_id": req["request_id"],
            "task_id": req["task_id"],
            "success": True,
            "output": {"echo": req["task_desc"]},
        }, ensure_ascii=False)

    adapter = InProcessAdapter(agent, model="local-agent")
    task = Task(id="t1", desc="翻译这段", required_resources={"model": "local-agent"})
    result = adapter.run_task(task, request_id="req-1")
    assert result.success
    assert result.request_id == "req-1"
    assert result.output == {"echo": "翻译这段"}


def test_run_task_dict_response():
    """fn 返回 dict → 自动 JSON 序列化后走解析层（便捷形态）。"""
    def agent(messages):
        req = extract_request(messages)
        return {"request_id": req["request_id"], "task_id": req["task_id"],
                "success": True, "output": {"n": 42}}

    adapter = InProcessAdapter(agent, model="local-agent")
    result = adapter.run_task(Task(id="t1", desc="x"), request_id="req-9")
    assert result.success
    assert result.output == {"n": 42}


# ---------------------------------------------------------------------------
# 解析层兜底（与 HTTP agent 完全一致）
# ---------------------------------------------------------------------------

def test_nonsense_then_retry():
    """杂文输出 → 基类解析重试（附正确示例）→ 第二次成功，retries=1。"""
    class Flaky:
        def __init__(self):
            self.calls = 0

        def __call__(self, messages):
            self.calls += 1
            if self.calls == 1:
                return "好的，我这就开始处理！"  # 杂文 → 触发解析重试
            req = extract_request(messages)
            return {"request_id": req["request_id"], "task_id": req["task_id"],
                    "success": True, "output": {"attempt": self.calls}}

    flaky = Flaky()
    adapter = InProcessAdapter(flaky, model="local-agent")
    result = adapter.run_task(Task(id="t1", desc="x"), request_id="req-3")
    assert result.success
    assert flaky.calls == 2            # 解析重试真实发生了（附正确示例再调一次）
    assert result.output == {"attempt": 2}
    # 解析重试与任务执行重试分离计数（§7.3）：Result.retries 是执行重试，
    # 由调度器填——单次 run_task 路径下保持 0，不混入解析重试
    assert result.retries == 0


def test_fn_exception_propagates():
    """fn 抛异常 → 向上传播（由调度器/调用方按执行失败处理）。"""
    def agent(messages):
        raise RuntimeError("本地 agent 崩了")

    adapter = InProcessAdapter(agent, model="local-agent")
    with pytest.raises(RuntimeError):
        adapter.run_task(Task(id="t1", desc="x"), request_id="req-4")


# ---------------------------------------------------------------------------
# 注册表集成：能力采集 + 并发调度全流程（免 HTTP）
# ---------------------------------------------------------------------------

def make_info_agent(model: str):
    def agent(messages):
        req = extract_request(messages)
        if req.get("type") == "info_request":
            scope = req["scope"]
            outputs = {
                "capability": {"q1": "code_review, data_analysis",
                               "q2": "擅长代码审查与数据分析"},
                "resource": {"q1": "4", "q2": "60", "q3": "0.5"},
                "constraint": {"q1": "无", "q2": "python, 中文"},
            }
            return {"request_id": req["request_id"], "task_id": "",
                    "success": True, "output": outputs[scope]}
        return {"request_id": req["request_id"], "task_id": req["task_id"],
                "success": True, "output": {"done": model}}

    return InProcessAdapter(agent, model=model)


def test_registry_collect_inprocess():
    """进程内 agent 照常被 info_request 采集——能力是问出来的。"""
    reg = AgentRegistry()
    reg.register(make_info_agent("local-a"), agent_id="a")
    reg.register(make_info_agent("local-b"), agent_id="b")
    summary = reg.collect()
    assert len(summary) == 6  # 2 agents × 3 scopes
    assert all(s["ok"] for s in summary)
    agent = reg.get("a")
    assert "code_review" in agent.capabilities
    assert agent.max_concurrency == 4
    assert agent.budget_limit_usd == 0.5


def test_async_scheduler_full_flow_inprocess():
    """AsyncScheduler + 进程内 agent：DAG 全流程免 HTTP 跑通。"""
    reg = AgentRegistry()
    reg.register(make_info_agent("local-a"), agent_id="a")
    reg.register(make_info_agent("local-b"), agent_id="b")
    sched = AsyncScheduler(reg)
    dag = DAG(tasks={
        "t1": Task(id="t1", desc="t1", required_resources={"model": "local-a"}),
        "t2": Task(id="t2", desc="t2", deps=["t1"],
                   required_resources={"model": "local-b"}),
    })
    report = run(sched, dag)
    assert report.final_status == "success"
    assert report.results["t1"].output == {"done": "local-a"}
    assert report.results["t2"].output == {"done": "local-b"}
    assert report.total_cost == 0.0
