"""阶段四：异步并发调度器测试。

覆盖：并发派发（时间重叠）、并发上限（per-agent semaphore）、速率限制、
失败剪枝竞态（冻结→逐级取消→收尾→晚到结果丢弃）、独立分支存活、
外部取消（cancel_event）、多 run 并发状态隔离、metrics 记录。
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from orchestration.adapters.base import AgentAdapter
from orchestration.metrics import MetricsCollector
from orchestration.models import (
    DAG,
    ErrorInfo,
    ResourceRequirement,
    Result,
    Task,
    TaskStatus,
)
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler
from helpers import AsyncScriptedAdapter, ConcurrencyProbeAdapter, ok, fail, dag_of


# ---------------------------------------------------------------------------
# 异步测试适配器
# ---------------------------------------------------------------------------

def run(sched: AsyncScheduler, dag: DAG, **kw):
    """同步测试里跑异步调度。"""
    return asyncio.run(sched.run(dag, **kw))


def make_scheduler(*adapters, retries=2, backoff_base=0.0, metrics=None):
    reg = AgentRegistry()
    for a in adapters:
        reg.register(a)
    return AsyncScheduler(registry=reg, retries=retries,
                          backoff_base=backoff_base, metrics=metrics), reg


# ---------------------------------------------------------------------------
# 并发派发
# ---------------------------------------------------------------------------

class TestConcurrentDispatch:
    def test_independent_tasks_run_parallel(self):
        """两个独立任务同轮派发，时间重叠（并发执行）。

        注意：注册表默认 max_concurrency=1（未采集时保守串行）——
        显式声明 2 才允许并行，这正是资源限制生效的证明。
        """
        dag = dag_of(("a", []), ("b", []))
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]}, delay=0.1)
        sched, reg = make_scheduler(adapter)
        reg.get("agent_001").max_concurrency = 2
        t0 = time.monotonic()
        report = run(sched, dag)
        elapsed = time.monotonic() - t0

        assert report.final_status == "success"
        # 两个 0.1s 任务并行 → 总耗时 < 0.2（串行会 >= 0.2）
        assert elapsed < 0.19, f"并发未生效：elapsed={elapsed}"

    def test_parallel_then_merge_topology(self):
        """a → {b, c} → d：b/c 并行，d 等两者都成功。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a", {"v": 1})],
            "b": [ok("b")],
            "c": [ok("c")],
            "d": [ok("d")],
        }, delay=0.05)
        sched, _ = make_scheduler(adapter)
        report = run(sched, dag)

        assert report.final_status == "success"
        d_call = [c for c in adapter.calls if c[0] == "d"][0]
        assert d_call[2]["b"] == {} and d_call[2]["c"] == {}
        # d 的派发晚于 b、c 完成
        d_t = d_call[3]
        assert all(c[3] < d_t for c in adapter.calls if c[0] in ("b", "c"))


class TestConcurrencyLimit:
    def test_max_concurrency_1_serializes(self):
        """并发上限 1：两个任务串行执行，峰值并发 = 1。"""
        dag = dag_of(("a", []), ("b", []))
        probe = ConcurrencyProbeAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, reg = make_scheduler(probe)
        reg.get("agent_001").max_concurrency = 1  # 默认即 1，显式声明
        run(sched, dag)

        assert probe.peak == 1

    def test_max_concurrency_2_allows_pair(self):
        """并发上限 2：两个任务并行，峰值并发 = 2。"""
        dag = dag_of(("a", []), ("b", []))
        probe = ConcurrencyProbeAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, reg = make_scheduler(probe)
        reg.get("agent_001").max_concurrency = 2
        run(sched, dag)

        assert probe.peak == 2

    def test_concurrency_split_across_agents(self):
        """两个 agent 各自限 1：各跑各的，互不阻塞。

        b 任务显式指定 claude-3.5（否则默认 deepseek-chat 会全部精确匹配 agent_001）。
        """
        dag = DAG(tasks={
            "a": Task(id="a", desc="a",
                      required_resources=ResourceRequirement(model="deepseek-chat")),
            "b": Task(id="b", desc="b",
                      required_resources=ResourceRequirement(model="claude-3.5")),
        })
        p1 = ConcurrencyProbeAdapter(
            {"a": [ok("a")]}, model="deepseek-chat")
        p2 = ConcurrencyProbeAdapter(
            {"b": [ok("b")]}, model="claude-3.5")
        sched, reg = make_scheduler(p1, p2)
        reg.get("agent_001").max_concurrency = 1
        reg.get("agent_002").max_concurrency = 1
        run(sched, dag)

        assert p1.peak == 1 and p2.peak == 1


class TestRateLimit:
    def test_rate_limit_throttles(self):
        """速率上限 1/窗口：第 2 个任务被限速等待窗口重置。"""
        dag = dag_of(("a", []), ("b", []))
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, reg = make_scheduler(adapter)
        reg.get("agent_001").rate_limit_per_min = 1
        sched._rate_window = 0.15  # 测试用短窗口

        t0 = time.monotonic()
        run(sched, dag)
        elapsed = time.monotonic() - t0

        # 第 2 个调用至少等到窗口重置（~0.15s）
        assert elapsed >= 0.14, f"限速未生效：elapsed={elapsed}"
        # 两次调用间隔 >= 窗口
        ts = sorted(c[3] for c in adapter.calls)
        assert ts[1] - ts[0] >= 0.14

    def test_no_limit_when_zero(self):
        """rate_limit=0（未知）→ 不限速。"""
        dag = dag_of(("a", []), ("b", []))
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]})
        sched, _ = make_scheduler(adapter)
        t0 = time.monotonic()
        run(sched, dag)
        assert time.monotonic() - t0 < 0.1


# ---------------------------------------------------------------------------
# 失败与竞态
# ---------------------------------------------------------------------------

class TestFailureRace:
    def test_retry_then_success_async(self):
        dag = dag_of(("a", []))
        adapter = AsyncScriptedAdapter({"a": [fail("a"), ok("a")]})
        sched, _ = make_scheduler(adapter, retries=2)
        report = run(sched, dag)

        assert report.final_status == "success"
        assert dag.tasks["a"].result.retries == 1
        assert len(adapter.calls) == 2

    def test_prune_cancels_running_and_drops_late_result(self):
        """竞态核心：b 快速失败，c 仍在运行 → 剪枝置 CANCELLED、
        下发取消、等待收尾、晚到结果直接丢弃（不进 results、不计成本）。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c")],   # 慢：b 失败剪枝时仍在运行
            "d": [ok("d")],
        })
        adapter.script_delay = {"c": 0.2}  # b 三连失败期间 c 仍在跑

        sched, _ = make_scheduler(adapter, retries=2)
        report = run(sched, dag)

        # c 收到取消信号（best-effort）
        assert "c" in adapter.cancelled
        # c 被剪枝置 CANCELLED
        assert dag.tasks["c"].status == TaskStatus.CANCELLED
        # 晚到结果被丢弃：results 里没有 c
        assert "c" not in report.results
        # c 的产出成本不计入（丢弃）；只有 a 的 0.01
        assert report.total_cost == 0.01
        assert report.final_status == "failed"

    def test_prune_waits_for_inflight_finish(self):
        """剪枝后等待 in-flight 收尾：调度返回时无残留 running 任务。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c")],
            "d": [ok("d")],
        })
        adapter.script_delay = {"c": 0.2}

        sched, _ = make_scheduler(adapter, retries=2)
        run(sched, dag)

        # 全部终态，无 running 残留
        assert dag.all_terminal()
        assert all(t.status != TaskStatus.RUNNING for t in dag.tasks.values())

    def test_independent_branch_survives_failure(self):
        """双分支：b 分支失败剪枝，c→e 独立分支照常完成 → partial。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b"]), ("e", ["c"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c", {"v": 1})],
            "e": [ok("e")],
        })
        sched, _ = make_scheduler(adapter, retries=2)
        report = run(sched, dag)

        assert report.final_status == "partial"
        assert dag.tasks["c"].status == TaskStatus.SUCCESS
        assert dag.tasks["e"].status == TaskStatus.SUCCESS
        assert dag.tasks["d"].status == TaskStatus.CANCELLED

    def test_adapter_exception_counts_as_failure(self):
        dag = dag_of(("a", []))

        class FlakyAsyncAdapter(AsyncScriptedAdapter):
            def __init__(self):
                super().__init__({})
                self._n = 0

            async def arun_task(self, task, request_id, inputs=None):
                self._n += 1
                if self._n == 1:
                    raise ConnectionError("network down")
                return Result(task_id=task.id, success=True, output={})

        adapter = FlakyAsyncAdapter()
        sched, _ = make_scheduler(adapter, retries=1)
        report = run(sched, dag)

        assert report.final_status == "success"
        assert dag.tasks["a"].result.retries == 1


# ---------------------------------------------------------------------------
# 外部取消 / 多 run
# ---------------------------------------------------------------------------

class TestExternalControl:
    def test_cancel_event_aborts_whole_dag(self):
        """外部取消：cancel_event 置位 → 整棵取消，final_status=cancelled。"""
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a")],
            "b": [ok("b")],
            "c": [ok("c")],
        }, delay=0.05)
        sched, _ = make_scheduler(adapter)
        ev = asyncio.Event()

        async def _run_then_cancel():
            t = asyncio.create_task(sched.run(dag, run_id="r1", cancel_event=ev))
            await asyncio.sleep(0.02)  # a 可能已完成，b/c 在跑
            ev.set()
            return await t

        report = asyncio.run(_run_then_cancel())

        assert report.final_status == "cancelled"
        # a 可能在 cancel 生效前已完成（SUCCESS 不回收）；未完成的全部取消
        assert dag.tasks["a"].status in (
            TaskStatus.SUCCESS, TaskStatus.CANCELLED,
        )
        assert dag.tasks["b"].status == TaskStatus.CANCELLED
        assert dag.tasks["c"].status == TaskStatus.CANCELLED

    def test_concurrent_runs_isolated(self):
        """同一 scheduler 实例并发跑两个 DAG：状态互不串扰（_RunCtx 隔离）。"""
        dag1 = dag_of(("x", []), ("y", ["x"]))
        dag2 = DAG(tasks={
            "p": Task(id="p", desc="p",
                      required_resources=ResourceRequirement(model="claude-3.5")),
            "q": Task(id="q", desc="q", deps=["p"],
                      required_resources=ResourceRequirement(model="claude-3.5")),
        })
        a1 = AsyncScriptedAdapter({"x": [ok("x")], "y": [ok("y")]}, model="deepseek-chat")
        a2 = AsyncScriptedAdapter({"p": [ok("p")], "q": [ok("q")]}, model="claude-3.5")
        sched, _ = make_scheduler(a1, a2)

        async def _both():
            r1, r2 = await asyncio.gather(
                sched.run(dag1, run_id="run1"),
                sched.run(dag2, run_id="run2"),
            )
            return r1, r2

        r1, r2 = asyncio.run(_both())

        assert r1.final_status == "success" and r2.final_status == "success"
        # 各自的结果归属各自 run
        assert set(r1.results) == {"x", "y"}
        assert set(r2.results) == {"p", "q"}
        # 分配不串：run1 全走 agent_001，run2 全走 agent_002
        assert {a.agent_id for a in r1.assignments} == {"agent_001"}
        assert {a.agent_id for a in r2.assignments} == {"agent_002"}


# ---------------------------------------------------------------------------
# 可观测性
# ---------------------------------------------------------------------------

class TestObservability:
    def test_metrics_recorded(self):
        dag = dag_of(("a", []), ("b", ["a"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a", cost=0.02)],
            "b": [ok("b", cost=0.03)],
        }, delay=0.02)
        metrics = MetricsCollector()
        sched, _ = make_scheduler(adapter, metrics=metrics)
        run(sched, dag, run_id="run_m")

        m = metrics.run("run_m")
        assert m is not None
        assert m.tasks_total == 2
        assert m.tasks_success == 2
        assert m.final_status == "success"
        assert m.duration_ms > 0
        am = m.agents["agent_001"]
        assert am.tasks == 2
        assert am.success == 2
        assert am.success_rate == 1.0
        assert abs(am.total_cost - 0.05) < 1e-9

    def test_metrics_peak_concurrency_tracked(self):
        """并行任务：峰值并发 > 1（进入执行区即统计，瞬时任务不漏）。"""
        dag = dag_of(("a", []), ("b", []))
        adapter = AsyncScriptedAdapter({"a": [ok("a")], "b": [ok("b")]}, delay=0.1)
        metrics = MetricsCollector()
        sched, reg = make_scheduler(adapter, metrics=metrics)
        reg.get("agent_001").max_concurrency = 2
        run(sched, dag, run_id="run_c")

        am = metrics.run("run_c").agents["agent_001"]
        assert am.peak_concurrency == 2

    def test_metrics_failure_and_prune(self):
        dag = dag_of(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
        adapter = AsyncScriptedAdapter({
            "a": [ok("a")],
            "b": [fail("b"), fail("b"), fail("b")],
            "c": [ok("c")],
            "d": [ok("d")],
        })
        metrics = MetricsCollector()
        sched, _ = make_scheduler(adapter, retries=2, metrics=metrics)
        run(sched, dag, run_id="run_f")

        m = metrics.run("run_f")
        assert m.tasks_failed == 1
        assert m.tasks_success == 1
        assert m.pruned_count >= 1
        assert m.final_status == "failed"
