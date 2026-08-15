"""可观测性测试（阶段四）：MetricsCollector 统计 + 结构化日志。"""
from __future__ import annotations

import logging

from orchestration.metrics import (
    KeyValueFormatter,
    MetricsCollector,
    configure_logging,
)


class TestMetricsCollector:
    def test_begin_finish_lifecycle(self):
        m = MetricsCollector()
        m.begin_run("r1", dag_size=3)
        m.finish_run("r1", "success")

        run = m.run("r1")
        assert run.tasks_total == 3
        assert run.final_status == "success"
        assert run.duration_ms >= 0
        assert m.run("nope") is None

    def test_task_done_aggregation(self):
        m = MetricsCollector()
        m.begin_run("r1", 2)
        m.task_launched("r1", "agent_001", "exact")
        m.task_launched("r1", "agent_001", "exact")
        m.task_done("r1", "agent_001", success=True, duration_ms=100,
                    cost=0.02, tokens_in=10, tokens_out=20)
        m.task_done("r1", "agent_001", success=False, duration_ms=50,
                    cost=0.01, tokens_in=5, tokens_out=5)
        m.finish_run("r1", "partial")

        run = m.run("r1")
        assert run.tasks_success == 1
        assert run.tasks_failed == 1
        am = run.agents["agent_001"]
        assert am.tasks == 2
        assert am.success == 1
        assert am.failed == 1
        assert am.success_rate == 0.5
        assert am.total_duration_ms == 150
        assert am.avg_duration_ms == 75.0
        assert abs(am.total_cost - 0.03) < 1e-9
        assert am.tokens_in == 15 and am.tokens_out == 25

    def test_degraded_and_concurrency_tracked(self):
        m = MetricsCollector()
        m.begin_run("r1", 3)
        m.task_launched("r1", "agent_001", "degraded")
        m.task_concurrency("r1", "agent_001", 1)
        m.task_concurrency("r1", "agent_001", 3)
        m.task_concurrency("r1", "agent_001", 2)
        m.finish_run("r1", "success")

        am = m.run("r1").agents["agent_001"]
        assert am.degraded == 1
        assert am.peak_concurrency == 3

    def test_prune_and_skipped(self):
        m = MetricsCollector()
        m.begin_run("r1", 5)
        m.pruned("r1", 3)
        m.skipped("r1", 1)
        m.finish_run("r1", "failed")

        run = m.run("r1")
        assert run.pruned_count == 3
        assert run.tasks_skipped == 1

    def test_concurrent_runs_isolated(self):
        """两个 run 的指标互不串扰（按 run_id 隔离）。"""
        m = MetricsCollector()
        m.begin_run("r1", 1)
        m.begin_run("r2", 1)
        m.task_launched("r1", "agent_001", "exact")
        m.task_launched("r2", "agent_002", "exact")
        m.task_done("r1", "agent_001", success=True)
        m.task_done("r2", "agent_002", success=True)
        m.finish_run("r1", "success")
        m.finish_run("r2", "success")

        assert set(m.run("r1").agents) == {"agent_001"}
        assert set(m.run("r2").agents) == {"agent_002"}
        assert m.run("r1").tasks_success == 1
        assert m.run("r2").tasks_success == 1


class TestStructuredLogging:
    def test_keyvalue_formatter(self):
        record = logging.LogRecord(
            name="orchestration.scheduler", level=logging.INFO,
            pathname=__file__, lineno=1, msg="task_launched",
            args=(), exc_info=None,
        )
        record.__dict__["run_id"] = "r1"
        record.__dict__["task_id"] = "a"
        f = KeyValueFormatter("%(levelname)s %(message)s")
        out = f.format(record)
        assert "task_launched" in out
        assert "run_id=r1" in out
        assert "task_id=a" in out

    def test_configure_logging_idempotent(self):
        configure_logging()
        configure_logging()  # 不重复加 handler
        root = logging.getLogger("orchestration")
        assert len(root.handlers) == 1
