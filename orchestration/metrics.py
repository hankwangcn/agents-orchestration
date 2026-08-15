"""可观测性（阶段四）：轻量指标收集 + 结构化日志。

设计原则：不引外部依赖（无 Prometheus/structlog）——指标是内存聚合的
dataclass，直接喂审计器与学习引擎；日志是标准 logging + key=value 结构化
formatter。数据面保持轻量纯净，观测面留给标准库。

MetricsCollector 是 asyncio 单线程安全的（事件循环内调用），
不涉及锁；若被多线程使用需自行加锁（当前无此场景）。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# 结构化日志
# ---------------------------------------------------------------------------

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


class KeyValueFormatter(logging.Formatter):
    """把 record.extra 字段格式化为 key=value，供日志采集/排查。"""

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _RESERVED and k not in ("message", "asctime", "levelname", "name")
        }
        if extras:
            kv = " ".join(f"{k}={v}" for k, v in extras.items())
            return f"{base} | {kv}"
        return base


def configure_logging(level: int = logging.INFO, fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s") -> None:
    """配置根 logger 为结构化输出（API 网关/CLI 入口调用）。"""
    handler = logging.StreamHandler()
    handler.setFormatter(KeyValueFormatter(fmt))
    root = logging.getLogger("orchestration")
    root.setLevel(level)
    if not root.handlers:
        root.addHandler(handler)


# ---------------------------------------------------------------------------
# 指标收集
# ---------------------------------------------------------------------------

@dataclass
class AgentMetrics:
    """单个 agent 的运行指标（累计量）。"""
    tasks: int = 0
    success: int = 0
    failed: int = 0
    cancelled: int = 0
    degraded: int = 0
    total_duration_ms: int = 0
    total_cost: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0
    peak_concurrency: int = 0

    @property
    def avg_duration_ms(self) -> float:
        return round(self.total_duration_ms / self.tasks, 1) if self.tasks else 0.0

    @property
    def success_rate(self) -> float:
        return round(self.success / self.tasks, 3) if self.tasks else 0.0


@dataclass
class RunMetrics:
    """一次 DAG 调度的运行指标。"""
    run_id: str
    started_at: float = 0.0
    finished_at: float = 0.0
    tasks_total: int = 0
    tasks_success: int = 0
    tasks_failed: int = 0
    tasks_cancelled: int = 0
    tasks_skipped: int = 0
    pruned_count: int = 0
    total_cost: float = 0.0
    final_status: str = ""
    agents: dict[str, AgentMetrics] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        if not self.started_at:
            return 0
        end = self.finished_at or time.time()
        return int((end - self.started_at) * 1000)


class MetricsCollector:
    """内存指标聚合器。run 生命周期由 AsyncScheduler 驱动。

    所有事件方法按 run_id 定位，支持同一实例并发运行多个 run（RunManager 场景）。
    """

    def __init__(self):
        self.runs: dict[str, RunMetrics] = {}

    # ---------- run 生命周期 ----------

    def begin_run(self, run_id: str, dag_size: int) -> None:
        m = RunMetrics(run_id=run_id, started_at=time.time())
        m.tasks_total = dag_size
        self.runs[run_id] = m

    def finish_run(self, run_id: str, final_status: str) -> None:
        m = self.runs.get(run_id)
        if m is None:
            return
        m.finished_at = time.time()
        m.final_status = final_status

    # ---------- 事件记录 ----------

    def _agent(self, run_id: str, agent_id: str) -> AgentMetrics:
        m = self.runs.get(run_id)
        if m is None:
            raise KeyError(f"未 begin_run：{run_id}")
        if agent_id not in m.agents:
            m.agents[agent_id] = AgentMetrics()
        return m.agents[agent_id]

    def task_launched(self, run_id: str, agent_id: str, match_type: str) -> None:
        am = self._agent(run_id, agent_id)
        am.tasks += 1
        if match_type == "degraded":
            am.degraded += 1

    def task_concurrency(self, run_id: str, agent_id: str, current: int) -> None:
        """记录 agent 当前并发水位（用于峰值统计）。"""
        am = self._agent(run_id, agent_id)
        if current > am.peak_concurrency:
            am.peak_concurrency = current

    def task_done(
        self,
        run_id: str,
        agent_id: str,
        *,
        success: bool,
        duration_ms: int = 0,
        cost: float = 0.0,
        tokens_in: int = 0,
        tokens_out: int = 0,
    ) -> None:
        am = self._agent(run_id, agent_id)
        if success:
            am.success += 1
        else:
            am.failed += 1
        am.total_duration_ms += duration_ms
        am.total_cost += cost
        am.tokens_in += tokens_in
        am.tokens_out += tokens_out
        m = self.runs.get(run_id)
        if m is not None:
            if success:
                m.tasks_success += 1
            else:
                m.tasks_failed += 1

    def task_cancelled(self, run_id: str, agent_id: str) -> None:
        am = self._agent(run_id, agent_id)
        am.cancelled += 1
        m = self.runs.get(run_id)
        if m is not None:
            m.tasks_cancelled += 1

    def pruned(self, run_id: str, count: int) -> None:
        m = self.runs.get(run_id)
        if m is not None:
            m.pruned_count += count

    def skipped(self, run_id: str, count: int) -> None:
        m = self.runs.get(run_id)
        if m is not None:
            m.tasks_skipped += count

    # ---------- 查询 ----------

    def run(self, run_id: str) -> Optional[RunMetrics]:
        return self.runs.get(run_id)


# ---------------------------------------------------------------------------
# 日志便捷函数（结构化 extra）
# ---------------------------------------------------------------------------

log = logging.getLogger("orchestration.scheduler")


def log_event(event: str, **fields: object) -> None:
    """发射一条结构化事件日志。"""
    log.info(event, extra=fields)
