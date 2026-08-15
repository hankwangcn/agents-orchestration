"""断点持久化（StateStore）：调度状态的 SQLite 落盘与恢复。

设计（对话共识）：
- **结果导向与断点不冲突**：断点只存框架侧的调度状态（DAG 快照 + 每个任务
  status/result/assignment），不碰 agent 内部状态——"只管结果"哲学不变。
- **事件驱动写入**：任务状态变更（启动/完成/失败/取消/剪枝）即写，非定期
  快照——崩溃点数据最新，丢失窗口≈0。
- **恢复策略（A+B）**：
  - RUNNING + 无副作用 → 重置 PENDING **重派**（纯产出，重复执行可接受，
    最坏损失一次成本；恢复后任务 attempt 语义由调度器重试计数表达）
  - RUNNING + 声明副作用 → 置 INTERRUPTED **不自动重派**（重派 = 副作用
    可能执行两次，不可逆），等人工 resolve（complete / cancel / retry）
  - 已终态任务 → 保留（结果/成本/审计记录直接复用，不重跑不重计费）

存储格式：整 DAG JSON（任务数小，整存简单可靠）+ assignments 表 +
prune_reports（随 run 行存）。SQLite 单文件、零依赖；可换 Postgres
（StateStore 抽象，实现同签名即可）。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from .models import Assignment, DAG, PruneReport, ScheduleReport

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    dag_json    TEXT NOT NULL,
    run_status  TEXT NOT NULL DEFAULT 'running',
    prune_json  TEXT NOT NULL DEFAULT '[]',
    report_json TEXT,
    updated_at  TEXT
);
CREATE TABLE IF NOT EXISTS assignments (
    run_id    TEXT NOT NULL,
    task_id   TEXT NOT NULL,
    agent_id  TEXT NOT NULL,
    match_type TEXT NOT NULL,
    reason    TEXT DEFAULT '',
    risk      INTEGER DEFAULT 0,
    PRIMARY KEY (run_id, task_id)
);
"""


class StateStore(ABC):
    """调度状态持久化抽象（SQLite 实现；可换 Postgres 等）。"""

    @abstractmethod
    def save_run(
        self,
        run_id: str,
        dag: DAG,
        run_status: Optional[str] = None,
        assignments: Optional[list[Assignment]] = None,
        prune_reports: Optional[list[PruneReport]] = None,
    ) -> None: ...

    @abstractmethod
    def save_report(self, run_id: str, report: ScheduleReport) -> None: ...

    @abstractmethod
    def load_run(self, run_id: str) -> dict:
        """返回 {dag, assignments: {tid: Assignment}, prune_reports}。"""

    @abstractmethod
    def has_run(self, run_id: str) -> bool: ...

    @abstractmethod
    def active_runs(self) -> list[str]: ...

    @abstractmethod
    def delete_run(self, run_id: str) -> None: ...


class SqliteStateStore(StateStore):
    """SQLite 实现。path=':memory:' 可用于测试。"""

    def __init__(self, path: str = "orchestration_state.db"):
        self._path = path
        # check_same_thread=False：网关 TestClient/多线程 HTTP 访问场景，
        # 连接可跨线程；操作串行性由 _lock 保证（SQLite 单写者）。
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ------------------------------------------------------------------

    def save_run(
        self,
        run_id: str,
        dag: DAG,
        run_status: Optional[str] = None,
        assignments: Optional[list[Assignment]] = None,
        prune_reports: Optional[list[PruneReport]] = None,
    ) -> None:
        """事件驱动整存：DAG（含每任务 status/result）+ 可选 assignment/prune。"""
        now = _ts()
        with self._lock:
            if run_status is None:
                # 保留已有 run_status（resolve 等局部变更不覆盖调度状态）
                row = self._conn.execute(
                    "SELECT run_status FROM runs WHERE run_id=?", (run_id,)
                ).fetchone()
                run_status = row["run_status"] if row else "running"
            self._conn.execute(
                """
                INSERT INTO runs (run_id, dag_json, run_status, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    dag_json=excluded.dag_json,
                    run_status=excluded.run_status,
                    updated_at=excluded.updated_at
                """,
                (run_id, dag.model_dump_json(), run_status, now),
            )
            if assignments is not None:
                self._conn.execute(
                    "DELETE FROM assignments WHERE run_id=?", (run_id,)
                )
                self._conn.executemany(
                    """
                    INSERT INTO assignments
                        (run_id, task_id, agent_id, match_type, reason, risk)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            run_id, a.task_id, a.agent_id, a.match_type,
                            a.reason, int(a.risk),
                        )
                        for a in assignments
                    ],
                )
            if prune_reports is not None:
                self._conn.execute(
                    "UPDATE runs SET prune_json=? WHERE run_id=?",
                    (
                        json.dumps([p.model_dump() for p in prune_reports]),
                        run_id,
                    ),
                )
            self._conn.commit()

    def save_report(self, run_id: str, report: ScheduleReport) -> None:
        """run 收尾时落盘最终报告（审计/成本/学习数据源，供报告查询）。"""
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET report_json=?, run_status=?, updated_at=? WHERE run_id=?",
                (report.model_dump_json(), report.final_status, _ts(), run_id),
            )
            self._conn.commit()

    def load_run(self, run_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT dag_json, run_status, prune_json FROM runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"run 不存在：{run_id}")
            dag = DAG.model_validate_json(row["dag_json"])
            assignments: dict[str, Assignment] = {}
            for r in self._conn.execute(
                "SELECT * FROM assignments WHERE run_id=?", (run_id,)
            ).fetchall():
                assignments[r["task_id"]] = Assignment(
                    task_id=r["task_id"],
                    agent_id=r["agent_id"],
                    match_type=r["match_type"],
                    reason=r["reason"],
                    risk=bool(r["risk"]),
                )
            prune_reports = [
                PruneReport.model_validate(p)
                for p in json.loads(row["prune_json"] or "[]")
            ]
        return {
            "dag": dag,
            "run_status": row["run_status"],
            "assignments": assignments,
            "prune_reports": prune_reports,
        }

    def has_run(self, run_id: str) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM runs WHERE run_id=?", (run_id,)
            ).fetchone() is not None

    def active_runs(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs WHERE run_status='running'"
            ).fetchall()
            return [r["run_id"] for r in rows]

    def delete_run(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM assignments WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
            self._conn.commit()


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
