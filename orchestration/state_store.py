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
prune_reports（随 run 行存）+ learning_lessons 表（学习层经验库——跨 run
记忆，回馈拆解提示词）+ run_events 表（**过程归档**：状态变更逐条落盘的
事件流）+ run_narratives 表（人读叙述摘要，**非确定**、显式生成、单独留痕）。
SQLite 单文件、零依赖；可换 Postgres（StateStore 抽象，实现同签名即可——
经验库、事件流、叙述摘要均为可选能力，未实现则各自退化）。

**归档分层（对话共识）**：运行存档 = 过程（事件流）+ 终态报告（report_json），
属**持久化底座**（state face），是唯一真源、单 run 不可变；治理层是其生产者
之一（审计/成本/判定挂回报告），学习层是消费者（读同一份 → 经验库）。人读版
是**读时投影**（按需渲染，不落盘成第二份真相）。
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
    goal        TEXT NOT NULL DEFAULT '',
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
CREATE TABLE IF NOT EXISTS learning_lessons (
    run_id     TEXT NOT NULL,
    rule_id    TEXT NOT NULL,
    tier       TEXT NOT NULL DEFAULT 'objective',
    severity   TEXT NOT NULL DEFAULT 'low',
    category   TEXT NOT NULL DEFAULT '',
    message    TEXT NOT NULL DEFAULT '',
    action     TEXT NOT NULL DEFAULT '',
    evidence   TEXT NOT NULL DEFAULT '{}',
    created_at TEXT,
    PRIMARY KEY (run_id, rule_id)
);
CREATE TABLE IF NOT EXISTS run_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL,
    ts       TEXT NOT NULL,
    event    TEXT NOT NULL,
    task_id  TEXT NOT NULL DEFAULT '',
    agent_id TEXT NOT NULL DEFAULT '',
    data     TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id, id);
CREATE TABLE IF NOT EXISTS run_narratives (
    run_id     TEXT PRIMARY KEY,
    created_at TEXT,
    model      TEXT NOT NULL DEFAULT '',
    text       TEXT NOT NULL DEFAULT ''
);
"""


class StateStore(ABC):
    """调度状态持久化抽象（SQLite 实现；可换 Postgres 等）。"""

    @abstractmethod
    def save_run(
        self,
        run_id: str,
        dag: DAG,
        goal: Optional[str] = None,
        run_status: Optional[str] = None,
        assignments: Optional[list[Assignment]] = None,
        prune_reports: Optional[list[PruneReport]] = None,
    ) -> None: ...

    @abstractmethod
    def save_report(self, run_id: str, report: ScheduleReport) -> None: ...

    @abstractmethod
    def load_run(self, run_id: str) -> dict:
        """返回 {dag, goal, assignments: {tid: Assignment}, prune_reports}。"""

    @abstractmethod
    def has_run(self, run_id: str) -> bool: ...

    @abstractmethod
    def active_runs(self) -> list[str]: ...

    @abstractmethod
    def delete_run(self, run_id: str) -> None: ...

    # -- 经验库（学习层闭环：跨 run 记忆；可选能力，默认退化为无记忆）--

    def save_lessons(self, run_id: str, report: object) -> None:
        """落盘一次 run 的学习规则（幂等：同 run 重跑覆盖）。

        存储实现未支持经验库时退化为 no-op——学习层仍产出报告，只是没有
        跨 run 记忆（提示词回馈退化为仅注册表事实）。
        """
        return None

    def load_lessons(self) -> list[dict]:
        """读全部经验库原始行（聚合交给 lessons.build_digest）。"""
        return []

    # -- 归档（过程事件流 + 报告读回 + 运行枚举；可选能力默认退化）--

    def append_event(
        self,
        run_id: str,
        event: str,
        task_id: str = "",
        agent_id: str = "",
        data: Optional[dict] = None,
    ) -> None:
        """追加一条过程事件（状态变更逐条落盘，run 内时序可回放）。

        未支持事件流的实现退化为 no-op——run 照常收尾，只是过程不可回放。
        """
        return None

    def load_events(self, run_id: str) -> list[dict]:
        """读一个 run 的事件流（按发生顺序）。"""
        return []

    def load_report(self, run_id: str) -> Optional[dict]:
        """读回已落盘的终态报告（dict）；未落盘或未支持返回 None。

        进程重启后运行存档仍可读——这是人读投影（按需渲染）的唯一真源。
        """
        return None

    def list_runs(self, limit: int = 50) -> list[dict]:
        """运行枚举（最近在前）：崩溃后/换进程后仍可发现已有 run。"""
        return []

    def save_narrative(self, run_id: str, narrative: dict) -> None:
        """落盘人读**叙述摘要**（非确定、显式生成、单独留痕，不混入确定性报告）。"""
        return None

    def load_narrative(self, run_id: str) -> Optional[dict]:
        """读回叙述摘要（None = 未生成）。"""
        return None


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
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """既有库补列（CREATE TABLE IF NOT EXISTS 不会给旧表加列）。"""
        cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(runs)")
        }
        if "goal" not in cols:
            self._conn.execute(
                "ALTER TABLE runs ADD COLUMN goal TEXT NOT NULL DEFAULT ''"
            )

    # ------------------------------------------------------------------

    def save_run(
        self,
        run_id: str,
        dag: DAG,
        goal: Optional[str] = None,
        run_status: Optional[str] = None,
        assignments: Optional[list[Assignment]] = None,
        prune_reports: Optional[list[PruneReport]] = None,
    ) -> None:
        """事件驱动整存：DAG（含每任务 status/result）+ 可选 assignment/prune。

        goal / run_status 传 None 表示"保留原值"（调度器的频繁落盘不该覆盖
        提交时写入的 goal，resolve 等局部变更不该覆盖调度状态）。
        """
        now = _ts()
        with self._lock:
            row = self._conn.execute(
                "SELECT run_status, goal FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if run_status is None:
                run_status = row["run_status"] if row else "running"
            if goal is None:
                goal = row["goal"] if row else ""
            self._conn.execute(
                """
                INSERT INTO runs (run_id, dag_json, goal, run_status, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    dag_json=excluded.dag_json,
                    goal=excluded.goal,
                    run_status=excluded.run_status,
                    updated_at=excluded.updated_at
                """,
                (run_id, dag.model_dump_json(), goal, run_status, now),
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
                "SELECT dag_json, goal, run_status, prune_json FROM runs WHERE run_id=?",
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
            "goal": row["goal"] or "",
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
            self._conn.execute(
                "DELETE FROM learning_lessons WHERE run_id=?", (run_id,)
            )
            self._conn.execute("DELETE FROM run_events WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM run_narratives WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
            self._conn.commit()

    # -- 经验库（学习层闭环）--

    def save_lessons(self, run_id: str, report: object) -> None:
        """落盘一次 run 的学习规则（幂等：同 run 重跑覆盖，不重复计数）。

        report：learning.LearningReport（duck-typing，避免 state_store 反向
        依赖学习层）。规则为空则只清空该 run 的旧行，不写入。
        """
        rules = list(getattr(report, "rules", []) or [])
        with self._lock:
            self._conn.execute(
                "DELETE FROM learning_lessons WHERE run_id=?", (run_id,)
            )
            if rules:
                self._conn.executemany(
                    """
                    INSERT INTO learning_lessons
                        (run_id, rule_id, tier, severity, category, message,
                         action, evidence, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            run_id,
                            r.rule_id,
                            getattr(r, "tier", "") or "objective",
                            r.severity,
                            r.category,
                            r.message,
                            r.action,
                            json.dumps(r.evidence or {}, ensure_ascii=False),
                            _ts(),
                        )
                        for r in rules
                    ],
                )
            self._conn.commit()

    def load_lessons(self) -> list[dict]:
        """读全部经验库原始行（按时间正序；聚合在 lessons.build_digest）。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT run_id, rule_id, tier, severity, category, message,
                       action, evidence, created_at
                FROM learning_lessons ORDER BY created_at, run_id, rule_id
                """
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            try:
                evidence = json.loads(r["evidence"] or "{}")
            except (ValueError, TypeError):
                evidence = {}
            out.append({
                "run_id": r["run_id"],
                "rule_id": r["rule_id"],
                "tier": r["tier"],
                "severity": r["severity"],
                "category": r["category"],
                "message": r["message"],
                "action": r["action"],
                "evidence": evidence if isinstance(evidence, dict) else {},
                "created_at": r["created_at"] or "",
            })
        return out

    # -- 归档（过程事件流 + 报告读回 + 运行枚举）--

    def append_event(
        self,
        run_id: str,
        event: str,
        task_id: str = "",
        agent_id: str = "",
        data: Optional[dict] = None,
    ) -> None:
        """追加一条过程事件（事件驱动：状态变更即写，崩溃点过程最新）。"""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO run_events (run_id, ts, event, task_id, agent_id, data)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, _ts(), event, task_id or "", agent_id or "",
                    json.dumps(data or {}, ensure_ascii=False, default=str),
                ),
            )
            self._conn.commit()

    def load_events(self, run_id: str) -> list[dict]:
        """读一个 run 的事件流（按发生顺序 = 自增 id 升序）。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT ts, event, task_id, agent_id, data FROM run_events
                WHERE run_id=? ORDER BY id
                """,
                (run_id,),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            try:
                data = json.loads(r["data"] or "{}")
            except (ValueError, TypeError):
                data = {}
            out.append({
                "ts": r["ts"] or "",
                "event": r["event"],
                "task_id": r["task_id"] or "",
                "agent_id": r["agent_id"] or "",
                "data": data if isinstance(data, dict) else {},
            })
        return out

    def load_report(self, run_id: str) -> Optional[dict]:
        """读回已落盘的终态报告（dict）；未落盘返回 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT report_json FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None or not row["report_json"]:
            return None
        try:
            return json.loads(row["report_json"])
        except (ValueError, TypeError):
            return None

    def list_runs(self, limit: int = 50) -> list[dict]:
        """运行枚举（最近在前；同秒并列按写入顺序倒序）。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT run_id, goal, run_status, updated_at,
                       (report_json IS NOT NULL) AS has_report
                FROM runs ORDER BY updated_at DESC, rowid DESC LIMIT ?
                """,
                (max(0, int(limit)),),
            ).fetchall()
        return [
            {
                "run_id": r["run_id"],
                "goal": r["goal"] or "",
                "run_status": r["run_status"],
                "updated_at": r["updated_at"] or "",
                "has_report": bool(r["has_report"]),
            }
            for r in rows
        ]

    def save_narrative(self, run_id: str, narrative: dict) -> None:
        """落盘叙述摘要（按 run 覆盖；非确定产物，单独留痕）。"""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO run_narratives (run_id, created_at, model, text)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    created_at=excluded.created_at,
                    model=excluded.model,
                    text=excluded.text
                """,
                (
                    run_id,
                    narrative.get("created_at") or _ts(),
                    narrative.get("model") or "",
                    narrative.get("text") or "",
                ),
            )
            self._conn.commit()

    def load_narrative(self, run_id: str) -> Optional[dict]:
        """读回叙述摘要（None = 未生成）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT created_at, model, text FROM run_narratives WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "created_at": row["created_at"] or "",
            "model": row["model"] or "",
            "text": row["text"] or "",
        }


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
