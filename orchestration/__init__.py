"""agents-orchestration：结果导向 Agent 编排框架。"""

from .models import DAG, Result, Task, TaskStatus, PruneReport
from .scheduler import Scheduler, ScheduleReport
from .scheduler_async import AsyncScheduler, ScheduleReport as AsyncScheduleReport
from .decomposer import Decomposer, DecomposeError
from .adapters.base import AgentAdapter

__all__ = [
    "DAG",
    "Result",
    "Task",
    "TaskStatus",
    "PruneReport",
    "Scheduler",
    "ScheduleReport",
    "AsyncScheduler",
    "AsyncScheduleReport",
    "Decomposer",
    "DecomposeError",
    "AgentAdapter",
]

__version__ = "0.2.0"
