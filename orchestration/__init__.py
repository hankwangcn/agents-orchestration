"""agents-orchestration：结果导向 Agent 编排框架。"""

from .models import DAG, Result, Task, TaskStatus, PruneReport
from .dependency import DependencyGraph, DependencyError
from .scheduler import Scheduler, ScheduleReport
from .scheduler_async import AsyncScheduler, ScheduleReport as AsyncScheduleReport
from .decomposer import Decomposer, DecomposeError
from .agent_pool import AgentPool, RegisteredAgent
from .allocator import Allocator
from .registry import AgentRegistry
from .adapters.base import AgentAdapter

__all__ = [
    "DAG",
    "Result",
    "Task",
    "TaskStatus",
    "PruneReport",
    "DependencyGraph",
    "DependencyError",
    "Scheduler",
    "ScheduleReport",
    "AsyncScheduler",
    "AsyncScheduleReport",
    "Decomposer",
    "DecomposeError",
    "AgentPool",
    "RegisteredAgent",
    "Allocator",
    "AgentRegistry",
    "AgentAdapter",
]

__version__ = "0.2.0"
