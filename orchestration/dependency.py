"""依赖分析（架构 §3.2 规划层）：DAG → DependencyGraph。

定位：把"数据流依赖"从任务模型里抽出来，成为**独立可分析的产物**——此前图
算法分散内联在拆解引擎（Kahn）与 DAG 模型（可达性）中，规划层这一环事实上
是空的。现收敛为本模块，与文档"依赖分析（数据流依赖，非运行时状态）"一致。

**纯结构视图**：只回答结构问题（引用合法性 / 无环 / 拓扑序 / 并行前沿 / 可达
性 / 交付点），**不持有运行时状态**——Task.status 归调度层，本模块不读它。

消费方：
- 拆解引擎（规划层）：产物 DAG 经 `validate()` 校验后返回（引用存在 / 无环 /
  至少一个交付点）；
- DAG 调度器（调度层）：`topological_order()` / `levels()`（并行前沿）；
- 失败处理器（调度层）：`reverse_reachable()` 是反向可达剪枝的判据（§5.2）。

构造零成本（仅建一张邻接表），故各调用点按需即时构造，无需缓存。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 仅类型标注用，避免与 models 循环导入
    from .models import DAG


class DependencyError(ValueError):
    """依赖结构非法：引用不存在的任务 / 成环 / 无最终交付任务。

    继承 ValueError，使调用方既有的 ``except (ResponseValidationError,
    ValueError)`` 兜底口径保持不变。
    """


class DependencyGraph:
    """DAG 之上的依赖分析视图。

    输入契约：一个 ``DAG``（``tasks: dict[str, Task]``，任务含 ``deps``）。
    输出：结构分析结果（集合 / 有序列表 / 层级），无副作用、不改任务状态。
    """

    def __init__(self, dag: "DAG"):
        self._dag = dag
        self._tasks = dag.tasks
        # 邻接表：children[上游] = 直接下游集合；孤岛任务也有空集条目
        self._children: dict[str, set[str]] = {tid: set() for tid in self._tasks}
        for tid, t in self._tasks.items():
            for d in t.deps:
                if d in self._children:  # 幽灵引用由 validate_edges() 另行报错
                    self._children[d].add(tid)

    # ---------- 基础结构 ----------

    @property
    def tasks(self) -> dict:
        return self._tasks

    def parents(self, task_id: str) -> list[str]:
        """直接上游（= 该任务的 deps，保持声明顺序）。"""
        return list(self._tasks[task_id].deps)

    def children(self, task_id: str) -> set[str]:
        """直接下游。"""
        return set(self._children.get(task_id, ()))

    def roots(self) -> set[str]:
        """入度为 0：无依赖，可立即派发（并行前沿的第一层）。"""
        return {tid for tid, t in self._tasks.items() if not t.deps}

    def final_tasks(self) -> set[str]:
        """最终交付任务：出度为 0（产出无人继续消费，即交付点）。"""
        return {tid for tid, ch in self._children.items() if not ch}

    # ---------- 可达性 ----------

    def descendants(self, task_id: str) -> set[str]:
        """T 的全部后代（含间接下游）——输入链断裂所波及的任务。"""
        result: set[str] = set()
        stack = list(self._children.get(task_id, ()))
        while stack:
            cur = stack.pop()
            if cur in result:
                continue
            result.add(cur)
            stack.extend(self._children.get(cur, ()))
        return result

    def reverse_reachable(self, roots: set[str]) -> set[str]:
        """反向可达：从 roots 沿依赖边反向遍历，返回全部上游（含 roots）。

        剪枝判据（架构 §5.2）："我的产出还有没有人要？"
        反向走得到 = 产出仍被最终交付消费 = 保留。
        """
        reachable: set[str] = set()
        stack = list(roots)
        while stack:
            cur = stack.pop()
            if cur in reachable or cur not in self._tasks:
                continue
            reachable.add(cur)
            stack.extend(self._tasks[cur].deps)
        return reachable

    def unreachable_from(self, roots: set[str]) -> set[str]:
        """反向不可达集（剪枝的取消集）：从 roots 反向走不到的任务。"""
        return set(self._tasks) - self.reverse_reachable(roots)

    # ---------- 拓扑 ----------

    def topological_order(self) -> list[str]:
        """Kahn 拓扑序；成环则抛 DependencyError。

        并列节点的顺序取 tasks 的插入序（确定性，便于测试与复现）。
        """
        indeg = {tid: len(t.deps) for tid, t in self._tasks.items()}
        queue = [tid for tid in self._tasks if indeg[tid] == 0]
        order: list[str] = []
        while queue:
            cur = queue.pop(0)
            order.append(cur)
            for nxt in self._children.get(cur, ()):
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    queue.append(nxt)
        if len(order) != len(self._tasks):
            raise DependencyError("依赖关系成环，不是 DAG")
        return order

    def has_cycle(self) -> bool:
        try:
            self.topological_order()
        except DependencyError:
            return True
        return False

    def levels(self) -> list[list[str]]:
        """拓扑分层（并行前沿）：同一层内互不依赖，可并行派发。

        层 0 = 根（无依赖）；层 k = 依赖全部落在前 k-1 层之内的任务。
        成环则抛 DependencyError。
        """
        indeg = {tid: len(t.deps) for tid, t in self._tasks.items()}
        current = [tid for tid in self._tasks if indeg[tid] == 0]
        out: list[list[str]] = []
        seen = 0
        while current:
            out.append(list(current))
            seen += len(current)
            nxt: list[str] = []
            for cur in current:
                for child in self._children.get(cur, ()):
                    indeg[child] -= 1
                    if indeg[child] == 0:
                        nxt.append(child)
            current = nxt
        if seen != len(self._tasks):
            raise DependencyError("依赖关系成环，不是 DAG")
        return out

    def max_parallel_width(self) -> int:
        """最大并行宽度：同一层内最多可同时派发的任务数（资源协调参考）。"""
        return max((len(lv) for lv in self.levels()), default=0)

    # ---------- 校验（拆解引擎消费） ----------

    def validate_edges(self) -> None:
        """依赖引用存在性：deps 只能引用本图内已定义的任务。"""
        for tid, t in self._tasks.items():
            for d in t.deps:
                if d not in self._tasks:
                    raise DependencyError(f"任务 {tid} 依赖不存在的任务 {d}")

    def validate(self, require_final: bool = True) -> None:
        """结构合法性总校验：引用存在 → 无环 → 至少一个最终交付任务。

        拆解引擎在把 LLM 输出转成 DAG 后调用（不合格即拒绝并重试）。
        """
        self.validate_edges()
        self.topological_order()
        if require_final and not self.final_tasks():
            raise DependencyError("不存在最终交付任务（出度为 0 的任务）")
