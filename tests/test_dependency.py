"""依赖分析测试（规划层 §3.2）：DAG → DependencyGraph 的独立结构视图。

覆盖：邻接结构（父/子/根/交付点）、可达性（后代 / 反向可达 / 反向不可达）、
拓扑（拓扑序 / 分层并行前沿 / 最大并行宽度 / 成环检测）、合法性校验
（引用存在 / 无环 / 至少一个交付点）、空图边界，以及 DAG 薄委托一致性
（结构算法已独立，DAG 保留同名方法作兼容）。
"""
from __future__ import annotations

import pytest

from orchestration.dependency import DependencyError, DependencyGraph
from orchestration.models import DAG, Task


def dag_of(*spec: tuple[str, list[str]]) -> DAG:
    """按 (id, deps) 规格建 DAG。"""
    return DAG(tasks={
        tid: Task(id=tid, desc=f"任务 {tid}", deps=list(deps))
        for tid, deps in spec
    })


# 钻石图：a → (b, c) → d（分层 a | b,c | d）
DIAMOND = [("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"])]


class TestStructure:
    def test_parents_and_children(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.parents("d") == ["b", "c"]
        assert g.children("a") == {"b", "c"}
        assert g.children("d") == set()

    def test_roots_and_final_tasks(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.roots() == {"a"}
        assert g.final_tasks() == {"d"}

    def test_parallel_only_graph(self):
        """无依赖 = 默认并行：全部是根，也全是交付点。"""
        g = DependencyGraph(dag_of(("a", []), ("b", []), ("c", [])))
        assert g.roots() == {"a", "b", "c"}
        assert g.final_tasks() == {"a", "b", "c"}

    def test_chain_graph(self):
        """串接 = 链：单根单交付点，分层逐级。"""
        g = DependencyGraph(dag_of(("a", []), ("b", ["a"]), ("c", ["b"])))
        assert g.roots() == {"a"}
        assert g.final_tasks() == {"c"}
        assert g.levels() == [["a"], ["b"], ["c"]]

    def test_ghost_dependency_not_in_children(self):
        g = DependencyGraph(dag_of(("a", ["ghost"])))
        assert g.children("ghost") == set()   # 幽灵引用不入邻接表
        assert g.final_tasks() == {"a"}


class TestReachability:
    def test_descendants(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.descendants("a") == {"b", "c", "d"}
        assert g.descendants("b") == {"d"}
        assert g.descendants("d") == set()

    def test_reverse_reachable(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.reverse_reachable({"d"}) == {"a", "b", "c", "d"}
        assert g.reverse_reachable({"b"}) == {"a", "b"}

    def test_unreachable_from(self):
        """剪枝取消集：存活交付点反向走不到的任务。"""
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.unreachable_from({"d"}) == set()      # 全图都喂给 d
        assert g.unreachable_from({"b"}) == {"c", "d"}  # c 分支与 d 被剪

    def test_two_independent_branches(self):
        """独立交付分支：一个失败不误杀另一个（反向可达只留自己的链）。"""
        g = DependencyGraph(dag_of(("a", []), ("b", ["a"]), ("x", []), ("y", ["x"])))
        assert g.unreachable_from({"b"}) == {"x", "y"}
        assert g.reverse_reachable({"b", "y"}) == {"a", "b", "x", "y"}


class TestTopology:
    def test_topological_order_respects_deps(self):
        order = DependencyGraph(dag_of(*DIAMOND)).topological_order()
        assert order.index("a") < order.index("b") < order.index("d")
        assert order.index("a") < order.index("c") < order.index("d")
        assert len(order) == 4

    def test_topological_order_is_deterministic(self):
        """并列节点按 tasks 插入序，保证可复现。"""
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.topological_order() == ["a", "b", "c", "d"]

    def test_levels_are_parallel_frontiers(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.levels() == [["a"], ["b", "c"], ["d"]]

    def test_max_parallel_width(self):
        assert DependencyGraph(dag_of(*DIAMOND)).max_parallel_width() == 2
        assert DependencyGraph(
            dag_of(("a", []), ("b", []), ("c", []))
        ).max_parallel_width() == 3

    def test_cycle_raises(self):
        g = DependencyGraph(dag_of(("a", ["b"]), ("b", ["a"])))
        assert g.has_cycle() is True
        with pytest.raises(DependencyError, match="成环"):
            g.topological_order()
        with pytest.raises(DependencyError, match="成环"):
            g.levels()

    def test_self_loop_is_cycle(self):
        g = DependencyGraph(dag_of(("a", ["a"])))
        assert g.has_cycle() is True


class TestDeterministicOrdering:
    """回归：邻接表曾用 set——字符串 hash 随机化（PYTHONHASHSEED）使并列节点
    顺序在进程间不可复现，拓扑序/并行前沿偶发翻转（曾在全量跑时间歇失败）。"""

    def test_sibling_order_follows_task_insertion(self):
        g = DependencyGraph(dag_of(*DIAMOND))
        assert g.levels() == [["a"], ["b", "c"], ["d"]]
        assert g.topological_order() == ["a", "b", "c", "d"]

    def test_repeated_construction_is_stable(self):
        """同一规格反复构造，顺序完全一致（不受 hash 随机化影响）。"""
        orders = {
            tuple(DependencyGraph(dag_of(*DIAMOND)).topological_order())
            for _ in range(200)
        }
        assert orders == {("a", "b", "c", "d")}

    def test_duplicate_dep_not_counted_twice(self):
        """重复声明同一 dep 不重复计入度（否则节点永不入队，误报成环）。"""
        g = DependencyGraph(dag_of(("a", []), ("b", ["a", "a"])))
        assert g.topological_order() == ["a", "b"]
        assert g.levels() == [["a"], ["b"]]


class TestValidate:
    def test_valid_graph_passes(self):
        DependencyGraph(dag_of(*DIAMOND)).validate()  # 不抛异常

    def test_missing_dependency_rejected(self):
        with pytest.raises(DependencyError, match="依赖不存在的任务 ghost"):
            DependencyGraph(dag_of(("a", ["ghost"]))).validate()

    def test_cycle_rejected(self):
        with pytest.raises(DependencyError, match="成环"):
            DependencyGraph(dag_of(("a", ["b"]), ("b", ["a"]))).validate()

    def test_require_final(self, monkeypatch):
        """无交付点：无环非空图必有 sink，正常不可达——monkeypatch 覆盖防守分支。"""
        monkeypatch.setattr(DependencyGraph, "final_tasks", lambda self: set())
        with pytest.raises(DependencyError, match="最终交付任务"):
            DependencyGraph(dag_of(("a", []))).validate(require_final=True)
        DependencyGraph(dag_of(("a", []))).validate(require_final=False)  # 可关

    def test_empty_dag(self):
        g = DependencyGraph(DAG(tasks={}))
        assert g.roots() == set()
        assert g.final_tasks() == set()
        assert g.levels() == []
        assert g.max_parallel_width() == 0
        g.validate(require_final=False)   # 空图无环无幽灵引用
        with pytest.raises(DependencyError, match="最终交付任务"):
            g.validate(require_final=True)  # 空图无交付点 → 非法

    def test_dependency_error_is_value_error(self):
        """继承 ValueError：调用方既有兜底捕获口径不变。"""
        assert issubclass(DependencyError, ValueError)


class TestDAGDelegation:
    """结构算法已独立到 DependencyGraph；DAG 同名方法为薄委托（兼容）。"""

    def test_delegates_to_dependency_graph(self, monkeypatch):
        called: list[str] = []
        real = DependencyGraph.descendants

        def spy(self, task_id):
            called.append(task_id)
            return real(self, task_id)

        monkeypatch.setattr(DependencyGraph, "descendants", spy)
        assert dag_of(*DIAMOND).descendants("a") == {"b", "c", "d"}
        assert called == ["a"]

    def test_results_identical_to_graph(self):
        dag = dag_of(*DIAMOND)
        g = dag.dependency_graph()
        assert dag.descendants("a") == g.descendants("a")
        assert dag.final_tasks() == g.final_tasks()
        assert dag.reverse_reachable({"d"}) == g.reverse_reachable({"d"})
