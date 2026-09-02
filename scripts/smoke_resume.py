"""断点恢复端到端冒烟：运行中"崩溃" → StateStore 恢复 → 继续跑完。

场景 1：崩溃点在无副作用任务执行中 → 恢复后重派，整棵跑完。
场景 2：崩溃点在声明副作用任务执行中 → 恢复后置 INTERRUPTED 不重派，
        人工 complete 后再恢复，下游继续。
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestration.models import DAG, SideEffects, Task, TaskStatus
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler
from orchestration.state_store import SqliteStateStore

from tests.helpers import AsyncScriptedAdapter, ok


async def crash_while_running() -> None:
    """模拟进程崩溃：取消全部协程（内存状态全部消失，store 保持崩溃点落盘）。

    真实崩溃 = 进程终止，所有 in-flight 协程连同其内存引用一起消失；
    这里取消 asyncio.all_tasks() 中除当前外的全部任务（含调度主协程与其
    派生的执行子任务），等价于崩溃瞬间的内存清空。
    """
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def scenario1_pure_rerun(store: SqliteStateStore) -> None:
    """无副作用任务执行中崩溃 → 恢复后重派。"""
    store.delete_run("s1")
    dag = DAG(tasks={
        "a": Task(id="a", desc="a"),
        "b": Task(id="b", desc="b", deps=["a"]),
        "c": Task(id="c", desc="c", deps=["b"]),
    })
    adapter = AsyncScriptedAdapter(
        {"a": [ok("a", {"n": 1})], "b": [ok("b")], "c": [ok("c")]},
        delay=0.15,
    )
    reg = AgentRegistry()
    reg.register(adapter)
    sched = AsyncScheduler(registry=reg, state_store=store)

    asyncio.get_running_loop().create_task(sched.run(dag, run_id="s1"))
    await asyncio.sleep(0.05)  # a 正在执行（delay 0.15）→ RUNNING 已落盘
    await crash_while_running()
    print("[S1] 崩溃时 a=running（无副作用）→ store 已落盘 RUNNING")

    # 重启：全新调度器 + 同一 store
    sched2 = AsyncScheduler(registry=reg, state_store=store)
    report = await sched2.resume_run("s1")
    assert report.final_status == "success"
    assert report.dag.tasks["c"].status == TaskStatus.SUCCESS
    assert report.dag.tasks["a"].result.output == {"n": 1}
    assert report.total_cost == 0.03
    calls = [c[0] for c in adapter.calls]
    # a 调用 2 次：崩溃瞬间请求已发出（agent 端可能已执行）+ 恢复重派——
    # 这正是 A 策略语义：纯产出任务重复执行可接受，代价是重复计费一次
    assert calls.count("a") == 2
    assert calls.count("b") == 1 and calls.count("c") == 1
    print(f"[S1] ✅ 恢复后整棵跑完，final={report.final_status}, cost=${report.total_cost}")


async def scenario2_side_effect_interrupted(store: SqliteStateStore) -> None:
    """副作用任务执行中崩溃 → INTERRUPTED，人工 complete 后继续。"""
    store.delete_run("s2")
    dag = DAG(tasks={
        "a": Task(id="a", desc="a"),
        "b": Task(id="b", desc="b", deps=["a"],
                  side_effects=SideEffects.EXTERNAL_API),
    })
    adapter = AsyncScriptedAdapter(
        {"a": [ok("a", {"n": 1})], "b": [ok("b")]},
        delay=0.15,
    )
    adapter.script_delay = {"a": 0.05, "b": 0.15}  # a 快完成，b 慢（执行中被崩溃）
    reg = AgentRegistry()
    reg.register(adapter)
    sched = AsyncScheduler(registry=reg, state_store=store)

    asyncio.get_running_loop().create_task(sched.run(dag, run_id="s2"))
    await asyncio.sleep(0.1)  # a 已成功（0.05），b（副作用）正在执行 → RUNNING 已落盘
    await crash_while_running()
    print("[S2] 崩溃时 b=running（声明副作用）→ store 已落盘 RUNNING")

    sched2 = AsyncScheduler(registry=reg, state_store=store)
    b_calls_before = [c[0] for c in adapter.calls].count("b")
    report = await sched2.resume_run("s2")
    assert report.final_status == "interrupted"
    assert report.dag.tasks["b"].status == TaskStatus.INTERRUPTED
    b_calls_after = [c[0] for c in adapter.calls].count("b")
    assert b_calls_after == b_calls_before  # 恢复后未重派 b（副作用任务）
    print(f"[S2] ✅ 恢复后 b=INTERRUPTED（未重派，等待人工），final={report.final_status}")

    # 人工确认：complete b（副作用已核实执行完成）
    from orchestration.models import Result
    data = store.load_run("s2")
    dag2 = data["dag"]
    dag2.tasks["b"].status = TaskStatus.SUCCESS
    dag2.tasks["b"].result = Result(
        task_id="b", success=True, output={"done": True},
        usage={"tokens_in": 5, "tokens_out": 5, "cost": 0.005},
    )
    store.save_run("s2", dag2, assignments=list(data["assignments"].values()),
                   prune_reports=data["prune_reports"])

    sched3 = AsyncScheduler(registry=reg, state_store=store)
    report2 = await sched3.resume_run("s2")
    assert report2.final_status == "success"
    print(f"[S2] ✅ 人工 complete 后再次恢复 → final={report2.final_status}, "
          f"b.result={report2.dag.tasks['b'].result.output}")


async def main() -> None:
    store = SqliteStateStore("/tmp/smoke_resume.db")
    await scenario1_pure_rerun(store)
    await scenario2_side_effect_interrupted(store)
    print("\n✅ 断点恢复冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
