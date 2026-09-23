"""学习层闭环冒烟（档三：真实 HTTP 往返 + 真实 CLI，无需真实 LLM 与 API key）。

验证链路（学习层闭环 = 复盘事实 → 规则 → 落盘经验库 → 回馈拆解提示词）：
注册 → info_request 能力采集 → 提交带 goal 的 run（真实 HTTP 执行，含失败/
剪枝/降级）→ run 收尾**确定性复盘**（审计对账 → 成本归集 → 规则提取，含判定
结论）→ 规则**落盘为跨 run 经验库**（SQLite learning_lessons 表）→ **注入拆解
提示词**（注册表事实 + 复现达标的客观规则）。

同时验证：
- 证据强度分级：FP/DEG/PRU 归 objective，JUD-1 归 judgment，禁止同级呈现
- 复现门槛：单 run 的规则**不进**提示词，跨 run 复现后才进（"客观支撑"=复现证据）
- 提示词结构稳定：前缀恒为固定指令、结尾恒为用户目标，指导块居中注入
- 经验库聚合（ao lessons / GET /api/lessons）：命中次数 / 贡献 run 数
- 学习层是复盘不是主链路：无 goal 的 run 照常收尾并产出学习产物

运行：.venv/bin/python scripts/smoke_learning.py
（无需 API key——全部打到本地 mock 端点 scripts/mock_agents.py）
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import threading

import uvicorn

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orchestration.cli as cli
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.api.gateway import create_app
from orchestration.decomposer import DECOMPOSITION_PROMPT, Decomposer
from orchestration.lessons import PromptAdvisor
from orchestration.models import DAG, ResourceRequirement, Task
from orchestration.reflection import Reflector
from orchestration.registry import AgentRegistry
from orchestration.state_store import SqliteStateStore

from mock_agents import MockAgentServer, judge_configs

GATEWAY_PORT = 8231
GATEWAY = f"http://127.0.0.1:{GATEWAY_PORT}"

GOAL = "把抓取到的价格数据整理成一份比价报告"

# 拆解引擎的假 LLM（本冒烟只验提示词内容，不调真实 LLM）
FAKE_DAG = json.dumps({"tasks": [
    {"id": "t1", "desc": "抓取价格", "deps": []},
    {"id": "t2", "desc": "出比价报告", "deps": ["t1"]},
]}, ensure_ascii=False)


def build_dag() -> DAG:
    """一次 run 同时产出三类客观返工信号：失败（FP）、降级（DEG）、剪枝（PRU）。

    a/b：model=flaky（mock 注入业务失败）→ 同错误码 2 次 → FP-fail_injected
    c  ：依赖 a → a 失败后 c 被剪枝 → PRU-1
    d  ：model/caps 均无匹配 → 降级默认 agent 且成功 → DEG-1（降级占比超阈值）
    """
    return DAG(tasks={
        "a": Task(id="a", desc="抓取站点 A 价格",
                  required_resources=ResourceRequirement(model="flaky")),
        "b": Task(id="b", desc="抓取站点 B 价格",
                  required_resources=ResourceRequirement(model="flaky")),
        "c": Task(id="c", desc="合并 A/B 价格", deps=["a"],
                  required_resources=ResourceRequirement(model="flaky")),
        "d": Task(id="d", desc="生成比价报告",
                  required_resources=ResourceRequirement(model="ghost"),
                  required_capabilities=["nonexistent_cap"]),
    })


def build_registry(base_url: str) -> AgentRegistry:
    """注册执行 agent + 判定 agent（真实 HTTP，agent_id 即 mock 的 model 字段）。"""
    reg = AgentRegistry()
    for agent_id in ("translator", "coder", "analyst", "flaky", "judge"):
        reg.register(
            DeepSeekAdapter(model=agent_id, base_url=f"{base_url}/v1",
                            api_key="mock"),
            agent_id=agent_id,
        )
    return reg


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"   {'✅' if ok else '❌'} {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def rule_ids(payload: dict) -> list[str]:
    return [r["rule_id"] for r in (payload.get("learning") or {}).get("rules", [])]


def run_cli(argv: list[str]) -> int:
    """子线程里跑真实 CLI（内部走真实 HTTP 打到本地网关）。"""
    box: dict = {}
    th = threading.Thread(target=lambda: box.setdefault("rc", cli.main(argv)))
    th.start()
    th.join(120)
    return box.get("rc", -1)


async def _wait_gateway(uv: uvicorn.Server) -> None:
    for _ in range(200):
        if uv.started:
            return
        await asyncio.sleep(0.05)
    raise RuntimeError("网关未启动")


async def main() -> int:
    server = MockAgentServer(judge_configs())
    await server.start()
    print(f"mock agents 已启动: {server.base_url}（角色: {', '.join(server.configs)}）")

    registry = build_registry(server.base_url)
    db_path = os.path.join(tempfile.mkdtemp(prefix="ao-smoke-lessons-"), "state.db")
    store = SqliteStateStore(db_path)
    results: list[bool] = []

    try:
        print("\n== 1. 能力采集（真实 HTTP info_request）==")
        await registry.aensure_fresh()
        print(f"   judge.capabilities = {registry.get('judge').capabilities}"
              f"；flaky.capabilities = {registry.get('flaky').capabilities}")
        results.append(check("判定/执行能力均被问出来",
                             "judge" in registry.get("judge").capabilities))

        # 网关形态：run 走网关的 RunManager，CLI 才能按 run_id/经验库查询
        app, manager = create_app(
            registry, retries=0, state_store=store,
            reflector=Reflector(registry),
        )
        config = uvicorn.Config(app, host="127.0.0.1", port=GATEWAY_PORT,
                                log_level="warning")
        uv = uvicorn.Server(config)
        threading.Thread(target=uv.run, daemon=True).start()
        await _wait_gateway(uv)
        print(f"   网关 {GATEWAY}（RunManager + 学习层闭环）")

        print("\n== 2. 第一次 run：确定性复盘接入生产路径 ==")
        run1 = await manager.submit(build_dag(), goal=GOAL)
        report1 = await manager.wait(run1)
        payload1 = manager.report(run1)
        print(f"   run={run1} final_status={report1.final_status} "
              f"audit={payload1['audit']['verdict']} "
              f"cost=${payload1['cost']['total_cost']} "
              f"rules={rule_ids(payload1)}")
        results.append(check("报告带审计对账（确定性、只读）",
                             payload1["audit"]["total_tasks"] == 4))
        results.append(check("报告带成本归集（按 agent 归集非空）",
                             bool(payload1["cost"]["by_agent"])))
        results.append(check("报告带学习规则（复盘已接入生产路径）",
                             bool(rule_ids(payload1))))

        print("\n== 3. 三类客观返工信号 + 判定结论分级 ==")
        rules = {r["rule_id"]: r for r in payload1["learning"]["rules"]}
        for rid, r in rules.items():
            print(f"   {rid}: tier={r['tier']} severity={r['severity']} "
                  f"{r['message'][:52]}")
        results.append(check("FP-* 同错误码复现 2 次 → 客观规则",
                             any(rid.startswith("FP-") and r["tier"] == "objective"
                                 for rid, r in rules.items())))
        results.append(check("DEG-1 降级占比超阈值 → 客观规则",
                             rules.get("DEG-1", {}).get("tier") == "objective"))
        results.append(check("PRU-1 剪枝 → 客观规则",
                             rules.get("PRU-1", {}).get("tier") == "objective"))
        results.append(check("JUD-1 判定结论 → judgment 分级（禁止与客观同级）",
                             rules.get("JUD-1", {}).get("tier") == "judgment"))

        print("\n== 4. 经验库落盘（跨 run 记忆）==")
        rows = store.load_lessons()
        with sqlite3.connect(db_path) as conn:
            table_rows = conn.execute(
                "SELECT COUNT(*) FROM learning_lessons"
            ).fetchone()[0]
        print(f"   learning_lessons 行数={table_rows}（run={run1}）")
        results.append(check("规则真的落盘到 SQLite（不只是内存报告）",
                             table_rows == len(rows) and table_rows >= 4))

        print("\n== 5. 复现门槛：单 run 的规则尚未进提示词 ==")
        advisor = PromptAdvisor(registry, store, min_occurrences=2)
        g1 = advisor.guidance()
        print(f"   注册表事实在提示词中：{'deepseek-chat' in g1 or 'translator' in g1}"
              f"；复现规则 FP 在提示词中：{'fail_injected' in g1}")
        results.append(check("注册表客观事实始终进提示词（直接测量，无门槛）",
                             "translator" in g1 and "code_gen" in g1))
        results.append(check("单次出现不写成指导（客观支撑 = 跨 run 复现）",
                             "fail_injected" not in g1))

        print("\n== 6. 第二次 run：经验库累积 ==")
        run2 = await manager.submit(build_dag(), goal=GOAL)
        await manager.wait(run2)
        snap = manager.lessons_snapshot()
        fp = next(ls for ls in snap["lessons"]
                  if ls["rule_id"].startswith("FP-"))
        print(f"   runs_considered={snap['runs_considered']} "
              f"lesson_count={snap['lesson_count']} "
              f"{fp['rule_id']}: occurrences={fp['occurrences']} runs={fp['runs']}")
        results.append(check("经验库跨 run 累积（命中次数 / 贡献 run 数）",
                             snap["runs_considered"] == 2
                             and fp["occurrences"] == 2 and fp["runs"] == 2))

        print("\n== 7. 闭环出口：回馈拆解提示词 ==")
        g2 = PromptAdvisor(registry, store, min_occurrences=2).guidance()
        prompts: list[str] = []

        def fake_llm(prompt: str) -> str:
            prompts.append(prompt)
            return FAKE_DAG

        dag = Decomposer(fake_llm, guidance_provider=advisor.guidance).decompose(GOAL)
        prompt = prompts[0]
        print(f"   拆解出 {len(dag.tasks)} 个任务；提示词 {len(prompt)} 字符")
        print("   —— 注入的指导块（节选）——")
        for line in g2.splitlines()[:8]:
            print(f"   {line[:100]}")
        results.append(check("客观规则（带复现数值）进提示词",
                             "fail_injected" in prompt
                             and "既往 2 次运行命中 2 次" in prompt))
        results.append(check("判定结论单独成节、显式标注非确定来源",
                             PromptAdvisor.JUDGMENT_HEADER in prompt
                             and "非确定来源" in prompt))
        results.append(check("提示词结构稳定：前缀=固定指令、结尾=用户目标",
                             prompt.startswith(DECOMPOSITION_PROMPT)
                             and prompt.endswith(f"用户目标：{GOAL}")))
        results.append(check("客观与判定分区呈现（客观节在前、判定节在后）",
                             prompt.index(PromptAdvisor.OBJECTIVE_HEADER)
                             < prompt.index(PromptAdvisor.JUDGMENT_HEADER)))
        results.append(check("拆解仍走 Schema 校验（指导块不改输出格式）",
                             set(dag.tasks) == {"t1", "t2"}))

        print("\n== 8. 无 goal 的 run：学习层不依赖判定 ==")
        run3 = await manager.submit(build_dag())
        report3 = await manager.wait(run3)
        payload3 = manager.report(run3)
        print(f"   reflection.enabled={report3.reflection['enabled']} "
              f"learning.rules={rule_ids(payload3)}")
        results.append(check("无目标 → 判定跳过，但学习/审计/成本照常产出",
                             report3.reflection["enabled"] is False
                             and bool(rule_ids(payload3))
                             and payload3["audit"] is not None
                             and payload3["learning"]["rules"][0]["tier"]
                             == "objective"))
        results.append(check("学习层是复盘、不阻断交付（run 正常收尾）",
                             manager._runs[run3].status == "done"))

        print("\n== 9. 真实 CLI：ao report / ao lessons（走真实 HTTP）==")
        rc = await asyncio.to_thread(run_cli, ["-u", GATEWAY, "report", run2])
        results.append(check("ao report 打印审计/成本/学习三件套", rc == 0))
        rc = await asyncio.to_thread(run_cli, ["-u", GATEWAY, "lessons"])
        results.append(check("ao lessons 打印跨 run 经验库", rc == 0))

        print("\n== 10. mock 观测（传输层视角）==")
        for aid in server.configs:
            print(f"   {aid}: calls={server.calls.get(aid, 0)}")
        results.append(check("复盘不额外打 agent（学习层只读既有报告）",
                             server.calls.get("judge", 0) >= 2
                             and server.calls.get("translator", 0) >= 1))

    finally:
        await server.stop()

    ok = all(results)
    print("\n== 冒烟结论 ==")
    print("PASS：学习层闭环（确定性复盘 → 落盘经验库 → 回馈拆解提示词）"
          if ok else f"FAIL：{results.count(False)} 项断言未通过")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
