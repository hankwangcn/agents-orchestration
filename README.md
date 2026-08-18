<div align="center">

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Language](https://img.shields.io/github/languages/top/hankwangcn/agents-orchestration?color=3572A5)](https://github.com/hankwangcn/agents-orchestration)
[![Tests](https://img.shields.io/badge/tests-168%2F168%20passing-brightgreen)](tests/)

**结果导向的 Agent 编排框架（Result-driven Orchestration）——框架统一调度，只管理"任务 → 结果"，不监控 agent 内部状态。**

</div>

---

## 特性总览

| | |
|---|---|
| **结果导向编排** | 框架只管理任务到结果的结果契约，不监控 agent 内部运行状态；批处理式"任务 → 结果"执行，过程交给 agent 自己 |
| **断点持久化** | 调度状态（DAG / 任务状态 / 分配 / 剪枝）事件驱动落盘 SQLite；进程崩溃后无缝恢复——纯产出任务自动重派，副作用任务置 `INTERRUPTED` 等人工确认，已终态任务复用结果与成本 |
| **提示词即协议** | 与 agent 的唯一沟通方式是格式化提示词模板 + JSON 请求（`task_request` / `info_request`）；agent 零适配，协议演进仅需修改模板文本 |
| **零成本接入** | agent 暴露 OpenAI 兼容 chat completions 端点即可接入，注册表配置即完成，框架零代码；不兼容的自建系统仅需实现一个 `_call_llm` 方法 |
| **资源统计与分配** | 通过 `info_request` 采集 agent 能力 / 资源 / 约束声明入库；三级分配策略：精确匹配 → 能力匹配 → 降级兜底，全程留痕 |
| **失败处理** | 自动重试 → 失败传播 → 反向可达性剪枝（死任务消除）；并发场景下竞态安全（先冻结派发，再逐级取消，晚到结果丢弃） |
| **并发调度** | asyncio 事件驱动并发派发；per-agent 并发上限与速率配额强制执行；单实例可并发运行多个 DAG，状态隔离 |
| **治理闭环** | 结果审计（对账 / 分配审计 / 剪枝审计 / 语义交叉校验）+ 成本核算（含失败成本与剪枝沉没成本）+ 自我学习规则提取 |
| **可观测性** | 结构化日志（key=value）+ 指标聚合，直接供给审计器与学习引擎 |
| **API 网关** | 框架以系统形态对外服务：提交 DAG / 查询进度 / 获取报告 / 取消运行 / agent 档案快照 |

---

## 架构概览

六层架构（详见 [架构文档](docs/architecture.md) 与 [架构总览图](docs/architecture-diagram.html)）：

```
接入层 → 规划层 → 调度层 → 执行层 → 治理层 → 学习层
(API 网关)  (拆解)  (注册表/调度器)  (适配器/Agent)  (审计/成本)  (规则提取)
```

核心设计原则：**解耦在协议面，稳定在解析组件**——与 agent 的耦合被压缩到"一个兼容端点"，稳定性兜底（双层校验、解析重试、注入面隔离）全部由框架解析组件吸收。

---

## 安装

```bash
pip install -e ".[dev,gateway]"   # 开发安装（含测试与网关依赖）
```

| 依赖组 | 用途 | 包含 |
|---|---|---|
| `dev` | 运行测试 | `pytest` |
| `gateway` | API 网关（FastAPI 服务） | `fastapi`, `uvicorn` |

> 最小运行环境仅需 `pydantic>=2.0` 与 `openai>=1.0`（`pip install .` 即可）。

## 配置凭据

`DeepSeekAdapter` 默认从环境变量读取 API key，无需写入代码：

```bash
export DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxx
```

也可在构造时显式传入：`DeepSeekAdapter(api_key="...", base_url="...")`。`base_url` 可指向任意 OpenAI 兼容端点——这是接入自有 agent 的入口（见下文"Agent 接入"）。

---

## 快速开始

### 1. 最小端到端示例（同步调度）

完整可运行，覆盖整条链路：注册 → 能力采集 → 拆解 → 调度：

```python
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.decomposer import Decomposer
from orchestration.registry import AgentRegistry
from orchestration.scheduler import Scheduler

# (1) 注册 agent 并采集声明
#     info_request 询问 capability / resource / constraint 三组问题，
#     agent 的答案解析为结构化声明入库（能力标签 / 并发上限 / 速率 / 预算）
adapter = DeepSeekAdapter()                    # api_key 默认读 $DEEPSEEK_API_KEY
registry = AgentRegistry()
registry.register(adapter, agent_id="general")
registry.collect()                             # 采集三类声明，单点失败不中断

# (2) 任务拆解：目标 → 依赖 DAG
#     拆解 LLM 产出任务集合、依赖关系与各任务的能力需求（required_capabilities）
decomposer = Decomposer(llm_call=adapter.chat) # 复用同一适配器
dag = decomposer.decompose("分析本周销售数据并生成周报")

# (3) 调度执行：拓扑派发 + 三级分配 + 失败剪枝
scheduler = Scheduler(registry=registry)
report = scheduler.run(dag)                    # ScheduleReport：结果 / 分配 / 剪枝明细

print(report.final_status)                     # success | failed | cancelled
print(report.total_cost)
for a in report.assignments:                   # 每个任务的 agent 归属与匹配类型
    print(a.task_id, a.agent_id, a.match_type) # exact | capability | degraded
```

### 2. 异步并发调度

同一 DAG 下并发派发全部就绪任务（阶段四），事件驱动推进，并在任务失败时执行竞态安全的剪枝与取消：

```python
import asyncio

from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.metrics import MetricsCollector
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler

async def main() -> None:
    registry = AgentRegistry()
    registry.register(DeepSeekAdapter(), agent_id="general")
    registry.collect()   # 并发上限 / 速率配额采集后由调度器强制执行

    scheduler = AsyncScheduler(registry=registry, metrics=MetricsCollector())
    report = await scheduler.run(dag, run_id="run_1")
    print(report.final_status, report.total_cost)

asyncio.run(main())
```

### 3. 治理层消费

调度产生的 `ScheduleReport` 可直接供给治理三件套——审计、成本、学习：

```python
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine

audit = Auditor().audit(report)               # verdict: ok / warning / critical
cost = CostAccountant().account(report)       # 按 agent / 匹配类型归集 + 预算判定
rules = LearningEngine().learn(audit, cost)   # 六类规则：REC / FP / DEG / CAP / BUG / PRU
```

### 4. API 网关（系统形态）

将框架以 HTTP 服务对外提供（提交 DAG 立即返回 `run_id`，后台执行，支持多 run 并发）：

```python
# my_entry.py
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.api.gateway import create_app
from orchestration.registry import AgentRegistry
from orchestration.state_store import SqliteStateStore

registry = AgentRegistry()
registry.register(DeepSeekAdapter(), agent_id="general")
registry.collect()

# 传入 state_store 启用断点持久化（调度状态事件驱动落盘 SQLite）
app, manager = create_app(registry, state_store=SqliteStateStore("state.db"))
```

```bash
uvicorn my_entry:app --port 8000
```

```bash
# 提交 DAG → 立即返回 run_id
curl -X POST http://localhost:8000/api/runs \
  -H 'Content-Type: application/json' \
  -d '{"dag": {"tasks": {"a": {"id": "a", "desc": "..."}}}}'

# 查询进度 / 获取收尾报告
curl http://localhost:8000/api/runs/{run_id}
curl http://localhost:8000/api/runs/{run_id}/report
```

### 4.1 断点恢复（进程崩溃后继续）

启用 `state_store` 后，进程崩溃（断电 / 宕机）时调度状态已逐事件落盘，重启后无缝续跑：

```bash
# 恢复 run：RUNNING 任务按 A+B 策略处理——
#   无副作用 → 自动重派；声明副作用 → 置 INTERRUPTED 等待人工
curl -X POST http://localhost:8000/api/runs/{run_id}/resume

# 人工确认中断任务（仅 INTERRUPTED 可 resolve）
curl -X POST http://localhost:8000/api/runs/{run_id}/tasks/{task_id}/resolve \
  -H 'Content-Type: application/json' \
  -d '{"action": "complete", "result": {"task_id": "...", "success": true, "output": {...}}}'
#   action: complete（附人工核实的结果契约）| cancel | retry
#   retry 后需再次 POST /resume 继续调度
```

恢复语义：已终态任务（成功/失败/取消）的结果与成本直接复用，不重跑不重计费；崩溃瞬间已发出的请求无法撤回，纯产出任务可能重复执行一次（A 策略的固有代价）。

### 5. 真实模型端到端冒烟

```bash
export DEEPSEEK_API_KEY=sk-...
.venv/bin/python scripts/smoke_deepseek.py
```

覆盖链路：`info_request` 能力采集 → 异步并发调度（2 并行 + 1 汇总）→ 双层校验解析 → 审计 / 成本 / 学习，验证真实 LLM 格式漂移下的解析兜底与真实 token / 耗时 / 成本回填。

---

## Agent 接入

**默认形态：agent 暴露 OpenAI 兼容的 chat completions 端点**——这是当前模型服务的事实标准（OpenAI / DeepSeek / vLLM / Ollama 等均可导出），框架以 HTTP 客户端身份主动调用，agent 零适配：

```python
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.registry import AgentRegistry

registry = AgentRegistry()
registry.register(
    DeepSeekAdapter(
        model="deepseek-chat",
        base_url="https://customer.internal/v1",  # 指向客户 agent 的端点
        api_key="...",                            # 或环境变量
    ),
    agent_id="customer_agent_1",
)
```

- **兼容端点** → 配置即接入，框架零代码
- **不兼容的自建系统** → 继承 `AgentAdapter` 实现 `_call_llm`（约 20 行），协议装配、双层校验、重试、成本回填由基类统一复用
- 协议内容（模板 + 请求 JSON）对所有接入方式相同，与传输层解耦

---

## 设计文档

- [架构文档](docs/architecture.md) — 分层架构、失败处理（剪枝 / 竞态）、数据模型
- [消息沟通协议](docs/messaging-protocol.md) — 协议模板、稳定性兜底（§7）
- [架构总览图（HTML）](docs/architecture-diagram.html)

---

## 项目结构

```
agents-orchestration/
├── orchestration/
│   ├── models.py            # 数据模型：Task / Result 契约 / DAG（图算法）/ ScheduleReport
│   ├── protocol.py          # 消息协议模板（完整/简化版）+ 请求构造 + 渲染
│   ├── validation.py        # 解析组件：双层校验（提取 → Schema 校验 → 重试）
│   ├── decomposer.py        # 任务拆解：目标 → 依赖 DAG（含能力需求声明）
│   ├── registry.py          # Agent 注册表：info_request 采集 / 三级分配 / 故障摘除
│   ├── scheduler.py         # 同步调度器：拓扑派发 + 失败传播 + 剪枝
│   ├── scheduler_async.py   # 异步并发调度器：并发派发 + 竞态处理 + 资源限制
│   ├── metrics.py           # 可观测性：指标聚合 + 结构化日志
│   ├── audit.py             # 结果审计：对账 / 分配审计 / 剪枝审计 / 交叉校验
│   ├── cost.py              # 成本核算：按 agent / 匹配类型归集 + 预算判定
│   ├── learning.py          # 自我学习：启发式规则提取（六类）
│   ├── state_store.py       # 断点持久化：SQLite StateStore（事件驱动落盘）
│   ├── api/gateway.py       # API 网关：RunManager + FastAPI 端点
│   └── adapters/            # Agent 适配器（base 抽象 同步/异步 + DeepSeek）
├── scripts/
│   ├── smoke_deepseek.py    # 真实模型端到端冒烟
│   └── smoke_resume.py      # 断点恢复冒烟（崩溃 → 恢复 → 续跑）
├── tests/                   # 168 项测试（解析组件 / 剪枝 / 调度 / 治理 / 并发 / 网关 / 断点）
├── docs/                    # 架构文档 / 消息协议 / 架构图
└── pyproject.toml
```

---

## 测试

```bash
pip install -e ".[dev,gateway]"
pytest        # 168/168 全绿
```

测试覆盖重点：解析组件（最严格模块，32 项）、剪枝算法（反向可达性，多 final 语义）、并发竞态、速率限制、治理三件套、网关生命周期、断点恢复（A+B 策略）。

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
