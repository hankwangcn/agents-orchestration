# agents-orchestration

结果导向 Agent 编排框架（Result-driven Orchestration）。

- **结果导向**：框架只管理"任务 → 结果"，不监控 agent 内部运行状态
- **提示词即协议**：与 agent 的唯一沟通方式是格式化提示词模板，agent 零适配
- **资源协调**：info_request 采集 agent 能力/资源/约束声明，三级分配（exact → capability → degraded）
- **失败处理**：自动重试 → 失败传播 → 死任务剪枝（反向可达性剪枝）
- **治理闭环**：结果审计 + 成本核算 + 自我学习规则提取

## 模块

| 模块 | 职责 |
|---|---|
| `orchestration/models.py` | Task / Result（结果契约）/ Assignment / DAG（含图算法）/ ScheduleReport |
| `orchestration/protocol.py` | 消息协议模板（完整/简化版）+ 请求构造 + 渲染 |
| `orchestration/validation.py` | 双层校验解析层（提取 → Schema 校验 → 解析重试，同步/异步） |
| `orchestration/decomposer.py` | 任务拆解：LLM 生成依赖 DAG（含能力需求声明） |
| `orchestration/registry.py` | Agent 注册表：info_request 采集 + 三级分配 + 摘除 |
| `orchestration/scheduler.py` | 同步调度器：拓扑派发 + 任务分配 + 失败传播 + 剪枝 |
| `orchestration/scheduler_async.py` | 异步并发调度器：并发派发 + 竞态处理 + 资源限制（阶段四） |
| `orchestration/metrics.py` | 可观测性：指标聚合 + 结构化日志（阶段四） |
| `orchestration/audit.py` | 结果审计器：对账 / 分配审计 / 剪枝审计 / 交叉校验 |
| `orchestration/cost.py` | 成本核算：按 agent/匹配类型归集 + 预算判定 |
| `orchestration/learning.py` | 自我学习：启发式规则提取（六类） |
| `orchestration/api/gateway.py` | API 网关：RunManager + FastAPI 端点（阶段四） |
| `orchestration/adapters/` | Agent 适配器（base 抽象 同步/异步 + DeepSeek AsyncOpenAI） |

## 快速开始

### 同步调度（串行）

```python
from orchestration.decomposer import Decomposer
from orchestration.registry import AgentRegistry
from orchestration.scheduler import Scheduler
from orchestration.adapters.deepseek import DeepSeekAdapter

adapter = DeepSeekAdapter()                      # api_key 默认读 $DEEPSEEK_API_KEY
registry = AgentRegistry()
registry.register(adapter, agent_id="general")     # 可注册多个 agent 实例
registry.collect()                                 # info_request 采集能力/资源/约束声明

decomposer = Decomposer(llm_call=adapter.chat)     # 拆解引擎复用同一适配器
scheduler = Scheduler(registry=registry)

dag = decomposer.decompose("你的目标...")
report = scheduler.run(dag)
print(report.final_status, report.total_cost)
print([(a.task_id, a.agent_id, a.match_type) for a in report.assignments])
```

### 异步并发调度（阶段四）

```python
import asyncio
from orchestration.registry import AgentRegistry
from orchestration.scheduler_async import AsyncScheduler
from orchestration.adapters.deepseek import DeepSeekAdapter
from orchestration.metrics import MetricsCollector

registry = AgentRegistry()
registry.register(DeepSeekAdapter(), agent_id="general")
registry.collect()   # max_concurrency / rate_limit 采集后由调度器强制执行

scheduler = AsyncScheduler(registry=registry, metrics=MetricsCollector())
report = asyncio.run(scheduler.run(dag, run_id="run_1"))
```

### 治理与学习（阶段二：消费 ScheduleReport）

```python
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine

report = scheduler.run(dag)                     # 或 AsyncScheduler.run(dag)
audit = Auditor().audit(report)                 # verdict: ok / warning / critical
cost = CostAccountant().account(report)         # 按 agent/匹配类型归集 + 预算判定
rules = LearningEngine().learn(report)          # 六类规则：REC/FP/DEG/CAP/BUG/PRU
```

### API 网关（阶段四）

```python
from orchestration.api.gateway import create_app

app, manager = create_app(registry)             # 构造 (app, RunManager)
# 启动：uvicorn 需要模块级 app 变量，在入口模块中保留上面的 `app` 再执行：
# uvicorn my_entry:app --port 8000
# 或编程式：uvicorn.run(app, port=8000)
```

```bash
curl -X POST http://localhost:8000/api/runs \
  -H 'Content-Type: application/json' \
  -d '{"dag": {"tasks": {"a": {"id": "a", "desc": "a"}}}}'
curl http://localhost:8000/api/runs/{run_id}/report
```

### 真实模型端到端冒烟（DeepSeek）

```bash
export DEEPSEEK_API_KEY=sk-...
.venv/bin/python scripts/smoke_deepseek.py
```

覆盖链路：info_request 能力采集 → 异步并发调度（2 并行 + 1 汇总）→ 双层校验解析
→ 审计 / 成本 / 学习。验证真实 LLM 格式漂移下的解析兜底与 usage 回填
（真实 token / 耗时 / 成本，`_post_process` 钩子）。

## 设计文档

- [架构文档](docs/architecture.md)
- [消息沟通协议](docs/messaging-protocol.md)
- [架构总览图（HTML）](docs/architecture-diagram.html)

## 开发

```bash
pip install -e ".[dev,gateway]"   # gateway 提供 fastapi/uvicorn：跑 API 网关与全量测试（含 test_gateway）必需
pytest                            # 151/151 全绿
```
