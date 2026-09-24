<div align="center">

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hankwangcn/agents-orchestration/blob/main/LICENSE)
[![Language](https://img.shields.io/github/languages/top/hankwangcn/agents-orchestration?color=3572A5)](https://github.com/hankwangcn/agents-orchestration)
[![Tests](https://img.shields.io/badge/tests-468%2F468%20passing-brightgreen)](https://github.com/hankwangcn/agents-orchestration/tree/main/tests)

**结果导向的 Agent 编排框架（Result-driven Orchestration）——框架统一调度，只管理"任务 → 结果"，不监控 agent 内部状态。**

</div>

---

## 特性总览

| | |
|---|---|
| **结果导向编排** | 框架只管理任务到结果的结果契约，不监控 agent 内部运行状态；批处理式"任务 → 结果"执行，过程交给 agent 自己 |
| **断点持久化** | 调度状态（DAG / 任务状态 / 分配 / 剪枝）事件驱动落盘 SQLite；进程崩溃后无缝恢复——纯产出任务自动重派，副作用任务置 `INTERRUPTED` 等人工确认，已终态任务复用结果与成本 |
| **反思/判定（独立 judge）** | 运行级语义判定：**最终交付 × 原始目标** → 达成/未达成 + score + 缺口。基准只能是用户原始目标（子任务描述是框架自产，拿来当基准即自证循环），子任务结果仅作证据；判定者 = 注册表中声明 `judge` 能力的独立 agent（可异构模型），判定就是一次普通 `task_request`（消息类型零新增、agent 零变更）；**advisory**——只写报告 + 喂学习（JUD-1/JUD-2），不改状态、不阻断、不自动重派 |
| **目标即入口** | 一句自然语言目标 → 规划层拆解为任务 DAG →（可选）直接提交执行：网关 `POST /api/decompose` / CLI `ao decompose --submit`，"目标 → 结果"一条链；拆解是框架内部 LLM 调用，**不走消息协议** |
| **依赖分析独立** | `DAG → DependencyGraph` 纯结构视图：成环检测 / 拓扑序 / **拓扑分层（并行前沿）** / 最大并行宽度 / 反向可达（剪枝判据）；`/api/decompose` 直接回 `analysis`（分层 + 并行宽度），规划层这一环有真实产物 |
| **提示词即协议** | 与 agent 的唯一沟通方式是格式化提示词模板 + JSON 请求（`task_request` / `info_request`）；agent 零适配，协议演进仅需修改模板文本 |
| **零成本接入** | agent 暴露 OpenAI 兼容 chat completions 端点即可接入（DeepSeek / vLLM / Ollama / 自研），注册表配置即完成；同进程 Python agent 免 HTTP（进程内适配器直调本地函数）；不兼容的自建系统仅需实现一个发起调用的扩展点 |
| **配置驱动注册** | 注册表可由配置文件批量注册 N 个 agent——只写"agent 在哪、叫什么模型"，能力 / 并发 / 预算声明由 `info_request` 自动问出；`api_key_env` 从环境变量取密钥 |
| **CLI 运维入口** | 网关瘦客户端 `ao`（仅标准库 urllib）：目标拆解 / 提交 DAG / 进度 / 报告 / 指标 / 取消 / 断点恢复 / 人工 resolve / agent 档案 / **学习层经验库**（`ao lessons`），一条命令完成运维与人工出口 |
| **资源统计与分配** | 通过 `info_request` 采集 agent 能力 / 限制声明入库（**规划层资源统计器**）；三级分配策略：精确匹配 → 能力匹配 → 降级兜底，全程留痕，失败摘除（**调度层资源协调器**）——按六层架构物理分文件，注册表为组合门面 |
| **失败处理** | 自动重试 → 失败传播 → 反向可达性剪枝（死任务消除）；并发场景下竞态安全（先冻结派发，再逐级取消，晚到结果丢弃） |
| **框架侧超时封顶** | 单次尝试超过 `required_resources.timeout` 由框架强制中断（不依赖 agent 履约），agent 挂死不再永久占住并发槽；超时汇入既有重试/剪枝链路 |
| **并发调度** | asyncio 事件驱动并发派发；per-agent 并发上限与速率配额强制执行；单实例可并发运行多个 DAG，状态隔离 |
| **治理闭环** | 结果审计（对账 / 分配审计 / 剪枝审计 / 语义交叉校验——**只读、可复跑、确定性**）+ 反思/判定（目标达成度，非确定、成本单列、与审计分离留痕）+ 成本核算（含失败成本与剪枝沉没成本），三件套在 run 收尾时自动产出并进报告 |
| **学习层闭环** | 确定性复盘 → 规则提取（证据强度分级：`objective` 确定性事实 / `judgment` LLM 判定，**禁止同级呈现**）→ **跨 run 经验库落盘**（SQLite，同 rule 累计命中次数与贡献 run 数）→ **回馈拆解提示词**：拆解引擎的经验注入接口在固定指令与用户目标之间插入指导块（注册表实测的可用模型 / 能力标签 + **跨 run 复现**的返工事实，每条带数值支撑）；`GET /api/lessons` / `ao lessons` 查看经验库 |
| **运行存档（归档）** | 运行存档 = **过程事件流**（状态变更逐条落盘，可回放时间线）+ **终态报告**（治理/学习产物挂回），落在持久化底座、单 run 不可变——**唯一真源**；报告**可读回**（进程重启后仍在）、run 可**枚举**（`GET /api/runs` / `ao runs`），崩溃后 / 换进程后仍可查阅 |
| **接入层 Web 页面** | 服务端渲染的 Web 页面（**同源、零构建、零 CORS**）：`GET /` 运行列表 + `GET /runs/{id}` 运行详情——**完整业务完成过程**（过程时间线 + 任务与结果 + 分配留痕 + 失败传播 + 治理 + 学习规则）。人读版是运行存档的**读时投影**（按需渲染、确定性、可复跑），不落盘成第二份真相 |
| **叙述摘要（非确定）** | `POST /api/runs/{id}/narrative` / `ao narrate`：LLM 产出的一段人读总结——**显式触发、单独留痕、标注来源**，**不默认生成、不混入确定性报告**（同"判定 vs 审计"的分离口径） |
| **可观测性** | 结构化日志（key=value）+ 指标聚合，直接供给审计器与学习引擎 |
| **API 网关** | 框架以系统形态对外服务：目标拆解 / 提交 DAG / 查询进度 / 获取报告 / 运行枚举 / 人读投影 / 取消运行 / agent 档案快照 / **跨 run 经验库**（`GET /api/lessons`） |

---

## 架构概览

六层架构（详见 [架构文档](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/architecture.md) 与 [架构总览图](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/architecture-diagram.html)）：

```
接入层 → 规划层 → 调度层 → 执行层 → 治理层 → 学习层
(API 网关/Web/CLI) (拆解) (注册表/调度器) (适配器/Agent) (审计/成本/判定) (复盘→经验库→回馈拆解)
```

核心设计原则：**解耦在协议面，稳定在解析组件**——与 agent 的耦合被压缩到"一个兼容端点"，稳定性兜底（双层校验、解析重试、注入面隔离）全部由框架解析组件吸收。

**运行存档**（过程事件流 + 终态报告）落在持久化底座，是唯一真源；Web 页面 / `ao view` / 叙述摘要都是它的**读时投影**（按需渲染）。

---

## 安装

```bash
pip install -e ".[dev,gateway]"   # 开发安装（含测试与网关依赖）
```

| 依赖组 | 用途 | 包含 |
|---|---|---|
| `dev` | 运行测试 | `pytest` |
| `gateway` | API 网关（FastAPI 服务） | `fastapi`, `uvicorn` |

> 最小运行环境仅需 `pydantic>=2.0`、`openai>=1.0`、`pyyaml>=6.0`（`pip install .` 即可；YAML 注册仅在用到配置文件批量注册时需要）。

## 配置凭据

DeepSeek 适配器默认从环境变量读取 API key，无需写入代码：

```bash
export DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxx
```

也可在构造适配器时显式传入 `api_key` / `base_url`（构造参数名同下）。`base_url` 可指向任意 OpenAI 兼容端点——这是接入自有 agent 的入口（见下文"Agent 接入"）。

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

调度产生的运行报告可直接供给治理三件套——审计、成本、学习：

```python
from orchestration.audit import Auditor
from orchestration.cost import CostAccountant
from orchestration.learning import LearningEngine

audit = Auditor().audit(report)               # verdict: ok / warning / critical
cost = CostAccountant().account(report)       # 按 agent / 匹配类型归集 + 预算判定
rules = LearningEngine().learn(audit, cost)   # 八类规则：REC / FP / DEG / CAP / BUG / PRU / JUD
```

走网关时三件套在 run 收尾**自动产出**（`GET /api/runs/{id}/report` 的 `audit` / `cost` / `learning`），
规则落盘为跨 run 经验库并回馈拆解提示词：

```python
from orchestration.decomposer import make_default_decomposer
from orchestration.lessons import PromptAdvisor

advisor = PromptAdvisor(registry, state_store)          # 经验库 + 注册表客观事实
decomposer = make_default_decomposer(guidance_provider=advisor.guidance)
decomposer.decompose(goal)      # 提示词 = 固定指令 → 〔历史经验指导块〕 → 用户目标
```

```bash
ao lessons                      # 经验库视图：命中次数 / 贡献 run 数 / 证据强度分级
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
# 提交 DAG → 立即返回 run_id（可选带 goal：收尾时按原始目标判定交付）
curl -X POST http://localhost:8000/api/runs \
  -H 'Content-Type: application/json' \
  -d '{"dag": {"tasks": {"a": {"id": "a", "desc": "..."}}}, "goal": "调研两款耳机并出对比报告"}'

# 查询进度 / 获取收尾报告
curl http://localhost:8000/api/runs/{run_id}
curl http://localhost:8000/api/runs/{run_id}/report

# 一句话目标 → DAG（规划层拆解；submit=true 则拆解后直接提交）
curl -X POST http://localhost:8000/api/decompose \
  -H 'Content-Type: application/json' \
  -d '{"goal": "调研两款降噪耳机的价格并生成对比报告", "submit": true}'
# → {"goal": "...", "status": "submitted", "dag": {...}, "run_id": "..."}
```

> 拆解引擎由应用构造时注入（默认复用 DeepSeek，key 读 `$DEEPSEEK_API_KEY`）；未注入时该端点返回 400，其余功能不受影响。`ao serve` 会自动装配（`--decompose-model` / `--no-decompose` 可调）。
>
> **反思/判定**同样由应用构造时注入（`ao serve` 自动装配，`--no-reflect` 可关）：提交带 `goal` 的 run 收尾后按原始目标判定交付，结论进报告的 `reflection` 字段。想让判定独立可信，注册一个声明 `judge` 能力的 agent（可用异构模型）——能力由 `info_request` 采集，无需额外配置；没有这类 agent 时降级自判并在报告中标记 `independent=false`。判定是 advisory，不改状态、不阻断交付。

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

### 4.2 CLI 与 N 个 agent 配置（`ao` 命令 + `agents.yaml`）

安装包自带 `ao` 命令（网关瘦客户端，仅标准库 urllib，无需额外依赖）。**CLI 不绕过网关直连调度器**——所有命令都发 HTTP 到网关，框架作为系统的对外门不变：

```bash
export AO_GATEWAY=http://127.0.0.1:8000        # 或 -u 指定；默认 localhost:8000

ao serve --config agents.yaml --state-store run.db   # 1) 启动网关（批量注册 + 断点持久化）
ao agents                                       # 2) 看 agent 档案（能力/并发/预算已采集入库）
ao decompose --goal "调研两款耳机价格并生成对比报告"   # 3) 目标 → DAG（打印 DAG JSON，供审阅/落盘）
ao decompose --goal "..." --submit              #    或拆解后直接提交（--run-id 可自定义）
ao submit dag.json --run-id r1                  # 3') 提交已有 DAG
ao wait r1 --timeout 300                        # 4) 等结束并打印报告
ao metrics r1                                   # 5) 运行指标（成本/并发峰值/成功率）
ao resolve r1 t5 --action complete --result '{"task_id":"t5","success":true,"output":{...}}'
                                                # 6) 人工出口：确认 INTERRUPTED 任务
ao resume r1                                    # 7) 断点恢复（需 state_store）
ao lessons                                      # 8) 学习层经验库（跨 run 规则聚合，需 state_store）
```

**交互模式**：`ao` 不带子命令（或 `ao shell`）进入 REPL——逐行执行任意子命令，错误不退出会话，tab 补全 + 历史持久化（`~/.ao_history`）：

```text
ao> agents                                  # 查 agent 档案
ao> submit                                  # 不带文件 → 引导式提问构建 DAG
  任务 t1 描述: 把下面句子翻成英文: 你好世界
  任务 t1 依赖 (逗号分隔, 可空):
  任务 t2 描述: 总结上一步的翻译
  任务 t2 依赖 (逗号分隔, 可空): t1
  任务 t3 描述:                           # 回车结束收集
共 2 个任务：
  t1: 把下面句子翻成英文: 你好世界
  t2: 总结上一步的翻译  deps: t1
提交? [y/N]: y
run_id: b4b65411222a
ao> wait                                    # submit 后 run_id 自动记忆，免重复敲
ao> report
ao> resolve                                 # 缺 task_id/action 时逐项引导
  task_id（run b4b65411222a，如 t1）: t2
  action (complete/cancel/retry): complete
  人工核实结果契约 JSON: {"task_id":"t2","success":true}
ao> exit
```

交互要点：`submit` 成功即记忆 run_id，后续 `status/report/metrics/cancel/resume/wait/resolve` 均可省略；显式传参优先于会话记忆；`-u/--url` 在 REPL 入口指定后会话内复用。

N 个 agent 只需一份配置文件，零代码注册——**只写"agent 在哪、叫什么模型"**，能力 / 并发上限 / 预算声明一条都不用配（那是 `info_request` 问出来的）：

```yaml
# agents.yaml
max_consecutive_failures: 3      # 可选：连续失败摘除阈值
default_agent: translator        # 可选：降级目标（缺省=第一个注册的）
agents:
  - agent_id: translator
    base_url: http://agent-translator:8000/v1   # 任意 OpenAI 兼容端点
    model: qwen2.5-7b
    api_key_env: TRANSLATOR_KEY                  # 或 api_key: sk-xxx
  - agent_id: coder-a                            # 同 model 多实例 → 自动分流
    base_url: http://agent-coder-1:8000/v1
    model: qwen2.5-coder
  - agent_id: coder-b
    base_url: http://agent-coder-2:8000/v1
    model: qwen2.5-coder
  - agent_id: analyst
    base_url: http://agent-analyst:8000/v1
    model: deepseek-chat
```

程序内也可用同一份配置：`registry = AgentRegistry.from_config("agents.yaml")`；自定义传输（如进程内函数）传 `adapter_factory` 覆盖默认工厂即可。

### 4.3 运行存档与 Web 页面（展示完整完成过程）

启用 `state_store` 后，每个 run 的**过程事件流**（状态变更逐条落盘）与**终态报告**即成为运行存档——框架的**唯一真源**。人读版是它的**读时投影**（按需渲染），Gateway 直接以服务端渲染的 HTML 呈现，**同源、零构建、零 CORS**：

```bash
# 浏览器打开（与网关同源）
http://127.0.0.1:8000/                 # 运行列表（run 枚举）
http://127.0.0.1:8000/runs/{run_id}    # 运行详情：过程时间线 + 任务 + 治理 + 学习
```

```bash
ao runs                       # 运行枚举（跨进程可发现已有 run）
ao view <run_id>              # 运行存档人读视图（完整过程 + 治理 + 学习，按需渲染）
ao narrate <run_id>           # 显式生成叙述摘要（LLM，非确定；需叙述引擎）
```

对应的只读 JSON 端点：`GET /api/runs`（枚举）、`GET /api/runs/{id}/view`（**确定性投影**，可复跑）、`POST /api/runs/{id}/narrative`（**非确定**，LLM 产出、单独留痕、不默认生成）。

> **三分离**：运行存档（过程 + 终态）＝不可变真源；人读投影＝确定性渲染；叙述摘要＝非确定 LLM 产出。前两者可复跑、零成本，后者显式触发、标注来源、不混入。见 [examples/web_run_demo.html](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/demo/web_run_demo.html)。

### 5. 真实模型端到端冒烟

```bash
export DEEPSEEK_API_KEY=sk-...
.venv/bin/python scripts/smoke_deepseek.py
```

覆盖链路：`info_request` 能力采集 → 异步并发调度（2 并行 + 1 汇总）→ 双层校验解析 → 审计 / 成本 / 学习，验证真实 LLM 格式漂移下的解析兜底与真实 token / 耗时 / 成本回填。

### 6. 多 agent 全流程冒烟（无需 API key / 真实 LLM）

```bash
.venv/bin/python scripts/smoke_multiagent.py
```

框架以客户端身份调用**本地 mock agent 服务**（`scripts/mock_agents.py`，OpenAI 兼容 chat completions 端点），传输层与真实 agent 完全一致（协议装配 → HTTP → 解析），差异仅在业务应答为脚本化：

- 4 个角色 agent（translator / coder / analyst / flaky）+ 11 任务 DAG
- 覆盖：三级分配留痕（exact / capability / degraded）、能力不覆盖 risk 标记、解析组件杂文兜底重试、任务重试耗尽 → 连续失败摘除 → 降级回退、反向可达剪枝（独立交付分支不误杀）、per-agent 并发上限真实生效（HTTP 并发峰值 ≤ 声明值）、usage 真实回填、审计 / 成本 / 学习
- mock 支持确定性故障注入（HTTP 500 / 杂文 / 业务失败），独立运行：`.venv/bin/python scripts/mock_agents.py`
- 可视化报告（DAG 执行全景图 / 三级分配留痕 / 失败传播 / 治理三栏）：`.venv/bin/python scripts/smoke_multiagent.py --visual`，示例报告见 [docs/demo/smoke_multiagent_report.html](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/demo/smoke_multiagent_report.html)

### 7. 目标 → DAG → 结果 冒烟（真实拆解 + mock 执行 + 真实 CLI）

```bash
export DEEPSEEK_API_KEY=sk-...       # 拆解引擎用真实 LLM；agent 侧仍走本地 mock
.venv/bin/python scripts/smoke_decompose.py
```

覆盖链路：真实 DeepSeek 拆解目标 → 网关 `POST /api/decompose`（`--submit`）→ mock agents（真实 HTTP）执行 → 报告；CLI 侧直接调 `orchestration.cli.main`（无 mock，真实 HTTP 打到本地网关）。同时验证执行层超时：挂死 agent（30s 才返回）+ 任务声明 `timeout=1` → 框架侧中断、`error.code=timeout`、最终槽位释放（同 agent 仍可派发）。14/14 断言通过。

### 8. 反思/判定冒烟（真实 HTTP 判定 agent，无需 API key）

```bash
.venv/bin/python scripts/smoke_reflection.py
```

覆盖链路：真实 HTTP 采集 `judge` 能力 → 提交带 `goal` 的 run（mock agents 真实 HTTP 执行）→ 收尾判定（适配器真实 HTTP 打独立判定角色：协议装配 → output_schema 强校验 → usage 真实回填）→ 结论进报告 + 学习层 `JUD-1` 触发。同时验证 advisory 边界：判定不改任务状态、成本单列不计入任务总成本、无 `goal` 的 run 跳过判定（`skipped_reason=no_goal`）。12/12 断言通过。

### 9. 学习层闭环冒烟（无需 API key）

```bash
.venv/bin/python scripts/smoke_learning.py
```

覆盖链路：真实网关 + 真实 CLI；一次 run 同时产出三类**客观**返工信号（失败错误码复现 → FP、降级分配 → DEG、剪枝 → PRU）与判定结论（JUD-1，`judgment` 分级）→ 规则落盘 SQLite 经验库 → **单 run 未达复现门槛故不进提示词** → 第二次 run 后经验库累积（命中 2 次 / 贡献 2 个 run）→ 指导块注入拆解提示词（带"既往 2 次运行命中 2 次"数值支撑），并校验提示词结构稳定（前缀=固定指令、结尾=用户目标）与客观/判定分节呈现；末尾以真实 CLI 走真实 HTTP 打印 `ao report`（审计/成本/学习三件套 + 判定 + 分配 + 剪枝）与 `ao lessons`（跨 run 经验库）。20/20 断言通过。

### 10. 运行存档与 Web 页面冒烟（无需 API key）

```bash
.venv/bin/python scripts/smoke_archive.py                    # 仅校验
.venv/bin/python scripts/smoke_archive.py --demo-dir out/    # 另写出 Web 演示页
```

覆盖链路：真实网关 + 真实 CLI；一次 run（含失败 / 剪枝 / 降级）→ **过程事件流**逐条落盘（首尾 run_started / run_finished，含派发 / 完成 / 剪枝）→ **运行枚举**（`/api/runs`、`ao runs`）→ **人读投影**（`/api/runs/{id}/view`，同一份存档两次调用**逐字节一致** = 确定性可复跑）→ **叙述摘要**（默认空 → 显式 `POST /narrative` → 标注 `source=llm` / `deterministic=false` → 单独留痕并入视图独立字段）→ **Web 页面**（`/` 含 run 链接、`/runs/{id}` 含过程时间线 / 任务 / 学习规则，未知 run 走 HTML 错误页）→ 真实 CLI `ao runs` / `ao view`。示例页见 [docs/demo/web_run_demo.html](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/demo/web_run_demo.html)。

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
- **不兼容的自建系统** → 继承适配器基类实现一个发起调用的扩展点（约 20 行），协议装配、双层校验、重试、成本回填由基类统一复用
- **同进程 Python agent（函数 / 类 / 脚本）** → 进程内适配器直调本地函数，免 HTTP 传输（序列化 + 网络往返只为调一个本地函数是纯开销）。协议约束（输出合法 JSON）由解析组件照样强制，稳定性兜底一个不少：

```python
from orchestration.adapters.inprocess import InProcessAdapter
from orchestration.registry import AgentRegistry

def my_agent(messages: list[dict]) -> str:
    """接收协议装配后的消息，返回 JSON 字符串（或 dict，自动序列化）。"""
    ...  # 业务逻辑，输出 Result 契约

registry = AgentRegistry()
registry.register(InProcessAdapter(my_agent, model="local-helper"), agent_id="local")
```

- **N 个 agent 批量注册** → 注册表可由 YAML / JSON / 字典配置批量构建（见 4.2），程序内等价用法：

```python
from orchestration.registry import AgentRegistry

registry = AgentRegistry.from_config("agents.yaml")   # 默认工厂：DeepSeekAdapter
# 自定义传输（进程内函数等）：
registry = AgentRegistry.from_config(cfg, adapter_factory=lambda e: InProcessAdapter(fn, model=e["model"]))
```

- 协议内容（模板 + 请求 JSON）对所有接入方式相同，与传输层解耦

---

## 设计文档

- [架构文档](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/architecture.md) — 分层架构、失败处理（剪枝 / 竞态）、数据模型、运行存档
- [消息沟通协议](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/messaging-protocol.md) — 协议模板、稳定性兜底（§7）
- [架构总览图（HTML）](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/architecture-diagram.html)
- [运行详情页示例（HTML）](https://github.com/hankwangcn/agents-orchestration/blob/main/docs/demo/web_run_demo.html) — 接入层 Web 页面（完整过程）单文件示例

---

## 项目结构

```
agents-orchestration/
├── orchestration/
│   ├── models.py            # 数据模型：Task / Result 契约 / DAG（调度期状态操作）/ ScheduleReport
│   ├── dependency.py        # 依赖分析（规划层）：DAG → DependencyGraph（拓扑/并行前沿/可达/校验）
│   ├── agent_pool.py        # 资源统计器（规划层）：注册 / info_request 采集 / 声明解析 / TTL 刷新
│   ├── allocator.py         # 资源协调器（调度层）：三级分配 / 多实例轮询 / 连续失败摘除
│   ├── protocol.py          # 消息协议模板（完整/简化版）+ 请求构造 + 渲染
│   ├── validation.py        # 解析组件：双层校验（提取 → 信封 Schema + output_schema 强校验 → 重试）
│   ├── timeouts.py          # 框架侧 wall-clock 超时原语（任务执行 / info 采集共用）
│   ├── decomposer.py        # 任务拆解：目标 → 依赖 DAG（含能力需求声明）
│   ├── registry.py          # Agent 注册表门面：组合规划层资源统计器 + 调度层资源协调器（零逻辑转发）
│   ├── scheduler.py         # 同步调度器：拓扑派发 + 失败传播 + 剪枝
│   ├── scheduler_async.py   # 异步并发调度器：并发派发 + 竞态处理 + 资源限制
│   ├── metrics.py           # 可观测性：指标聚合 + 结构化日志
│   ├── audit.py             # 结果审计：对账 / 分配审计 / 剪枝审计 / 交叉校验
│   ├── cost.py              # 成本核算：按 agent / 匹配类型归集 + 预算判定
│   ├── reflection.py        # 反思/判定（治理层）：最终交付 × 原始目标 → 判定结论（advisory）
│   ├── learning.py          # 自我学习：启发式规则提取（八类）+ 证据强度分级
│   ├── lessons.py           # 经验库与提示词顾问：跨 run 聚合 + 回馈拆解提示词
│   ├── report_view.py       # 运行存档人读投影：确定性视图 + 文本 / HTML 渲染（接入层 Web / CLI）
│   ├── narrative.py         # 叙述摘要（LLM，非确定）：显式生成、单独留痕
│   ├── state_store.py       # 断点持久化 + 运行存档：SQLite（事件流 / 报告 / 经验库 / 叙述）
│   ├── cli.py               # CLI `ao`：网关瘦客户端（仅标准库 urllib）
│   ├── api/gateway.py       # API 网关：RunManager + FastAPI 端点 + Web 页面（同源）
│   └── adapters/            # Agent 适配器：base（协议装配/双层校验/重试/成本回填）
│                            #   + deepseek（OpenAI 兼容 HTTP）+ inprocess（免 HTTP）
├── scripts/
│   ├── mock_agents.py        # 本地 OpenAI 兼容 mock agent 服务（多角色 + 确定性故障注入）
│   ├── smoke_multiagent.py   # 多 agent 全流程冒烟（真实 HTTP，无需 key）
│   ├── smoke_deepseek.py     # 真实模型端到端冒烟（需 $DEEPSEEK_API_KEY）
│   ├── smoke_decompose.py    # 目标 → DAG → 结果 冒烟（真实拆解 + mock 执行 + 真实 CLI）
│   ├── smoke_reflection.py   # 反思/判定冒烟（目标基准 + 独立 judge + advisory 边界）
│   ├── smoke_learning.py     # 学习层闭环冒烟（复盘 → 经验库落盘 → 回馈拆解提示词）
│   ├── smoke_archive.py      # 运行存档 + Web 页面冒烟（事件流 → 人读投影 → Web / 叙述）
│   └── smoke_resume.py       # 断点恢复冒烟（崩溃 → 恢复 → 续跑）
├── tests/                   # 468 项测试（解析组件 / 拆解 / 剪枝 / 调度 / 治理 / 反思判定 / 学习闭环 / 运行存档 / Web / 并发 / 网关 / 断点 / CLI / 适配器）
├── docs/                    # 架构文档 / 消息协议 / 架构图 / 可视化示例报告 + Web 页面示例
└── pyproject.toml           # 包配置（`ao` 命令入口）
```

---

## 测试

```bash
pip install -e ".[dev,gateway]"
pytest        # 468/468 全绿
```

测试覆盖重点：依赖分析（拓扑分层/并行前沿/可达性/成环与引用校验，23 项）、层职责切分（规划层资源统计器 × 调度层资源协调器）、解析组件（最严格模块，47 项：信封校验 + output_schema 强校验 + 重试兜底）、剪枝算法（反向可达性，多 final 语义）、并发竞态、速率限制、治理三件套、网关生命周期、断点恢复（A+B 策略）、CLI 命令与 payload 构造、交互 shell（run_id 记忆 / 引导式 submit / 错误不退出）、进程内适配器（str/dict 返回、解析重试、免 HTTP 全流程）、配置批量注册（YAML/JSON/环境变量取 key/自定义工厂）、采集侧 wall-clock 超时、学习层闭环（证据分级 / 经验库落盘与跨 run 聚合 / 复现门槛 / 提示词注入与客观-判定分离，43 项）、CLI 学习层出口（报告打印审计/成本/学习 + `ao lessons`，5 项）、运行存档（事件流 / 报告读回 / 运行枚举 / 叙述留痕 / 级联清理 + 底座退化，10 项）、人读投影（确定性 / 结构 / 转义 / 运行中投影 / HTML 渲染，10 项）、网关存档与 Web（过程事件流落盘 / 重启后报告读回 / 运行枚举与投影端点 / Web 页面 / 叙述端点 + 故障映射，9 项）、CLI 存档出口（`ao runs` / `ao view` / `ao narrate`，4 项）。

---

## License

Apache License 2.0 — see [LICENSE](https://github.com/hankwangcn/agents-orchestration/blob/main/LICENSE).
