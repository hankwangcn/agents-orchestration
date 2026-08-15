# Agent 消息沟通协议 v1.1 — 提示词即协议（Prompt-as-Protocol）

> 版本：v1.1（模板内嵌 few-shot 正/反示例，见 §7.4）
> 日期：2026-08-15
> 定位：框架与 agent 之间**唯一**的沟通方式
> 关联：架构文档 §4（数据模型）、§5（失败处理）

---

## 1. 设计原则

| # | 原则 | 说明 |
|---|---|---|
| P1 | **框架永远主动** | 发起方只有框架；agent 是被动响应方，永不主动向框架发消息 |
| P2 | **协议载体 = 提示词** | 协议不是 SDK / 接口代码，而是**格式化提示词模板**，随请求一并注入 |
| P3 | **agent 零适配** | agent 只需"接收请求 → 返回结果"，不为编排系统做任何优化、适配或改变 |
| P4 | **演进 = 改模板** | 格式 / 内容更新只改提示词模板，agent 侧零变更 |
| P5 | **结构即契约** | 所有请求、响应均为 JSON 内嵌于提示词；响应统一符合结果契约 Result |

> 本质：LLM agent 的"接口"就是自然语言，提示词是它唯一认识的协议。框架不要求 agent 集成任何东西——**格式想怎么改就怎么改，代价只是改一段模板文本**。

---

## 2. 消息类型

两个方向、两类请求、统一响应：

| 方向 | 类型 | 形态 | 用途 |
|---|---|---|---|
| 框架 → agent | `task_request` | `action=run` | 派发任务执行 |
| 框架 → agent | `task_request` | `action=cancel` | 取消已派发任务（best-effort，见架构 §5.3） |
| 框架 → agent | `info_request` | `scope=capability/resource/constraint` | 收集 agent 能力、资源、约束信息 |
| agent → 框架 | `response` | 复用 Result 契约 | 所有请求的统一响应 |

> 取消不是第三类请求——它是任务请求的 `action` 变体，保持"agent 只接收两类请求"的表述成立。

---

## 3. 提示词模板（协议的唯一定义）

```text
# Agent 执行协议 v1.1

你是任务执行 agent。你只做一件事：接收请求，返回结构化结果。
你不需要了解编排系统的任何内部机制，也不需要为任何系统做适配。

## 你能接收的请求

请求以 JSON 形式出现在用户消息中，共两种：

### 1. 任务请求 task_request

{
  "type": "task_request",
  "action": "run",                  // run=执行任务；cancel=停止已接收的任务
  "request_id": "唯一请求ID",        // 响应时必须原样带回
  "task_id": "任务ID",
  "task_desc": "要完成的任务描述",   // action=run 必填
  "inputs": { },                    // 任务输入（来自上游任务的结果）
  "output_schema": { },             // 期望的输出结构（简化的 JSON Schema 描述）
  "constraints": {
    "timeout_seconds": 300,
    "budget_usd": 1.2,
    "side_effects": "none"          // none | external_api | file_write
  }
}

### 2. 信息请求 info_request

{
  "type": "info_request",
  "request_id": "唯一请求ID",
  "scope": "capability",            // capability=能力 | resource=资源 | constraint=约束
  "questions": ["问题1", "问题2"]
}

## 你必须如何响应

所有请求统一返回一个 JSON 对象（只输出这一个 JSON，放在代码块内，不要其他内容）：

{
  "request_id": "唯一请求ID",        // 必须原样带回请求里的 request_id
  "task_id": "任务ID",              // 必须原样带回请求里的 task_id
  "success": true,
  "output": { },
  "error": null,                    // 失败时: { "code": "reason", "message": "说明" }
  "usage": { "tokens_in": 0, "tokens_out": 0, "cost": 0.0 },
  "duration_ms": 0,
  "side_effect_report": ""          // 执行了外部副作用时如实说明
}

> **usage / duration_ms 字段说明（2026-08-15 冒烟实测修正）**：
> 真实 LLM 不会自报 token / 成本 / 耗时（实测 DeepSeek 输出全 0）。这两个字段的
> **真实值由适配器层从 API 响应捕获并回填**（`AgentAdapter._post_process` 钩子，
> 见 adapters/deepseek.py），agent 自报仅作兜底、且以 API 真实值为准。简化版模板下
> 缺失时（=0）审计按"未知"处理，不误判超支。

- 任务请求：output 按 output_schema 给出结果
- 信息请求：output 按 questions 逐条回答，如 { "q1": "...", "q2": "..." }
- 取消请求：output 为 null，success=true 表示已停止（尽力而为）
- 无法完成：success=false，error 说明原因

## 行为约束（必须遵守）

1. 只响应收到的请求，不主动发起任何消息
2. 只输出上面定义的 JSON，不要输出其他格式或内容
3. 不需要为"编排系统"做任何优化、适配或变更
4. 不确定时如实说明，不要编造

## 响应示例

✅ 正确响应（任务请求）：
{"request_id":"req_0001","task_id":"task_001","success":true,"output":{"summary":"..."},"error":null,
 "usage":{"tokens_in":100,"tokens_out":50,"cost":0.02},
 "duration_ms":1234,"side_effect_report":""}

❌ 常见错误 1：request_id 缺失或不是原值（必须原样带回）
{"success":true,"output":{}}
❌ 常见错误 2：字段类型错误（usage.cost 必须是数字，不能是字符串）
{"request_id":"req_0001","task_id":"task_001","success":true,"output":{},"usage":{"cost":"0.02"}}
❌ 常见错误 3：成功响应夹带说明文字（只输出 JSON）
{"request_id":"req_0001","task_id":"task_001","success":true,"output":{},"说明":"我完成了"}
```

> 此模板即协议全部内容。框架侧将其作为 `PROTOCOL_PROMPT_FULL` 常量维护（另有简化版 `PROTOCOL_PROMPT_SIMPLE`，档位由框架按 agent model 配置，见 §7.8），每次请求 = 模板 + 具体请求 JSON 拼接后发给 agent。

---

## 4. 字段定义

### 4.1 task_request

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| type | string | ✅ | 固定 `"task_request"` |
| action | enum | ✅ | `run` / `cancel` |
| request_id | string | ✅ | 请求唯一 ID，响应原样带回，用于对账 |
| task_id | string | ✅ | 任务 ID（`cancel` 时用于定位目标任务） |
| task_desc | string | run 时 | 任务描述 |
| inputs | object | run 时 | 任务输入，来自上游任务 output |
| output_schema | object | run 时 | 期望输出结构（简化 Schema） |
| constraints | object | 可选 | `timeout_seconds` / `budget_usd` / `side_effects` |

### 4.2 info_request

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| type | string | ✅ | 固定 `"info_request"` |
| request_id | string | ✅ | 同上 |
| scope | enum | ✅ | `capability`（能做什么）/ `resource`（可用模型、并发、配额）/ `constraint`（限制条件） |
| questions | string[] | ✅ | 具体问题清单，逐条回答 |

### 4.3 response（复用结果契约 Result）

字段与架构文档 §4.2 一致，补充语义：

| 字段 | 语义差异 |
|---|---|
| request_id | **必须**原样带回请求的 request_id（§7.7 对账）；简化版可缺失，框架补值 |
| task_id | 任务 ID，原样带回；info_request 下可为空串 |
| output | run → 按 output_schema；info → `{q1:.., q2:..}` 逐条回答；cancel → null |
| success | false 时 error 必填；框架据此决定重试或失败传播 |
| side_effect_report | 声明外部副作用（外部 API / 文件写入），供审计与剪枝判断 |

---

## 5. 时序示例

### 5.1 任务执行（正常路径）

```text
框架 → agent：
[协议模板（§3）]
用户消息: {"type":"task_request","action":"run","request_id":"req_0001",
  "task_id":"task_003","task_desc":"总结本季度销售数据",
  "inputs":{"sales_data":"/data/q3.csv"},
  "output_schema":{"summary":"string","top_trends":["string"]},
  "constraints":{"timeout_seconds":300,"budget_usd":1.2,"side_effects":"none"}}

agent → 框架：
{"request_id":"req_0001","task_id":"task_003","success":true,"output":{"summary":"...","top_trends":["...","..."]},
 "usage":{"tokens_in":1200,"tokens_out":800,"cost":0.42},"duration_ms":12345,
 "side_effect_report":""}
```

### 5.2 信息收集（资源统计场景）

```text
框架 → agent：
[协议模板]
用户消息: {"type":"info_request","request_id":"req_0002","scope":"capability",
  "questions":["你能处理哪些类型的任务？","你依赖哪些工具或外部系统？"]}

agent → 框架：
{"success":true,"output":{"q1":"数据分析、文档撰写、代码审查",
 "q2":"依赖 DeepSeek API，无外部系统"},
 "usage":{"tokens_in":300,"tokens_out":150,"cost":0.05},"duration_ms":2100,
 "side_effect_report":""}
```

### 5.3 取消（best-effort）

```text
框架 → agent：
[协议模板]
用户消息: {"type":"task_request","action":"cancel","request_id":"req_0003",
  "task_id":"task_003"}

agent → 框架：
{"success":true,"output":null,"error":null,
 "usage":{"tokens_in":500,"tokens_out":50,"cost":0.02},"duration_ms":800,
 "side_effect_report":""}
```

> 若 agent 未响应取消（已跑完 / 无法中断），框架直接丢弃其晚到结果——取消是调度层动作，不强制（架构 §5.3）。

---

## 6. 版本管理

| 规则 | 说明 |
|---|---|
| 版本号 | 模板内声明 `Agent 执行协议 v1.1`，随模板一起下发 |
| 更新流程 | 改模板文本 → 升版本号 → **新请求用新模板**；在途请求不受影响 |
| agent 侧 | 无需感知版本变化——它只是按提示词响应 |
| 存储 | 模板作为框架代码常量 / 独立文件管理，与框架版本同步 |

> 协议演进成本 = 改一段文本。这正是 P4 的价值：**格式更新不再需要任何 agent 配合**。

---

## 7. 稳定性兜底：解耦在协议层，稳定在解析层

> 核心原则：agent 侧保持零适配（P3），**所有稳定性风险由框架解析层吸收**。
> 协议牺牲的稳定性大头（格式漂移、语义漂移）不需要 agent 配合，全部在框架侧补回。

### 7.1 风险清单与兜底矩阵

| # | 风险 | 兜底措施 | 所在层 |
|---|---|---|---|
| R1 | **格式漂移**：字段缺失 / 类型错 / 转义坏 / 幻觉加字段 | 双层校验 + 解析失败重试 + few-shot 示例（§7.2~7.4） | 解析层 |
| R2 | **语义漂移**：capability 误报、side_effects 乱填、同一模板不可复现 | 交叉校验 + 计入审计（§7.5） | 治理层 |
| R3 | **协议与业务数据同通道**：业务内容被 agent 误读为协议指令（间接注入面） | 分隔标记 + 解析时丢弃非 JSON 内容（§7.6） | 解析层 |
| R4 | **可观测性弱**：日志 / 对账 / 成本核算依赖事后解析 | `request_id` 强制回带 + 结构化日志（§7.7） | 解析层 |
| R5 | **低能力模型静默降级**：收到完整版模板理解不了 | 模板复杂度分档（§7.8） | 接入层 |

### 7.2 双层校验（R1 主防线）

| 层 | 职责 | 手段 |
|---|---|---|
| 第一层：提取 | 从响应中取出 JSON 对象 | 容忍代码块包裹、前后杂文；提取后丢弃其余内容 |
| 第二层：校验 | 验证结构完整合法 | JSON Schema 严格校验：必填字段、类型、枚举值、嵌套结构、`request_id` 与请求一致 |

任何一层不过 → 走 §7.3 重试，不计入任务执行重试次数。

### 7.3 解析失败重试（R1 第二道防线）

```
响应校验失败 → 自动重试 1 次（携带修正提示）→ 仍失败 → 判定任务失败，进入失败传播（架构 §5）
```

- 修正提示：**"上次输出不符合要求（原因），这是正确示例：…"**——附正确响应样例，不解释协议
- 与任务执行重试（架构 §5.1）分离计数：解析重试只消耗框架侧资源，不消耗任务重试次数

### 7.4 few-shot 正/反示例（R1 根治）

> 给示例比给指令稳一个量级，且成本为零。示例是模板的一部分，随模板升级。

模板 v1.1 已内嵌（见 §3 末段）：

```text
## 响应示例

✅ 正确响应（任务请求）：
{"success":true,"output":{"summary":"..."},"error":null,
 "usage":{"tokens_in":100,"tokens_out":50,"cost":0.02},
 "duration_ms":1234,"side_effect_report":""}

❌ 常见错误 1：output 缺失（必须给，不能省略）
{"success":true}
❌ 常见错误 2：字段类型错误（usage.cost 必须是数字）
{"success":true,"output":{},"usage":{"cost":"0.02"}}
❌ 常见错误 3：成功响应夹带说明文字（只输出 JSON）
{"success":true,"output":{},"说明":"我完成了"}
```

### 7.5 语义交叉校验（R2）

| 校验项 | 规则 | 不一致处理 |
|---|---|---|
| capability 声明 vs 实际任务结果 | 声明"能做 X"的任务多次失败 | 记入审计，标记 agent 可信度下降 |
| side_effects 声明 vs side_effect_report | 声明 `none` 但报告非空 / 报告缺失 | 记入审计，作为剪枝判断修正信号 |
| 同类任务输出结构一致性 | 同 output_schema 不同输出形态 | 记入审计，触发模板示例更新 |

> 交叉校验是框架侧比对，不要求 agent 感知——它只是"按请求返回结果"，判定权在框架。

### 7.6 注入面隔离（R3）

```
拼接格式（框架侧组装，模板 + 请求 + 上游数据）：
[协议模板（§3）]
=== REQUEST ===
{"type":"task_request", ...}
=== INPUT ===
{上游任务结果}
```

- 分隔标记让 agent 明确区分"协议"与"业务数据"，降低误读
- 解析层只提取响应中的 JSON，丢弃一切非 JSON 内容——即使业务数据被污染也不影响协议

### 7.7 可观测性（R4）

- `request_id` 强制回带：响应缺失或与请求不符 → 直接判定失败（不进入语义校验）
- 每次请求/响应/解析结果（含重试与修正）全部落结构化日志
- 对账、成本核算、审计追溯不依赖解析自然语言内容

### 7.8 模板复杂度分档（R5）

| 档位 | 适用 | 内容 |
|---|---|---|
| 完整版（默认） | 能力较强的模型 | §3 全量模板 + few-shot 正/反例 + 完整字段说明 |
| 简化版 | 低能力模型 | 只要求返回最小可用 JSON：`{"success":bool, "output":…, "error":…}`；其余字段由框架补默认值（usage=0、duration_ms=0、side_effect_report=""） |

- 档位是**框架侧注册配置**（按 agent 的 model 决定下发哪个模板），agent 无感知
- 简化版意味着该 agent 的成本 / 用量数据缺失——审计按未知处理，不误判

### 7.9 唯一硬约束与工程要求

- **唯一硬约束**：agent 输出的 JSON 有效性（指令遵循能力）。这是 LLM 的基本能力，不要求 agent 做任何适配，但也没有任何兜底能替代它
- **工程要求**：JSON 解析层（提取 + Schema 校验 + 重试）必须是框架内**最严格、测试最全**的模块，没有之一——它是整个协议稳定性的地基

---

## 8. 边界与约束

1. **响应解析失败**（非 JSON / 缺 `success` 等关键字段）→ 框架侧先自动重试一次（§7.3，携带修正提示）→ 仍失败则判定任务失败，进入任务级重试（架构 §5.1）。解析重试与任务执行重试**分离计数**，不消耗任务重试次数。
2. **info_request 是框架唯一的主动询问手段**：用于资源统计（agent 能力声明）与审计（补充证据）。
3. **取消是尽力而为**：不保证 agent 一定停止，晚到结果直接丢弃。
4. **副作用责任在 agent**：结果契约的 `side_effect_report` 是剪枝 / 审计判断的唯一依据，agent 须如实声明。
5. **协议不包含 agent → 框架的主动消息**：agent 有任何异常只能通过响应的 `success=false` + `error` 表达——框架不监听、不轮询。
6. **传输层默认 OpenAI 兼容 HTTP 端点**：本协议只定义"说什么"（模板 + 请求 JSON）；"怎么送"由适配器决定。默认形态是 agent 暴露 OpenAI 兼容的 chat completions HTTP 端点（事实标准，OpenAI / DeepSeek / vLLM / Ollama / 自建 agent 均可导出）——兼容端点即配置接入（`base_url + api_key + model`），框架零代码；不兼容的自建系统才需要写 adapter（实现 `_call_llm` 一个方法），协议装配 / 双层校验 / 重试 / 成本回填由基类复用。协议内容与传输层解耦：换传输（gRPC / 消息队列 / 本地进程）只动 adapter 一个点，模板与请求 JSON 不变（架构文档 §3.3）。
