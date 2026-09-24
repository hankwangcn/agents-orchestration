# Agent 消息沟通协议 v1.2 — 提示词即协议

> 版本：v1.2（模板内嵌少量样本示例，见 §7.4；v1.2 增补执行侧两项声明——副作用任务的幂等责任与目标及契约范围内的取舍裁定权）
> 更新日期：2026-09-24
> 定位：框架与 agent 之间唯一的沟通方式
> 关联：架构文档 §4（数据模型）、§5（失败处理）

---

## 1. 设计原则

| # | 原则 | 说明 |
|---|---|---|
| P1 | 框架永远主动 | 发起方只有框架；agent 为被动响应方，不主动发起消息 |
| P2 | 协议载体为提示词 | 协议不是 SDK 或接口代码，而是格式化提示词模板，随请求一并注入 |
| P3 | agent 零适配 | agent 仅需接收请求并返回结果，不为编排系统做任何优化、适配或改动 |
| P4 | 演进即改模板 | 格式与内容的更新仅修改提示词模板，agent 侧零变更 |
| P5 | 结构即契约 | 所有请求与响应均为内嵌于提示词的 JSON；响应统一符合结果契约 |

> 大模型 agent 的接口即自然语言，提示词是它唯一识别的协议。框架不要求 agent 集成任何组件：格式变更的代价仅为修改一段模板文本。

---

## 2. 消息类型

两个方向、两类请求、统一响应：

| 方向 | 类型 | 形态 | 用途 |
|---|---|---|---|
| 框架 → agent | `task_request` | `action=run` | 派发任务执行 |
| 框架 → agent | `task_request` | `action=cancel` | 取消已派发任务（best-effort，见架构 §5.3） |
| 框架 → agent | `info_request` | `scope=capability/resource/constraint` | 收集 agent 的能力、资源与约束 |
| agent → 框架 | 响应 | 复用结果契约 | 所有请求的统一响应 |

> 取消不是第三类请求，而是任务请求的 `action` 变体，以维持"agent 仅接收两类请求"的表述。

> **边界说明**：规划层的目标拆解（目标 → 任务图）由框架直接调用自身的底层大模型，不经本协议——拆解是框架内部组件，不在"框架 ↔ agent"这条边界上。本协议约束的仅是框架与 agent 通信的这一面。
>
> 与之相对，治理层的反思 / 判定（最终交付 × 原始目标 → 判定结论）即为本协议的一次普通 `task_request`——判定能力同样由信息请求采集（`judge` / `reviewer`），判定 agent 与执行 agent 同构、零变更。因此协议不新增消息类型（无独立的判定请求类型），判定输出复用结果契约，并受 §7.2 输出结构校验与 §7.3 解析重试约束。

---

## 3. 提示词模板（协议的唯一定义）

```text
# Agent 执行协议 v1.2

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

- 任务请求：output 按 output_schema 给出结果
- 信息请求：output 按 questions 逐条回答，如 { "q1": "...", "q2": "..." }
- 取消请求：output 为 null，success=true 表示已停止（尽力而为）
- 无法完成：success=false，error 说明原因

## 行为约束（必须遵守）

1. 只响应收到的请求，不主动发起任何消息
2. 只输出上面定义的 JSON，不要输出其他格式或内容
3. 不需要为"编排系统"做任何优化、适配或变更
4. 不确定时如实说明，不要编造
5. 声明 side_effects 非 none 的任务须自行保证幂等（或自行去重）——同一任务可能被重复下发（失败重试、断点恢复），重复执行不得产生重复副作用
6. 目标与契约（task_desc / inputs / output_schema / constraints）范围内的全部取舍由你自行裁定，含价值性取舍——框架不提供征询通道，不得以待外部决策为由中止执行或返回失败；所作取舍随 output 给出

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

> 上表即协议全部内容。框架侧以完整版与简化版两个模板维护，档位按 agent 的模型配置（见 §7.8）；每次请求为模板与具体请求 JSON 拼接后下发给 agent。

> **`usage` 与 `duration_ms` 说明**：真实大模型不自报 token、成本与耗时（冒烟实测 DeepSeek 输出全为 0）。这两个字段的真实值由执行层适配器在解析完成后从模型 API 响应捕获并回填，agent 自报仅作备用，且以 API 真实值为准。简化版模板下字段缺失（取值为 0）时，成本核算按"未知"处理，不作超支判定。

---

## 4. 字段定义

### 4.1 task_request

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| type | string | 是 | 固定为 `"task_request"` |
| action | enum | 是 | `run` / `cancel` |
| request_id | string | 是 | 请求唯一标识，响应须原样回带，用于对账 |
| task_id | string | 是 | 任务标识（`cancel` 时用于定位目标任务） |
| task_desc | string | run 时必填 | 任务描述 |
| inputs | object | run 时必填 | 任务输入，来自上游任务的产出 |
| output_schema | object | run 时必填 | 期望输出结构（简化 Schema，方言见 §7.2）；框架据此强校验产出 |
| constraints | object | 可选 | `timeout_seconds` / `budget_usd` / `side_effects` |

### 4.2 info_request

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| type | string | 是 | 固定为 `"info_request"` |
| request_id | string | 是 | 同上 |
| scope | enum | 是 | `capability`（能做哪些事）/ `resource`（并发、速率、预算）/ `constraint`（限制条件） |
| questions | string[] | 是 | 问题清单，逐条回答 |

> 框架侧对信息请求同样设墙钟上限（与任务请求的 `timeout` 对称）：采集卡死不能无上限地拖住调用方（运行起始的池级刷新、派发前的决策点校验）。超时按单点采集失败处理——保留该 agent 上次已知画像，不中断其余 agent 的采集。

### 4.3 响应（复用结果契约）

字段与架构文档 §4.2 一致，补充语义如下：

| 字段 | 语义 |
|---|---|
| request_id | 必须原样回带请求的 request_id（§7.7 对账）；简化版可缺失，由框架补值 |
| task_id | 任务标识，原样回带；信息请求下可为空串 |
| output | run 时按 output_schema；info 时为逐条回答对象；cancel 时为 null |
| success | 为 false 时 error 必填；框架据此决定重试或失败传播 |
| side_effect_report | 声明外部副作用（外部 API 调用或文件写入），供审计与剪枝判断 |

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

### 5.2 信息采集

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

> 若 agent 未响应取消（已完成或无法中断），框架直接丢弃其迟到结果——取消是调度层动作，不强制执行（架构 §5.3）。

---

## 6. 版本管理

| 规则 | 说明 |
|---|---|
| 版本号 | 模板内声明 `Agent 执行协议 v1.2`，随模板一并下发 |
| 更新流程 | 修改模板文本 → 升版本号 → 新请求使用新模板；在途请求不受影响 |
| agent 侧 | 无需感知版本变化，仅按提示词响应 |
| 存储 | 模板作为框架代码常量或独立文件管理，与框架版本同步 |

> 协议演进成本即为修改一段文本，这正是 P4 的价值：格式更新不再需要任何 agent 配合。

---

## 7. 稳定性保障：解耦在协议面，稳定在解析组件

> 核心原则：agent 侧保持零适配（P3），所有稳定性风险由框架解析组件吸收。协议在稳定性上的主要代价（格式漂移与语义漂移）无需 agent 配合，全部在框架侧补回。

### 7.1 风险清单与应对矩阵

| # | 风险 | 应对措施 | 所在层 |
|---|---|---|---|
| R1 | 格式漂移：字段缺失、类型错误、转义异常、额外幻觉字段 | 两级校验、解析失败重试、样本示例（§7.2~7.4） | 执行层 |
| R2 | 语义漂移：能力误报、副作用声明失实、同一模板输出不可复现 | 交叉校验并计入审计（§7.5） | 治理层 |
| R3 | 协议与业务数据同通道：业务内容被误读为协议指令 | 分隔标记与解析时丢弃非 JSON 内容（§7.6） | 执行层 |
| R4 | 可观测性弱：日志、对账与成本核算依赖事后解析 | `request_id` 强制回带与结构化日志（§7.7） | 执行层 |
| R5 | 低能力模型静默降级：无法理解完整版模板 | 模板复杂度分档（§7.8） | 接入层 |

### 7.2 两级校验（R1 主防线）

| 层 | 职责 | 手段 |
|---|---|---|
| 第一层：提取 | 从响应中取出 JSON 对象 | 容忍代码块包裹与前后杂文，提取后丢弃其余内容 |
| 第二层：校验 | 验证结构完整合法 | ① 信封严格校验：必填字段、类型、枚举值、`request_id` 与请求一致；② 输出结构强校验（run 请求）：成功响应的产出须符合期望结构 |

任何一层不通过则进入 §7.3 重试，且不计入任务执行重试次数。

**输出结构方言（简化 Schema）**：以"字段 → 类型描述"的映射表达，而非完整 JSON Schema。

```json
{ "summary": "string", "top_trends": ["string"], "level": {"enum": ["low", "high"]} }
```

| spec 形态 | 含义 |
|---|---|
| `"string"` | 类型标识：`string` / `number` / `integer` / `boolean` / `object` / `array` / `null` / `any` |
| `"string\|null"` | 并集（任一命中即可），常用于可空字段 |
| `["string"]` | 数组，元素按唯一元素 spec 校验（可嵌套） |
| `{"lang": "string"}` | 嵌套对象（字段映射递归） |
| `{"enum": [...]}` | 枚举（取值须落在列表内） |

- 列出的字段为必需，多余字段容忍（agent 常附加上下文，不据此判失败）。
- 未知 spec 形态与未知类型标识保守不强制（不因 schema 书写问题误判 agent）。
- schema 省略、简化版模板（§7.8）以及信息请求与取消请求，均不做结构强校验。
- 完整 JSON Schema 节点（`{"type": "object", "properties": {...}}`）整体跳过，结构一致性交由审计层（§7.5）。

> 此前输出结构仅作提示随请求下发，框架侧无强制，结构异常可静默通过；现补为第二层校验的一部分：不符即判失败并进入 §7.3。

### 7.3 解析失败重试（R1 第二道防线）

```
响应校验失败 → 自动重试一次（携带修正提示）→ 仍失败 → 判定任务失败，进入失败传播（架构 §5）
```

- 修正提示为"上次输出不符合要求（原因），以下为正确示例"，附正确响应样例，不解释协议；若失败源于输出结构不符，提示中一并给出期望结构（§7.2），比仅给出通用示例更稳定。
- 与任务执行重试（架构 §5.1）分离计数：解析重试仅消耗框架侧资源，不消耗任务重试次数。

### 7.4 样本示例（R1 根治）

> 给出示例比给出指令在格式稳定性上更有效，且成本为零。示例是模板的一部分，随模板升级。

模板 v1.2 已内嵌（见 §3 末段）：

```text
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

### 7.5 语义交叉校验（R2）

| 校验项 | 规则 | 不一致处理 |
|---|---|---|
| 能力声明与实际结果 | 声明可完成某类任务但该类任务多次失败 | 计入审计，标记该 agent 可信度下降 |
| 副作用声明与实际报告 | 声明 `none` 但报告非空，或报告缺失 | 计入审计，作为剪枝判断的修正信号 |
| 同类任务输出结构一致性 | 同一 output_schema 下输出形态不一致 | 计入审计，触发模板示例更新 |

> 交叉校验由框架侧比对，不要求 agent 感知；agent 仅按请求返回结果，判定权在框架。

### 7.6 注入面隔离（R3）

拼接格式由框架侧组装（模板 + 请求 + 上游数据）：

```
[协议模板（§3）]
=== REQUEST ===
{"type":"task_request", ...}
=== INPUT ===
{上游任务结果}
```

- 分隔标记使 agent 明确区分协议与业务数据，降低误读。
- 解析组件仅提取响应中的 JSON，丢弃一切非 JSON 内容，即使业务数据被污染也不影响协议。

### 7.7 可观测性（R4）

- `request_id` 强制回带：响应缺失或与请求不符即判失败（不进入语义校验）。
- 每次请求、响应与解析结果（含重试与修正）全部落结构化日志。
- 对账、成本核算与审计追溯均不依赖解析自然语言内容。

### 7.8 模板复杂度分档（R5）

| 档位 | 适用 | 内容 |
|---|---|---|
| 完整版（默认） | 能力较强的模型 | §3 全量模板、样本示例与完整字段说明 |
| 简化版 | 低能力模型 | 仅要求返回最小可用 JSON：`{"success":bool, "output":…, "error":…}`，其余字段由框架补默认值（用量为 0、耗时为 0、副作用报告为空串） |

- 档位是框架侧注册配置（按 agent 的模型决定下发哪个模板），agent 无感知。
- 两档模板承载同一套义务声明：副作用任务的幂等责任与目标及契约范围内的取舍裁定权（§8.4 / §8.8），简化版不因字段裁剪而豁免。
- 简化版意味着该 agent 的成本与用量数据缺失，成本核算按未知处理，不作超支判定。

### 7.9 唯一硬约束与工程要求

- **唯一硬约束**：agent 输出的 JSON 有效性（指令遵循能力）。这是大模型的基本能力，不要求 agent 做任何适配，且没有其他容错可替代它。
- **工程要求**：解析组件（提取、Schema 校验、重试）必须是框架内最严格、测试覆盖最全的模块，没有例外——它是整个协议稳定性的基础。

---

## 8. 边界与约束

1. 响应解析失败（非 JSON、缺少 `success` 等关键字段）时，框架侧先自动重试一次（§7.3，携带修正提示），仍失败则判定任务失败并进入任务级重试（架构 §5.1）。解析重试与任务执行重试分离计数，不消耗任务重试次数。
2. 信息请求是框架唯一的主动询问手段，用于资源统计（agent 能力声明）与判定者识别（判定 agent 的能力声明）。
3. 取消为尽力而为，不保证 agent 停止；迟到结果直接丢弃。
4. 副作用责任在 agent：结果契约的 `side_effect_report` 是剪枝与审计判断的唯一依据，agent 须如实声明；声明副作用的任务（`side_effects` 非 none）还须自行保证幂等或自行去重——失败重试与断点恢复均会对同一任务重复下发（架构 §5.1 / §5.5），框架侧不对副作用任务的重复执行设拦截，幂等性由执行侧承担。
5. 协议不包含 agent → 框架的主动消息：agent 的异常只能通过响应的 `success=false` 与 `error` 表达，框架不监听、不轮询。
6. 判定（反思）不是新的协议面：治理层的运行级判定复用 `task_request`（§4.1）——`task_desc` 放置判定指令，`inputs` 放置目标与交付证据，`output_schema` 放置判定结构（`achieved` / `score` / `reasons` / `gaps`）。它不引入新消息类型，不要求 agent 做任何适配；判定结论为 advisory（不改变任务状态、不阻断交付），与框架的确定性审计分离记录。
7. 传输层默认采用 OpenAI 兼容的 HTTP 端点：本协议只定义"说什么"（模板与请求 JSON），"如何传输"由适配器决定。默认形态是 agent 暴露 OpenAI 兼容的 chat completions HTTP 端点（事实标准，OpenAI、DeepSeek、vLLM、Ollama 与自建 agent 均可提供）——兼容端点即配置接入（端点地址、密钥与模型名），框架零代码；不兼容的自建系统才需实现适配器（仅需实现一个发起调用的扩展点），协议装配、两级校验、重试与成本回填由适配器基类统一复用。协议内容与传输层解耦：更换传输方式（gRPC、消息队列、本地进程）仅改动适配器一处，模板与请求 JSON 不变（架构文档 §3.3）。
8. 框架不提供征询通道：目标与契约（`task_desc` / `inputs` / `output_schema` / `constraints`）范围内的全部取舍（含价值性取舍，即答案不能由目标与契约推出、取决于使用方偏好的取舍）由执行侧自行裁定，并由模板显式声明（§3 行为约束第 6 条）。协议为单轮封闭：一次请求对应一次响应，响应即终局，框架不持有任务的执行中间态。因此"等待外部决策"不能表示为执行中间态，只能表示为终局结果（失败及其原因）；框架不承担"取舍是否符合使用方偏好"的判定责任，事后纠偏由运行级判定（advisory）与后继运行承担。
