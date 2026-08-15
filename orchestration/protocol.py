"""消息沟通协议（Prompt-as-Protocol）：模板定义 + 请求构造 + 渲染拼接。

对应 docs/messaging-protocol.md。协议的唯一事实来源是模板文本——
格式演进 = 改这里的常量 + 升版本号，agent 零变更（P4）。

模板分两档（协议 §7.8）：完整版（默认）/ 简化版（低能力模型）。
"""
from __future__ import annotations

import json
from typing import Any, Optional

PROTOCOL_VERSION = "v1.1"

# ---------------------------------------------------------------------------
# 完整版模板：全量字段说明 + few-shot 正/反例（协议 §3 + §7.4）
# ---------------------------------------------------------------------------
PROTOCOL_PROMPT_FULL = """# Agent 执行协议 v1.1

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
"""

# ---------------------------------------------------------------------------
# 简化版模板：低能力模型只要求最小可用 JSON，其余字段框架补默认（协议 §7.8）
# ---------------------------------------------------------------------------
PROTOCOL_PROMPT_SIMPLE = """# Agent 执行协议 v1.1（简化版）

你只做一件事：接收请求，返回一个 JSON 对象。

用户消息中包含一个 JSON 请求。你的响应必须严格是：

{"success": true 或 false, "output": 结果, "error": null 或 {"code":"原因","message":"说明"}}

- 请求是任务执行时：output 放任务结果
- 请求是信息收集时：output 按问题逐条回答 {"q1":"...","q2":"..."}
- 无法完成时：success=false，error 说明原因

只输出这个 JSON，不要其他内容。
"""

# 模板档位映射
TEMPLATES = {
    "full": PROTOCOL_PROMPT_FULL,
    "simple": PROTOCOL_PROMPT_SIMPLE,
}


# ---------------------------------------------------------------------------
# 请求构造
# ---------------------------------------------------------------------------

def build_task_request(
    request_id: str,
    task_id: str,
    task_desc: str = "",
    inputs: Optional[dict] = None,
    output_schema: Optional[dict] = None,
    constraints: Optional[dict] = None,
    action: str = "run",
) -> dict:
    """构造 task_request（action=run / cancel 变体，协议 §4.1）。"""
    req: dict[str, Any] = {
        "type": "task_request",
        "action": action,
        "request_id": request_id,
        "task_id": task_id,
    }
    if action == "run":
        req["task_desc"] = task_desc
        if inputs is not None:
            req["inputs"] = inputs
        if output_schema is not None:
            req["output_schema"] = output_schema
        if constraints is not None:
            req["constraints"] = constraints
    return req


def build_info_request(
    request_id: str,
    scope: str,
    questions: list[str],
) -> dict:
    """构造 info_request（协议 §4.2），用于资源统计 / 能力收集。"""
    return {
        "type": "info_request",
        "request_id": request_id,
        "scope": scope,
        "questions": questions,
    }


# ---------------------------------------------------------------------------
# 渲染拼接（协议 §7.6 注入面隔离：模板 / REQUEST / INPUT 分段）
# ---------------------------------------------------------------------------

def render(
    request: dict,
    template_mode: str = "full",
    inputs: Optional[dict] = None,
) -> str:
    """拼接模板 + 请求 + 上游数据，分隔标记降低协议与业务数据误读。"""
    prompt = TEMPLATES.get(template_mode, PROTOCOL_PROMPT_FULL)
    parts = [prompt, "=== REQUEST ===", json.dumps(request, ensure_ascii=False)]
    if inputs is not None:
        parts.append("=== INPUT ===")
        parts.append(json.dumps(inputs, ensure_ascii=False))
    return "\n".join(parts)
