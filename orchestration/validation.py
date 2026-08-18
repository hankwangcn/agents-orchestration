"""双层校验解析组件（消息协议 §7）：提取 → Schema 校验 → 解析重试。

工程要求（协议 §7.9）：本模块必须是框架内**最严格、测试最全**的模块——
它是整个协议稳定性的地基。所有稳定性风险（R1 格式漂移 / R4 可观测性）
在此吸收，agent 侧保持零适配。
"""
from __future__ import annotations

import json
import re
from typing import Any, Awaitable, Callable, Optional

from .models import ErrorInfo, Result, Usage


class ResponseValidationError(Exception):
    """响应校验失败（格式漂移），reason 为中文原因，可直接用于修正提示（§7.3）。"""

    def __init__(self, reason: str, raw: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.raw = raw


# 正确响应示例：修正提示（§7.3）与 few-shot 使用
RESPONSE_EXAMPLE = {
    "request_id": "req_0001",
    "task_id": "task_001",
    "success": True,
    "output": {"summary": "..."},
    "error": None,
    "usage": {"tokens_in": 100, "tokens_out": 50, "cost": 0.02},
    "duration_ms": 1234,
    "side_effect_report": "",
}


# ---------------------------------------------------------------------------
# 第一层：提取（协议 §7.2 / §7.6）
# ---------------------------------------------------------------------------

def extract_json(text: str) -> Any:
    """从响应文本提取 JSON 对象。

    容忍 markdown 代码块包裹与前后杂文（§7.6：解析时丢弃非 JSON 内容）。
    返回解析后的对象；无法提取时抛 ResponseValidationError。
    """
    text = text.strip()
    if not text:
        raise ResponseValidationError("响应为空", text)

    # 去掉 markdown 代码块围栏（```json ... ``` / ``` ... ```）
    fence = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.S)
    if fence:
        text = fence.group(1).strip()

    # 定位第一个顶层 {...}，按括号配平截取（容忍前后杂文）
    start = text.find("{")
    if start == -1:
        raise ResponseValidationError("响应中未找到 JSON 对象", text)

    depth = 0
    in_str = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError as e:
                    raise ResponseValidationError(
                        f"JSON 语法错误: {e.msg}", candidate
                    )
    raise ResponseValidationError("JSON 对象不完整（未闭合）", text)


# ---------------------------------------------------------------------------
# 第二层：Schema 校验（协议 §7.2 / §7.7）
# ---------------------------------------------------------------------------

def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


def validate_result(
    obj: Any,
    *,
    request_id: Optional[str] = None,
    task_id: Optional[str] = None,
    template_mode: str = "full",
    response_kind: str = "run",
) -> Result:
    """把解析出的对象校验为 Result 契约。

    完整版（§7.7）：request_id 必填且与请求一致，缺失/不符直接判失败。
    简化版（§7.8）：只校验最小可用字段，其余补默认值。
    """
    if not _is_dict(obj):
        raise ResponseValidationError("响应不是 JSON 对象", str(obj))

    # ---- 简化版：最小可用校验，其余补默认 ----
    if template_mode == "simple":
        return _validate_simple(obj, request_id=request_id, task_id=task_id)

    # ---- 完整版：严格校验 ----
    # R4 可观测性：request_id 强制回带（§7.7）
    if request_id is not None:
        got_rid = obj.get("request_id")
        if not isinstance(got_rid, str) or got_rid != request_id:
            raise ResponseValidationError(
                f"request_id 缺失或与请求不符（期望 {request_id}，收到 {got_rid!r}）",
                json.dumps(obj, ensure_ascii=False),
            )

    # task_id 必填
    got_tid = obj.get("task_id")
    if not isinstance(got_tid, str):
        raise ResponseValidationError("task_id 缺失或不是字符串")
    if task_id is not None and got_tid != task_id:
        raise ResponseValidationError(
            f"task_id 与请求不符（期望 {task_id}，收到 {got_tid}）"
        )

    # success 必填且为布尔
    success = obj.get("success")
    if not isinstance(success, bool):
        raise ResponseValidationError("success 缺失或不是布尔值")

    # error 结构
    error = obj.get("error")
    error_info: Optional[ErrorInfo] = None
    if success is False:
        if not _is_dict(error) or not isinstance(error.get("code"), str):
            raise ResponseValidationError("失败响应缺少 error.code")
        error_info = ErrorInfo(code=error["code"], message=str(error.get("message", "")))
    elif error is not None:
        # success=true 且 error 非空：语义矛盾，判失败
        raise ResponseValidationError("success=true 但 error 非空，语义矛盾")

    # run/info 成功必须有 output（cancel 允许 null）
    if success and response_kind in ("run", "info") and "output" not in obj:
        raise ResponseValidationError("成功响应缺少 output 字段")

    # usage：若存在则类型校验，字段缺省补 0（严格：不接受字符串数字）
    usage = obj.get("usage", {})
    if not _is_dict(usage):
        raise ResponseValidationError("usage 必须是对象")
    tokens_in = usage.get("tokens_in", 0)
    tokens_out = usage.get("tokens_out", 0)
    cost = usage.get("cost", 0.0)
    if (
        not isinstance(tokens_in, int)
        or isinstance(tokens_in, bool)
        or not isinstance(tokens_out, int)
        or isinstance(tokens_out, bool)
    ):
        raise ResponseValidationError("usage.tokens_in/tokens_out 必须是整数")
    if not isinstance(cost, (int, float)) or isinstance(cost, bool):
        raise ResponseValidationError("usage.cost 必须是数字")
    usage_obj = Usage(tokens_in=tokens_in, tokens_out=tokens_out, cost=float(cost))

    duration_ms = obj.get("duration_ms", 0)
    if not isinstance(duration_ms, (int, float)):
        raise ResponseValidationError("duration_ms 必须是数字")

    side_effect_report = obj.get("side_effect_report", "")
    if not isinstance(side_effect_report, str):
        raise ResponseValidationError("side_effect_report 必须是字符串")

    return Result(
        request_id=obj.get("request_id"),
        task_id=got_tid,
        success=success,
        output=obj.get("output"),
        error=error_info,
        usage=usage_obj,
        duration_ms=int(duration_ms),
        side_effect_report=side_effect_report,
    )


def _validate_simple(
    obj: dict,
    *,
    request_id: Optional[str],
    task_id: Optional[str],
) -> Result:
    """简化版：只校验最小可用字段（§7.8），其余由框架补默认值。"""
    success = obj.get("success")
    if not isinstance(success, bool):
        raise ResponseValidationError("success 缺失或不是布尔值")

    error = obj.get("error")
    error_info: Optional[ErrorInfo] = None
    if success is False:
        if not _is_dict(error) or not isinstance(error.get("code"), str):
            raise ResponseValidationError("失败响应缺少 error.code")
        error_info = ErrorInfo(code=error["code"], message=str(error.get("message", "")))

    return Result(
        request_id=request_id,  # 简化版不回带，框架按请求方补值
        task_id=task_id or obj.get("task_id") or "",
        success=success,
        output=obj.get("output"),
        error=error_info,
    )


# ---------------------------------------------------------------------------
# 解析重试（协议 §7.3）：与任务执行重试分离计数
# ---------------------------------------------------------------------------

def _correction_prompt(first: ResponseValidationError) -> str:
    """构造修正提示（§7.3）：原因 + 正确示例。同步/异步解析共用。"""
    return (
        f"你上一次的响应不符合要求：{first.reason}。\n"
        "请严格按照下面的正确示例重新输出 JSON，不要附加任何其他内容：\n"
        f"{json.dumps(RESPONSE_EXAMPLE, ensure_ascii=False, indent=2)}"
    )


def parse_result(
    response_text: str,
    *,
    request_id: Optional[str] = None,
    task_id: Optional[str] = None,
    template_mode: str = "full",
    response_kind: str = "run",
    retry_fn: Optional[Callable[[str], str]] = None,
) -> Result:
    """提取 → 校验 → 失败带修正提示自动重试一次（§7.3）。

    retry_fn(correction) 由调用方提供：携带修正提示再调一次模型。
    重试仍失败则抛 ResponseValidationError——由调用方判定任务失败，
    进入失败传播（架构 §5.1）。解析重试不消耗任务执行重试次数。
    """
    try:
        obj = extract_json(response_text)
        return validate_result(
            obj,
            request_id=request_id,
            task_id=task_id,
            template_mode=template_mode,
            response_kind=response_kind,
        )
    except ResponseValidationError as first:
        if retry_fn is None:
            raise
        retry_text = retry_fn(_correction_prompt(first))
        obj = extract_json(retry_text)  # 重试仍失败则抛原始错误
        return validate_result(
            obj,
            request_id=request_id,
            task_id=task_id,
            template_mode=template_mode,
            response_kind=response_kind,
        )


async def aparse_result(
    response_text: str,
    *,
    request_id: Optional[str] = None,
    task_id: Optional[str] = None,
    template_mode: str = "full",
    response_kind: str = "run",
    aretry_fn: Optional[Callable[[str], Awaitable[str]]] = None,
) -> Result:
    """异步版解析（阶段四并发调度使用）：逻辑与 parse_result 完全一致。"""
    try:
        obj = extract_json(response_text)
        return validate_result(
            obj,
            request_id=request_id,
            task_id=task_id,
            template_mode=template_mode,
            response_kind=response_kind,
        )
    except ResponseValidationError as first:
        if aretry_fn is None:
            raise
        retry_text = await aretry_fn(_correction_prompt(first))
        obj = extract_json(retry_text)
        return validate_result(
            obj,
            request_id=request_id,
            task_id=task_id,
            template_mode=template_mode,
            response_kind=response_kind,
        )
