"""解析组件测试（协议 §7.9 工程要求：本模块测试最全）。

覆盖：提取（杂文/代码块/未闭合/嵌套引号）、Schema 校验（必填/类型/
request_id 一致性/语义矛盾）、解析重试（§7.3）、简化版（§7.8）。
"""
import json

import pytest

from orchestration.validation import (
    RESPONSE_EXAMPLE,
    ResponseValidationError,
    extract_json,
    parse_result,
    validate_result,
)


# ---------------------------------------------------------------------------
# 第一层：提取
# ---------------------------------------------------------------------------

class TestExtractJson:
    def test_pure_json(self):
        assert extract_json('{"a": 1}') == {"a": 1}

    def test_code_block(self):
        text = '```json\n{"success": true}\n```'
        assert extract_json(text) == {"success": True}

    def test_surrounding_prose(self):
        text = '好的，结果如下：\n{"success": true, "output": {"x": 1}}\n希望对你有帮助'
        assert extract_json(text)["output"] == {"x": 1}

    def test_escaped_quotes_in_string(self):
        text = '{"msg": "他说 \\"你好\\"", "n": 1}'
        obj = extract_json(text)
        assert obj["msg"] == '他说 "你好"'

    def test_empty(self):
        with pytest.raises(ResponseValidationError):
            extract_json("   ")

    def test_no_json(self):
        with pytest.raises(ResponseValidationError):
            extract_json("这不是 JSON，没有花括号")

    def test_unclosed_brace(self):
        with pytest.raises(ResponseValidationError):
            extract_json('{"success": true')

    def test_bad_json_inside_braces(self):
        with pytest.raises(ResponseValidationError):
            extract_json('{"success": tru}')

    def test_picks_first_top_level(self):
        text = '先来一个 {"a": 1} 再来一个 {"b": 2}'
        assert extract_json(text) == {"a": 1}


# ---------------------------------------------------------------------------
# 第二层：Schema 校验
# ---------------------------------------------------------------------------

class TestValidateResult:
    def test_valid_full(self):
        obj = {
            "request_id": "req_1",
            "task_id": "t1",
            "success": True,
            "output": {"summary": "ok"},
            "usage": {"tokens_in": 100, "tokens_out": 50, "cost": 0.02},
            "duration_ms": 123,
            "side_effect_report": "",
        }
        r = validate_result(obj, request_id="req_1", task_id="t1")
        assert r.success and r.usage.cost == 0.02 and r.duration_ms == 123

    def test_request_id_mismatch(self):
        obj = {"request_id": "req_2", "task_id": "t1", "success": True}
        with pytest.raises(ResponseValidationError, match="request_id"):
            validate_result(obj, request_id="req_1")

    def test_request_id_missing(self):
        obj = {"task_id": "t1", "success": True}
        with pytest.raises(ResponseValidationError, match="request_id"):
            validate_result(obj, request_id="req_1")

    def test_success_missing(self):
        obj = {"request_id": "r", "task_id": "t1"}
        with pytest.raises(ResponseValidationError, match="success"):
            validate_result(obj, request_id="r")

    def test_success_not_bool(self):
        obj = {"request_id": "r", "task_id": "t1", "success": "yes"}
        with pytest.raises(ResponseValidationError, match="success"):
            validate_result(obj, request_id="r")

    def test_failure_needs_error_code(self):
        obj = {"request_id": "r", "task_id": "t1", "success": False, "error": {"message": "x"}}
        with pytest.raises(ResponseValidationError, match="error.code"):
            validate_result(obj, request_id="r")

    def test_failure_with_error_code_ok(self):
        obj = {"request_id": "r", "task_id": "t1", "success": False,
               "error": {"code": "model_timeout", "message": "超时"}}
        r = validate_result(obj, request_id="r")
        assert r.error.code == "model_timeout"

    def test_success_with_error_contradiction(self):
        obj = {"request_id": "r", "task_id": "t1", "success": True,
               "error": {"code": "x"}}
        with pytest.raises(ResponseValidationError, match="语义矛盾"):
            validate_result(obj, request_id="r")

    def test_run_success_missing_output(self):
        obj = {"request_id": "r", "task_id": "t1", "success": True}
        with pytest.raises(ResponseValidationError, match="output"):
            validate_result(obj, request_id="r", response_kind="run")

    def test_cancel_null_output_allowed(self):
        obj = {"request_id": "r", "task_id": "t1", "success": True, "output": None}
        r = validate_result(obj, request_id="r", response_kind="cancel")
        assert r.success and r.output is None

    def test_usage_cost_wrong_type(self):
        obj = {"request_id": "r", "task_id": "t1", "success": True,
               "output": {}, "usage": {"cost": "0.02"}}
        with pytest.raises(ResponseValidationError, match="usage"):
            validate_result(obj, request_id="r")

    def test_extra_fields_tolerated(self):
        """幻觉加字段（§7.1 R1）不判死，保留容忍策略。"""
        obj = {"request_id": "r", "task_id": "t1", "success": True,
               "output": {}, "extra_field": "无所谓"}
        r = validate_result(obj, request_id="r")
        assert r.success

    def test_side_effect_report_type(self):
        obj = {"request_id": "r", "task_id": "t1", "success": True,
               "output": {}, "side_effect_report": 123}
        with pytest.raises(ResponseValidationError, match="side_effect_report"):
            validate_result(obj, request_id="r")


# ---------------------------------------------------------------------------
# 简化版（§7.8）
# ---------------------------------------------------------------------------

class TestSimpleMode:
    def test_minimal_valid(self):
        obj = {"success": True, "output": {"x": 1}}
        r = validate_result(obj, template_mode="simple", request_id="r", task_id="t1")
        assert r.success and r.output == {"x": 1}
        # 框架补默认值
        assert r.usage.cost == 0.0 and r.duration_ms == 0
        assert r.request_id == "r"  # 框架按请求方补值

    def test_minimal_failure(self):
        obj = {"success": False, "error": {"code": "x"}}
        r = validate_result(obj, template_mode="simple", request_id="r", task_id="t1")
        assert not r.success and r.error.code == "x"

    def test_missing_success(self):
        with pytest.raises(ResponseValidationError):
            validate_result({"output": {}}, template_mode="simple")


# ---------------------------------------------------------------------------
# 解析重试（§7.3）
# ---------------------------------------------------------------------------

class TestParseResult:
    def test_retry_on_bad_then_good(self):
        good = json.dumps(RESPONSE_EXAMPLE, ensure_ascii=False)
        calls = []

        def retry_fn(correction: str) -> str:
            calls.append(correction)
            return good

        r = parse_result(
            "不是 JSON",
            request_id="req_0001",
            task_id="task_001",  # 与 RESPONSE_EXAMPLE 一致
            retry_fn=retry_fn,
        )
        assert r.success
        assert len(calls) == 1
        assert "不符合要求" in calls[0]  # 修正提示携带原因
        assert "正确示例" in calls[0]

    def test_retry_exhausted_raises(self):
        def retry_fn(correction: str) -> str:
            return "还是不对"

        with pytest.raises(ResponseValidationError):
            parse_result(
                "坏响应",
                request_id="req_1",
                task_id="t1",
                retry_fn=retry_fn,
            )

    def test_no_retry_fn_raises_immediately(self):
        with pytest.raises(ResponseValidationError):
            parse_result("坏响应", request_id="req_1", task_id="t1")

    def test_first_try_valid_no_retry(self):
        obj = {"request_id": "req_1", "task_id": "t1", "success": True, "output": {}}
        called = False

        def retry_fn(correction: str) -> str:
            nonlocal called
            called = True
            return ""

        r = parse_result(json.dumps(obj), request_id="req_1", task_id="t1",
                         retry_fn=retry_fn)
        assert r.success and not called
