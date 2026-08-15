"""协议构造与渲染测试（协议 §4 / §7.6）。"""
import json

from orchestration.protocol import (
    PROTOCOL_PROMPT_FULL,
    PROTOCOL_PROMPT_SIMPLE,
    build_info_request,
    build_task_request,
    render,
)


class TestBuildTaskRequest:
    def test_run(self):
        req = build_task_request(
            request_id="r1", task_id="t1", task_desc="做某事",
            inputs={"a": 1}, output_schema={"x": "string"},
            constraints={"timeout_seconds": 300},
        )
        assert req["type"] == "task_request"
        assert req["action"] == "run"
        assert req["task_id"] == "t1"
        assert req["inputs"] == {"a": 1}
        assert req["output_schema"] == {"x": "string"}

    def test_cancel_variant(self):
        """取消是 task_request 的 action 变体（协议 §2），不新增消息类别。"""
        req = build_task_request(request_id="r2", task_id="t9", action="cancel")
        assert req["action"] == "cancel"
        assert "task_desc" not in req  # cancel 不携带执行字段


class TestBuildInfoRequest:
    def test_capability(self):
        req = build_info_request("r3", "capability", ["你能做什么？"])
        assert req["type"] == "info_request"
        assert req["scope"] == "capability"
        assert req["questions"] == ["你能做什么？"]


class TestRender:
    def test_sections_and_separators(self):
        req = build_task_request("r1", "t1", task_desc="d")
        text = render(req, template_mode="full", inputs={"up": 1})
        assert "=== REQUEST ===" in text
        assert "=== INPUT ===" in text
        assert "Agent 执行协议 v1.1" in text
        # INPUT 段是独立 JSON
        input_part = text.split("=== INPUT ===")[1]
        assert json.loads(input_part) == {"up": 1}

    def test_no_input_no_input_section(self):
        req = build_info_request("r1", "capability", ["q"])
        text = render(req, template_mode="full")
        assert "=== INPUT ===" not in text

    def test_simple_mode_template(self):
        req = build_task_request("r1", "t1", task_desc="d")
        text = render(req, template_mode="simple")
        assert "简化版" in text
        assert PROTOCOL_PROMPT_SIMPLE in text

    def test_full_template_has_few_shot(self):
        assert "响应示例" in PROTOCOL_PROMPT_FULL
        assert "常见错误" in PROTOCOL_PROMPT_FULL
