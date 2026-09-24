"""协议构造与渲染测试（协议 §4 / §7.6）。"""
import json

from orchestration.protocol import (
    PROTOCOL_PROMPT_FULL,
    PROTOCOL_PROMPT_SIMPLE,
    PROTOCOL_VERSION,
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
        assert "Agent 执行协议 v1.2" in text
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


class TestIdempotencyObligation:
    """#58 定标（方案 B）：幂等责任归执行侧，由模板显式声明；
    框架侧不对副作用任务的重复执行设拦截。"""

    def test_full_template_declares_idempotency_duty(self):
        assert "幂等" in PROTOCOL_PROMPT_FULL
        assert "side_effects" in PROTOCOL_PROMPT_FULL

    def test_simple_template_declares_idempotency_duty(self):
        assert "幂等" in PROTOCOL_PROMPT_SIMPLE
        assert "side_effects" in PROTOCOL_PROMPT_SIMPLE

    def test_template_version_consistent(self):
        """版本号常量与两档模板内声明一致（协议 §6 版本管理）。"""
        assert PROTOCOL_VERSION == "v1.2"
        assert f"Agent 执行协议 {PROTOCOL_VERSION}" in PROTOCOL_PROMPT_FULL
        assert f"Agent 执行协议 {PROTOCOL_VERSION}" in PROTOCOL_PROMPT_SIMPLE


class TestAdjudicationScope:
    """执行侧裁定范围声明：目标与契约范围内的全部取舍（含价值性取舍）由执行侧
    自行裁定，框架不设征询通道（协议 §3 行为约束第 6 条、§8.8）。"""

    def test_full_template_declares_adjudication_scope(self):
        assert "全部取舍由你自行裁定" in PROTOCOL_PROMPT_FULL
        assert "含价值性取舍" in PROTOCOL_PROMPT_FULL
        assert "不提供征询通道" in PROTOCOL_PROMPT_FULL

    def test_full_template_forbids_waiting_on_external_decision(self):
        """等待外部决策不得表现为执行中间态或失败——只能由执行侧自行裁定。"""
        assert "不得以待外部决策为由中止执行或返回失败" in PROTOCOL_PROMPT_FULL

    def test_simple_template_declares_adjudication_scope(self):
        assert "取舍由你自行裁定" in PROTOCOL_PROMPT_SIMPLE
        assert "不等待外部决策" in PROTOCOL_PROMPT_SIMPLE
