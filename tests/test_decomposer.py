"""任务拆解引擎测试（#30）：Schema 七关校验、修正提示重试、temperature 接线。

拆解**不走消息协议**（框架内部 LLM 调用，协议是框架 ↔ 外部 agent 的边界），
故用最简的脚本化 llm_call（提示词 → 原始文本）替代适配器——正好验证拆解
引擎只依赖"给提示词、拿回文本"这一个约定。
"""
from __future__ import annotations

import json

import pytest

from orchestration.decomposer import (
    DECOMPOSE_PROMPT_VERSION,
    DECOMPOSITION_EXAMPLE,
    DECOMPOSITION_PROMPT,
    DecomposeError,
    Decomposer,
    make_default_decomposer,
)
from orchestration.dependency import DependencyGraph
from orchestration.models import DAG, SideEffects


def raw(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)


# 合法拆解输出：t1 无依赖（并行前沿），t2 依赖 t1（串接）
VALID = {
    "tasks": [
        {
            "id": "task_001",
            "desc": "抓取目标站点价格",
            "deps": [],
            "required_capabilities": ["web_scraping"],
            "side_effects": "external_api",
        },
        {"id": "task_002", "desc": "汇总为比价报告", "deps": ["task_001"]},
    ]
}


class ScriptedLLM:
    """脚本化 llm_call：按调用顺序返回预设响应，记录提示词与 temperature。

    accept_temperature=True 时签名接受 temperature 关键字参数（模拟
    DeepSeekAdapter.chat 这类可注入温度的调用方）。
    """

    def __init__(self, *responses: str, accept_temperature: bool = False):
        self.responses = list(responses)
        self.prompts: list[str] = []
        self.temperatures: list[float] = []
        self._accept = accept_temperature

    def __call__(self, prompt: str, temperature: float | None = None) -> str:
        self.prompts.append(prompt)
        if temperature is not None:
            self.temperatures.append(temperature)
        if not self.responses:
            raise AssertionError("脚本化 llm_call 响应已耗尽（调用次数超预期）")
        return self.responses.pop(0)


class TestDecomposeSuccess:
    def test_builds_dag_from_valid_output(self):
        llm = ScriptedLLM(raw(VALID))
        dag = Decomposer(llm).decompose("做一个比价报告")

        assert isinstance(dag, DAG)
        assert set(dag.tasks) == {"task_001", "task_002"}
        t1, t2 = dag.tasks["task_001"], dag.tasks["task_002"]
        assert t1.deps == []
        assert t2.deps == ["task_001"]
        assert t1.required_capabilities == ["web_scraping"]
        assert t1.side_effects == SideEffects.EXTERNAL_API
        assert t2.side_effects == SideEffects.NONE  # 缺省
        assert t2.required_resources.model == "deepseek-chat"  # 缺省
        assert dag.final_tasks() == {"task_002"}

    def test_code_fenced_output_tolerated(self):
        """markdown 代码块包裹 + 前后杂文 → 解析组件照常提取（§7.6）。"""
        llm = ScriptedLLM(f"这是结果：\n```json\n{raw(VALID)}\n```\n以上。")
        dag = Decomposer(llm).decompose("目标")
        assert set(dag.tasks) == {"task_001", "task_002"}

    def test_goal_is_embedded_in_prompt(self):
        llm = ScriptedLLM(raw(VALID))
        Decomposer(llm).decompose("把商品页做成比价表")
        assert llm.prompts[0].startswith(DECOMPOSITION_PROMPT)
        assert llm.prompts[0].endswith("用户目标：把商品页做成比价表")

    def test_explicit_model_and_side_effects_kept(self):
        payload = {
            "tasks": [
                {"id": "a", "desc": "d", "model": "custom-model",
                 "side_effects": "file_write"},
            ]
        }
        dag = Decomposer(ScriptedLLM(raw(payload))).decompose("目标")
        assert dag.tasks["a"].required_resources.model == "custom-model"
        assert dag.tasks["a"].side_effects == SideEffects.FILE_WRITE


class TestSchemaGates:
    """Schema 校验各关：非法输出一律拒绝（重试耗尽 → DecomposeError）。"""

    @pytest.mark.parametrize(
        "payload,expect",
        [
            ({"foo": 1}, "缺少 tasks 列表"),                      # tasks 缺失
            ({"tasks": []}, "tasks 为空"),                        # 非空
            ({"tasks": ["x"]}, "非对象项"),                       # 逐项为对象
            ({"tasks": [{"desc": "d"}]}, "缺少 id"),              # id 必备
            ({"tasks": [{"id": "t1"}]}, "缺少 desc"),             # desc 必备
            ({"tasks": [{"id": "", "desc": "d"}]}, "缺少 id"),    # id 非空
            ({"tasks": [{"id": "t1", "desc": "a"},
                        {"id": "t1", "desc": "b"}]}, "id 重复"),  # 去重
            ({"tasks": [{"id": "t1", "desc": "a", "deps": "t2"}]},
             "deps 必须是字符串数组"),                            # deps 类型
            ({"tasks": [{"id": "t1", "desc": "a", "deps": [1]}]},
             "deps 必须是字符串数组"),
            ({"tasks": [{"id": "t1", "desc": "a",
                         "required_capabilities": "x"}]},
             "required_capabilities 必须是非空字符串数组"),
            ({"tasks": [{"id": "t1", "desc": "a",
                         "required_capabilities": [""]}]},
             "required_capabilities 必须是非空字符串数组"),
            ({"tasks": [{"id": "t1", "desc": "a",
                         "side_effects": "drop_db"}]}, "side_effects 非法"),
            ({"tasks": [{"id": "t1", "desc": "a", "deps": ["nope"]}]},
             "依赖不存在的任务"),                                 # 引用存在
            ({"tasks": [{"id": "t1", "desc": "a", "deps": ["t2"]},
                        {"id": "t2", "desc": "b", "deps": ["t1"]}]},
             "成环"),                                             # Kahn 无环
        ],
    )
    def test_invalid_output_rejected(self, payload, expect):
        llm = ScriptedLLM(raw(payload), raw(payload))
        with pytest.raises(DecomposeError) as ei:
            Decomposer(llm).decompose("目标")
        assert expect in str(ei.value)

    def test_non_json_output_rejected(self):
        llm = ScriptedLLM("抱歉，我无法完成", "抱歉，我无法完成")
        with pytest.raises(DecomposeError) as ei:
            Decomposer(llm).decompose("目标")
        assert "未找到 JSON 对象" in str(ei.value)

    def test_final_task_gate(self, monkeypatch):
        """「至少一个出度 0 的任务」关：无环非空图必有 sink，正常路径不可达，
        故 monkeypatch 强制 final_tasks() 为空以覆盖该防守分支。

        该判定已随依赖分析独立到 DependencyGraph（规划层 §3.2），
        校验入口为 DependencyGraph.validate()。
        """
        monkeypatch.setattr(DependencyGraph, "final_tasks", lambda self: set())
        llm = ScriptedLLM(raw(VALID))
        with pytest.raises(DecomposeError) as ei:
            Decomposer(llm, max_retries=0).decompose("目标")
        assert "最终交付任务" in str(ei.value)


class TestRetry:
    def test_retry_recovers_with_correction_prompt(self):
        """首次不合规 → 第二次带「失败原因 + 正确示例」重试（§7.3 口径）。"""
        bad = raw({"tasks": [{"id": "t1", "desc": "a", "deps": ["ghost"]}]})
        llm = ScriptedLLM(bad, raw(VALID))
        dag = Decomposer(llm).decompose("目标")

        assert set(dag.tasks) == {"task_001", "task_002"}
        assert len(llm.prompts) == 2
        first, second = llm.prompts
        assert "上一次输出不合规" not in first  # 首轮不夹带修正提示
        assert "【上一次输出不合规】" in second
        assert "依赖不存在的任务 ghost" in second          # 失败原因
        assert json.dumps(DECOMPOSITION_EXAMPLE, ensure_ascii=False,
                          indent=2) in second  # 正确示例（给示例 > 给指令）
        assert second.startswith(DECOMPOSITION_PROMPT)  # 基础提示词仍在

    def test_exhausted_retries_raise_with_reason(self):
        llm = ScriptedLLM(raw({"tasks": []}), raw({"tasks": []}))
        with pytest.raises(DecomposeError) as ei:
            Decomposer(llm, max_retries=1).decompose("目标")
        assert "连续 2 次" in str(ei.value)
        assert len(llm.prompts) == 2

    def test_max_retries_zero_single_attempt(self):
        llm = ScriptedLLM(raw({"tasks": []}))
        with pytest.raises(DecomposeError):
            Decomposer(llm, max_retries=0).decompose("目标")
        assert len(llm.prompts) == 1


class TestTemperature:
    def test_temperature_injected_when_supported(self):
        """调用方签名接受 temperature → 注入 self.temperature（死参数修复）。"""
        llm = ScriptedLLM(raw(VALID), accept_temperature=True)
        Decomposer(llm, temperature=0.7).decompose("目标")
        assert llm.temperatures == [0.7]

    def test_default_temperature_value(self):
        llm = ScriptedLLM(raw(VALID), accept_temperature=True)
        Decomposer(llm).decompose("目标")
        assert llm.temperatures == [0.2]

    def test_temperature_skipped_when_not_supported(self):
        """调用方只接受提示词 → 不注入（不因死参数报 TypeError）。"""
        seen: list[str] = []

        def llm(prompt: str) -> str:
            seen.append(prompt)
            return raw(VALID)

        Decomposer(llm, temperature=0.7).decompose("目标")
        assert len(seen) == 1


class TestDefaultDecomposer:
    """默认拆解引擎：复用 DeepSeekAdapter 的裸聊天入口（同一套端点/鉴权）。"""

    def test_wires_model_and_temperature(self, monkeypatch):
        captured: dict = {}

        class FakeAdapter:
            def __init__(self, **kw):
                captured.update(kw)

            def chat(self, prompt: str, temperature: float | None = None) -> str:
                captured["temperature_arg"] = temperature
                return raw(VALID)

        monkeypatch.setattr(
            "orchestration.adapters.deepseek.DeepSeekAdapter", FakeAdapter
        )
        d = make_default_decomposer(model="deepseek-chat", temperature=0.9)
        dag = d.decompose("目标")

        assert set(dag.tasks) == {"task_001", "task_002"}
        assert captured["model"] == "deepseek-chat"
        assert captured["temperature"] == 0.9        # 构造期注入 adapter
        assert captured["temperature_arg"] == 0.9    # 调用期注入 llm_call

    def test_requires_api_key(self, monkeypatch):
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        with pytest.raises(ValueError):
            make_default_decomposer()


class TestAdjudicationScopeDeclaration:
    """执行侧裁定范围声明（#57 ⑤）：子任务描述给出目标与结果要求即可——契约
    范围内的取舍由执行侧自行裁定，无需在子任务中穷举偏好或预设决策分支。"""

    def test_prompt_declares_execution_side_adjudication(self):
        assert "取舍由执行侧自行裁定" in DECOMPOSITION_PROMPT
        assert "无需在子任务中穷举偏好或预设决策分支" in DECOMPOSITION_PROMPT

    def test_prompt_version_matches_text(self):
        """提示词文本变更即升版留痕（新版声明与旧版提示词不得错配）。"""
        assert DECOMPOSE_PROMPT_VERSION == "v3"
