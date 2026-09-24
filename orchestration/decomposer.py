"""任务拆解引擎（架构 §3.2 规划层）：LLM 将目标拆解为子任务 + 依赖 DAG。

拆解是框架内部组件，**不经过消息协议**（协议是框架 ↔ 外部 agent 的边界）；
框架直接调用底层 LLM，输出按拆解 Schema 严格校验（缺字段 / 引用不存在的
依赖 / 成环均拒绝并重试）。

重试口径与解析组件（协议 §7.3）对齐：首次不合规**不重放同一提示词**，
而是把失败原因 + 正确示例追加进提示词再试一次——"给示例比给指令在格式
稳定性上稳一个量级"（项目准则）。

**学习层闭环出口**：提示词 = 固定指令 → 〔学习层指导块〕 → 用户目标。
指导块由 `lessons.PromptAdvisor` 生成（经验库跨 run 聚合 + 注册表客观事实），
经 `guidance_provider` 注入——既往运行的返工事实（降级分配、能力风险、高频
错误码、剪枝）在拆解期即被规避。指导块缺失/生成失败一律退化为不注入。

DAG 是任务的统一描述抽象（对话共识）：无依赖 = 默认并行，有依赖 = 串/并
混合，都在同一 DAG 表达内；依赖结构在提交时一次性给定（批处理"任务→
结果"定位下不支持运行中动态扩图）。
"""
from __future__ import annotations

import inspect
import json
from typing import Callable, Optional

from .dependency import DependencyError, DependencyGraph
from .models import DAG, ResourceRequirement, SideEffects, Task
from .validation import ResponseValidationError, extract_json

DECOMPOSE_PROMPT_VERSION = "v3"
"""提示词版本——提示词文本变更即升版留痕。
v2：学习层回馈（动态指导块）改变了提示词形态。
v3：增补执行侧裁定范围声明（子任务描述给出目标与结果要求即可，取舍在
执行侧裁定，无需预设决策分支）。"""

DECOMPOSITION_PROMPT = """你是任务拆解引擎。将用户目标拆解为可执行的子任务集合，输出任务间的数据流依赖。

只输出一个 JSON 对象（不要其他内容）：

{
  "tasks": [
    {
      "id": "task_001",
      "desc": "子任务描述，要求：单个 agent 可独立完成、描述自包含（不依赖其他任务的执行细节）",
      "deps": ["task_000"],
      "model": "deepseek-chat",
      "required_capabilities": ["code_review"],
      "side_effects": "none"
    }
  ]
}

字段说明：
- id: 唯一标识，格式 task_XXX
- desc: 自包含的子任务描述，产出明确、可供下游消费
- deps: 依赖的上游任务 id 列表，无依赖填 []
- model: 建议使用的模型（可选，默认 deepseek-chat）
- required_capabilities: 任务所需的能力标签列表（可选，默认 []；框架据此匹配
  具备相应能力的 agent，无匹配时降级通用模型并留痕）
- side_effects: none | external_api | file_write（可选，默认 none）

硬性要求：
1. deps 只能引用本 JSON 中已定义的任务 id，且依赖关系不能成环
2. 至少有一个任务没有下游（最终交付任务）
3. 拆分粒度：每个任务可在一次 LLM 调用内独立完成，不要过度拆分
4. 子任务描述给出目标与结果要求即可：目标与契约（输入、输出结构、约束）范围内的
   全部取舍由执行侧自行裁定，无需在子任务中穷举偏好或预设决策分支
"""

# 正确拆解示例：重试修正提示用（口径同协议 §7.3——失败原因 + 正确示例）
DECOMPOSITION_EXAMPLE: dict = {
    "tasks": [
        {
            "id": "task_001",
            "desc": "抓取目标站点的产品价格列表",
            "deps": [],
            "model": "deepseek-chat",
            "required_capabilities": ["web_scraping"],
            "side_effects": "external_api",
        },
        {
            "id": "task_002",
            "desc": "把抓取结果整理为比价报告",
            "deps": ["task_001"],
            "model": "deepseek-chat",
            "required_capabilities": ["report_writing"],
            "side_effects": "none",
        },
    ]
}


class DecomposeError(Exception):
    """拆解失败：输出非法或重试后仍无法通过 Schema 校验。"""


class Decomposer:
    """目标 → DAG。

    llm_call：接收完整提示词文本，返回模型原始响应。若其签名接受
    ``temperature`` 关键字参数，则注入 ``self.temperature``（否则忽略——
    调用方可自行在闭包里固化温度）。**框架内部 LLM 调用，不走消息协议。**

    guidance_provider：**学习层闭环出口**——每次拆解前调用一次，返回一段
    历史经验/注册表事实指导块（`lessons.PromptAdvisor.guidance`），追加在
    基础提示词与用户目标之间。返回空串即不注入；provider 抛异常一律视为
    无指导（拆解链路不因学习层故障而失败）。

    提示词结构（保持稳定，便于经验积累与回归）：基础提示词 → 〔指导块〕 →
    用户目标。
    """

    def __init__(
        self,
        llm_call: Callable[..., str],
        max_retries: int = 1,
        temperature: float = 0.2,
        guidance_provider: Optional[Callable[[], str]] = None,
    ):
        self._llm_call = llm_call
        self.max_retries = max_retries
        self.temperature = temperature
        self._passes_temperature = _accepts_kwarg(llm_call, "temperature")
        self.guidance_provider = guidance_provider
        self.last_guidance = ""
        """最近一次拆解实际注入的指导块（空串 = 无指导），供观测与测试。"""
        self.last_prompt = ""
        """最近一次拆解实际发出的提示词（含指导块），供观测与测试。"""

    def decompose(self, goal: str) -> DAG:
        """目标 → DAG；连续 max_retries + 1 次不合规则抛 DecomposeError。"""
        self.last_guidance = self._guidance()
        base_prompt = _base_prompt(goal, self.last_guidance)
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            # 首次用基础提示词；重试追加"失败原因 + 正确示例"（§7.3 口径）
            prompt = (
                base_prompt if attempt == 0
                else _correction_prompt(base_prompt, last_error)
            )
            self.last_prompt = prompt
            raw = self._invoke(prompt)
            try:
                obj = extract_json(raw)
                return self._build_dag(obj)
            except (ResponseValidationError, ValueError) as e:
                last_error = e

        raise DecomposeError(
            f"拆解输出连续 {self.max_retries + 1} 次未通过 Schema 校验：{last_error}"
        ) from last_error

    def _guidance(self) -> str:
        """取指导块：无 provider / 空串 / 异常 → 空串（学习层故障不拖垮拆解）。"""
        if self.guidance_provider is None:
            return ""
        try:
            return (self.guidance_provider() or "").strip()
        except Exception:
            return ""

    def _invoke(self, prompt: str) -> str:
        """调用底层 LLM（支持 temperature 注入的调用方按需传入）。"""
        if self._passes_temperature:
            return self._llm_call(prompt, temperature=self.temperature)
        return self._llm_call(prompt)

    def _build_dag(self, obj) -> DAG:
        """把拆解输出校验为合法 DAG：非空、依赖引用存在、无环、有 final。"""
        if not isinstance(obj, dict) or not isinstance(obj.get("tasks"), list):
            raise ResponseValidationError("拆解输出缺少 tasks 列表")

        tasks_raw = obj["tasks"]
        if not tasks_raw:
            raise ResponseValidationError("tasks 为空")

        tasks: dict[str, Task] = {}
        for item in tasks_raw:
            if not isinstance(item, dict):
                raise ResponseValidationError("tasks 中存在非对象项")
            tid = item.get("id")
            desc = item.get("desc")
            if not isinstance(tid, str) or not tid:
                raise ResponseValidationError("任务缺少 id")
            if not isinstance(desc, str) or not desc:
                raise ResponseValidationError(f"任务 {tid} 缺少 desc")
            if tid in tasks:
                raise ResponseValidationError(f"任务 id 重复：{tid}")

            deps = item.get("deps", [])
            if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
                raise ResponseValidationError(f"任务 {tid} 的 deps 必须是字符串数组")

            model = item.get("model") or "deepseek-chat"
            caps = item.get("required_capabilities") or []
            if not isinstance(caps, list) or not all(
                isinstance(c, str) and c for c in caps
            ):
                raise ResponseValidationError(
                    f"任务 {tid} 的 required_capabilities 必须是非空字符串数组"
                )
            side = item.get("side_effects") or "none"
            if side not in ("none", "external_api", "file_write"):
                raise ResponseValidationError(f"任务 {tid} 的 side_effects 非法：{side}")

            tasks[tid] = Task(
                id=tid,
                desc=desc,
                deps=list(deps),
                required_resources=ResourceRequirement(model=str(model)),
                required_capabilities=list(caps),
                side_effects=SideEffects(side),
            )

        # 依赖分析（规划层 §3.2）：引用存在 → 无环 → 至少一个交付点。
        # 原为拆解引擎内联 Kahn 实现，现收敛到独立 DependencyGraph。
        dag = DAG(tasks=tasks)
        try:
            DependencyGraph(dag).validate()
        except DependencyError as e:
            raise ResponseValidationError(str(e)) from e

        return dag


def _base_prompt(goal: str, guidance: str = "") -> str:
    """基础提示词 = 固定指令 → 〔学习层指导块〕 → 用户目标。

    指导块居中注入：前缀恒为固定指令、结尾恒为用户目标，便于回归断言与
    经验积累（结构稳定比位置好看更重要）。
    """
    parts = [DECOMPOSITION_PROMPT]
    if guidance:
        parts.append(guidance)
    parts.append(f"用户目标：{goal}")
    return "\n\n".join(parts)


def _correction_prompt(base_prompt: str, error: Optional[Exception]) -> str:
    """重试修正提示（口径同协议 §7.3）：失败原因 + 正确示例。"""
    return (
        f"{base_prompt}\n\n"
        f"【上一次输出不合规】{error}\n"
        "请严格按下面的正确示例重新输出（只输出 JSON，不要其他内容）：\n"
        f"{json.dumps(DECOMPOSITION_EXAMPLE, ensure_ascii=False, indent=2)}"
    )


def _accepts_kwarg(fn: Callable[..., object], name: str) -> bool:
    """调用方签名是否接受某关键字参数（含 **kwargs）。无法内省时视为否。"""
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    if name in params:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def make_default_decomposer(
    model: str = "deepseek-chat",
    temperature: float = 0.2,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    guidance_provider: Optional[Callable[[], str]] = None,
) -> Decomposer:
    """默认拆解引擎：复用 DeepSeekAdapter 的裸聊天入口（同一套端点与鉴权）。

    api_key 缺省回落到环境变量 DEEPSEEK_API_KEY；两者皆无则抛 ValueError——
    由调用方决定是否启用拆解能力（框架其余功能不依赖拆解引擎）。

    guidance_provider：学习层闭环出口（经验库 + 注册表事实 → 指导块），
    见 ``lessons.PromptAdvisor.guidance``。
    """
    from .adapters.deepseek import DeepSeekAdapter

    kwargs: dict = {"model": model, "temperature": temperature}
    if api_key:
        kwargs["api_key"] = api_key
    if base_url:
        kwargs["base_url"] = base_url
    adapter = DeepSeekAdapter(**kwargs)
    return Decomposer(
        llm_call=adapter.chat,
        temperature=temperature,
        guidance_provider=guidance_provider,
    )
