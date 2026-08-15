"""任务拆解引擎（架构 §3.2）：LLM 将目标拆解为子任务 + 依赖 DAG。

拆解是框架内部组件，不经过消息协议（协议是框架 ↔ 外部 agent 的边界）；
直接调用底层 LLM，输出按拆解 Schema 严格校验（缺字段 / 引用不存在的
依赖 / 成环均拒绝并重试）。
"""
from __future__ import annotations

import json
from typing import Callable

from .models import DAG, ResourceRequirement, SideEffects, Task
from .validation import ResponseValidationError, extract_json

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
"""


class DecomposeError(Exception):
    """拆解失败：输出非法或重试后仍无法通过 Schema 校验。"""


class Decomposer:
    """目标 → DAG。llm_call 接收完整提示词文本，返回模型原始响应。"""

    def __init__(
        self,
        llm_call: Callable[[str], str],
        max_retries: int = 1,
        temperature: float = 0.2,
    ):
        self._llm_call = llm_call
        self.max_retries = max_retries
        self.temperature = temperature

    def decompose(self, goal: str) -> DAG:
        prompt = f"{DECOMPOSITION_PROMPT}\n\n用户目标：{goal}"
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            raw = self._llm_call(prompt)
            try:
                obj = extract_json(raw)
                return self._build_dag(obj)
            except (ResponseValidationError, ValueError) as e:
                last_error = e

        raise DecomposeError(
            f"拆解输出连续 {self.max_retries + 1} 次未通过 Schema 校验：{last_error}"
        ) from last_error

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

        # 依赖引用存在性
        for tid, t in tasks.items():
            for d in t.deps:
                if d not in tasks:
                    raise ResponseValidationError(f"任务 {tid} 依赖不存在的任务 {d}")

        # 无环：Kahn 拓扑排序
        dag = DAG(tasks=tasks)
        indeg = {tid: len(t.deps) for tid, t in tasks.items()}
        queue = [tid for tid, deg in indeg.items() if deg == 0]
        visited = 0
        while queue:
            cur = queue.pop()
            visited += 1
            for tid, t in tasks.items():
                if cur in t.deps:
                    indeg[tid] -= 1
                    if indeg[tid] == 0:
                        queue.append(tid)
        if visited != len(tasks):
            raise ResponseValidationError("依赖关系成环，不是 DAG")

        # 至少一个 final（出度为 0）
        if not dag.final_tasks():
            raise ResponseValidationError("不存在最终交付任务（出度为 0 的任务）")

        return dag
