"""Agent 注册表 + 资源统计 + 任务分配（架构 §3.2 规划/调度层；阶段三）。

核心设计（对话共识）：**能力是"问"出来的，不是配出来的**。
- 注册表只登记 agent 实例的存在（agent_id + model + adapter 引用）
- 能力 / 资源 / 约束通过 info_request 采集（协议 §4.2 scope=capability/
  resource/constraint），解析为结构化声明入库，可刷新
- 任务分配三级策略，全程留痕（Assignment）：
  1. exact      —— task.model 精确匹配（同 model 多实例轮询）
  2. capability —— 无 exact 时按能力标签匹配（部分覆盖标记风险）
  3. degraded   —— 无匹配降级到默认通用 LLM（显式留痕，不静默消化）
- 多 agent 资源协调：连续失败达到阈值自动摘除（unavailable），不再参与分配
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Union


from .adapters.base import AgentAdapter
from .models import Assignment, Task

# ---------------------------------------------------------------------------
# 采集问题集（协议 §4.2：info_request 的 questions）
# ---------------------------------------------------------------------------

INFO_QUESTIONS: dict[str, list[str]] = {
    "capability": [
        "你具备哪些能力？用逗号分隔的能力标签回答（如 code_review, data_analysis, report_writing）",
        "你最擅长处理哪类任务？一句话描述",
    ],
    "resource": [
        "你的最大并发任务数是多少？只回答数字",
        "每分钟最多能处理多少个请求？只回答数字，未知填 0",
        "单个任务的预算上限是多少美元？只回答数字，未知填 0",
    ],
    "constraint": [
        "你有哪些使用约束？如可用时间窗口、禁用事项，用分号分隔；没有则回答 无",
        "你支持哪些语言？用逗号分隔",
    ],
}


# ---------------------------------------------------------------------------
# 注册表数据结构
# ---------------------------------------------------------------------------

@dataclass
class RegisteredAgent:
    """一个 agent 实例的完整档案。adapter 是运行时对象，故用 dataclass 而非 pydantic。"""
    agent_id: str
    model: str
    adapter: AgentAdapter
    # 能力声明（info_request capability scope 采集）
    capabilities: list[str] = field(default_factory=list)
    description: str = ""
    # 资源声明（resource scope）
    max_concurrency: int = 1
    rate_limit_per_min: int = 0
    budget_limit_usd: float = 0.0
    # 约束声明（constraint scope）
    time_windows: list[str] = field(default_factory=list)
    forbidden: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    # 运行时状态（调度器维护）
    status: str = "available"  # available | unavailable
    consecutive_failures: int = 0
    last_collected_at: Optional[str] = None

    @property
    def available(self) -> bool:
        return self.status == "available"


class RegistryError(Exception):
    """注册表异常：无可用 agent / 采集失败等。"""


def _split_tags(text: str) -> list[str]:
    """把逗号/分号/顿号分隔的标签文本切成干净列表。"""
    tags = re.split(r"[,;，；、\n]+", text or "")
    return [t.strip() for t in tags if t.strip()]


def _is_time_window(tag: str) -> bool:
    """约束标签是否为时间窗描述（如"工作时间 9点~18点"）。

    用于 constraint 分流：时间窗归 time_windows，不混入 forbidden。
    """
    t = (tag or "").lower()
    return "~" in t or "点" in t or "window" in t or "时间窗" in t


def _first_number(text: str) -> float:
    """从文本提取第一个数字（容错：agent 可能回答"5 个"）。"""
    m = re.search(r"\d+(?:\.\d+)?", text or "")
    return float(m.group()) if m else 0.0


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------

class AgentRegistry:
    """agent 档案库：注册 → 采集 → 分配 → 摘除。

    default_agent_id：降级目标（通用 LLM），注册时第一个注册的可用 agent。
    """

    def __init__(self, max_consecutive_failures: int = 3):
        self._agents: dict[str, RegisteredAgent] = {}
        self._rr_counter: dict[str, int] = {}  # model → 轮询游标
        self._default_id: Optional[str] = None
        self.max_consecutive_failures = max_consecutive_failures

    # ---------- 注册 ----------

    def register(
        self,
        adapter: AgentAdapter,
        agent_id: Optional[str] = None,
    ) -> str:
        """登记一个 agent 实例。agent_id 缺省自动生成 agent_XXX。"""
        if agent_id is None:
            agent_id = f"agent_{len(self._agents) + 1:03d}"
        if agent_id in self._agents:
            raise RegistryError(f"agent_id 已存在：{agent_id}")
        self._agents[agent_id] = RegisteredAgent(
            agent_id=agent_id,
            model=adapter.model,
            adapter=adapter,
        )
        if self._default_id is None:
            self._default_id = agent_id  # 第一个注册的作为默认降级目标
        return agent_id

    def register_default(self, agent_id: str) -> None:
        """显式指定降级目标（通用 LLM）。"""
        if agent_id not in self._agents:
            raise RegistryError(f"未注册的 agent：{agent_id}")
        self._default_id = agent_id

    # ---------- 采集（info_request → 结构化声明入库） ----------

    def collect(
        self,
        agent_id: Optional[str] = None,
        scope: Optional[str] = None,
    ) -> list[dict]:
        """采集 agent 声明并入库。scope=None 采集全部三类。

        返回采集摘要列表（审计/可观测用）；单次失败不中断整体。
        """
        targets = [
            a for a in self._agents.values()
            if agent_id is None or a.agent_id == agent_id
        ]
        scopes = [scope] if scope else list(INFO_QUESTIONS)

        summary: list[dict] = []
        for agent in targets:
            for sc in scopes:
                entry = self._collect_one(agent, sc)
                summary.append(entry)
        return summary

    def _collect_one(self, agent: RegisteredAgent, scope: str) -> dict:
        """对单个 agent 发一次 info_request 并解析入库。"""
        request_id = f"info:{agent.agent_id}:{scope}:{_ts()}"
        try:
            result = agent.adapter.run_info(
                scope=scope,
                questions=INFO_QUESTIONS[scope],
                request_id=request_id,
            )
            if not result.success:
                raise RegistryError(
                    result.error.code if result.error else "info 请求失败"
                )
            self._apply_declaration(agent, scope, result.output)
            agent.last_collected_at = _ts()
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": True,
                "output": result.output,
            }
        except Exception as e:  # 适配器异常 / 解析失败 → 单点失败不中断
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": False,
                "error": str(e),
            }

    def _apply_declaration(
        self,
        agent: RegisteredAgent,
        scope: str,
        output: object,
    ) -> None:
        """把 info 响应的 output 解析为结构化字段（容错：宽松解析）。"""
        if isinstance(output, dict):
            answers = output
        elif isinstance(output, str):
            answers = {"q1": output}  # 兜底：整段当能力描述
        else:
            answers = {}

        def q(i: int) -> str:
            v = answers.get(f"q{i}") or answers.get(f"question_{i}") or ""
            return v if isinstance(v, str) else str(v)

        if scope == "capability":
            agent.capabilities = _split_tags(q(1))
            agent.description = q(2)
        elif scope == "resource":
            agent.max_concurrency = max(1, int(_first_number(q(1))))
            agent.rate_limit_per_min = int(_first_number(q(2)))
            agent.budget_limit_usd = _first_number(q(3))
        elif scope == "constraint":
            # q1 是混合自由文本（如"工作时间 9点~18点；禁止访问外网"）：
            # 时间窗短语归 time_windows，不扫入 forbidden——forbidden 是
            # 约束匹配/审计/学习规则的输入，混入时间窗会污染判定口径；
            # "无"（含空白变体）不计入任何一方
            agent.time_windows = []
            agent.forbidden = []
            for t in _split_tags(q(1)):
                if _is_time_window(t):
                    agent.time_windows.append(t)
                elif t != "无":
                    agent.forbidden.append(t)
            agent.languages = _split_tags(q(2))

    # ---------- 分配（三级策略 + 多实例轮询） ----------

    def assign(self, task: Task) -> tuple[Assignment, AgentAdapter]:
        """为任务分配 agent。

        1. exact：task.model 精确匹配（available 实例，同 model 轮询）
        2. capability：按 required_capabilities 匹配（部分覆盖标记 risk）
        3. degraded：降级默认通用 LLM（显式留痕）
        """
        model = task.required_resources.model
        req_caps = [c for c in (task.required_capabilities or []) if c]

        # 1. 精确 model 匹配
        pool = [
            a for a in self._agents.values()
            if a.model == model and a.available
        ]
        if pool:
            agent = self._round_robin(pool, model)
            # 能力声明未覆盖任务需求 → 标记风险（审计重点盯），但不降级
            risk = bool(req_caps) and not (set(req_caps) & set(agent.capabilities))
            return (
                Assignment(
                    task_id=task.id,
                    agent_id=agent.agent_id,
                    match_type="exact",
                    risk=risk,
                    reason=(
                        f"能力声明未覆盖需求 {req_caps}" if risk
                        else f"覆盖能力：{sorted(set(req_caps) & set(agent.capabilities))}"
                    ),
                ),
                agent.adapter,
            )

        # 2. 能力匹配：需求覆盖最多的 available agent
        if req_caps:
            best: Optional[RegisteredAgent] = None
            best_cover = 0
            for a in self._agents.values():
                if not a.available:
                    continue
                cover = len(set(req_caps) & set(a.capabilities))
                if cover > best_cover:
                    best, best_cover = a, cover
            if best is not None and best_cover > 0:
                partial = best_cover < len(set(req_caps))
                return (
                    Assignment(
                        task_id=task.id,
                        agent_id=best.agent_id,
                        match_type="capability",
                        risk=partial,
                        reason=(
                            f"能力覆盖 {best_cover}/{len(set(req_caps))}：{sorted(set(req_caps) & set(best.capabilities))}"
                            + ("（部分覆盖）" if partial else "")
                        ),
                    ),
                    best.adapter,
                )

        # 3. 降级：默认通用 LLM 优先；默认不可用则任意可用 agent 兜底
        #    （默认 agent 被摘除不应导致系统瘫痪）；全部不可用才抛错
        fallback: Optional[RegisteredAgent] = None
        if self._default_id is not None:
            d = self._agents.get(self._default_id)
            if d is not None and d.available:
                fallback = d
        if fallback is None:
            fallback = next(
                (a for a in self._agents.values() if a.available), None
            )
        if fallback is None:
            raise RegistryError(
                f"任务 {task.id} 无任何可用 agent："
                f"model={model}, caps={req_caps}"
            )
        return (
            Assignment(
                task_id=task.id,
                agent_id=fallback.agent_id,
                match_type="degraded",
                risk=False,
                reason=f"无匹配 agent（model={model}, caps={req_caps}），降级默认",
            ),
            fallback.adapter,
        )

    def _round_robin(self, pool: list[RegisteredAgent], model: str) -> RegisteredAgent:
        """同 model 多实例轮询分流（多 agent 资源协调）。"""
        idx = self._rr_counter.get(model, 0)
        agent = pool[idx % len(pool)]
        self._rr_counter[model] = idx + 1
        return agent

    # ---------- 状态维护（故障摘除 / 恢复） ----------

    def record_success(self, agent_id: str) -> None:
        agent = self._agents[agent_id]
        agent.consecutive_failures = 0

    def record_failure(self, agent_id: str) -> None:
        """连续失败达到阈值自动摘除（best-effort，调度器每次执行后调用）。"""
        agent = self._agents[agent_id]
        agent.consecutive_failures += 1
        if agent.consecutive_failures >= self.max_consecutive_failures:
            agent.status = "unavailable"

    def mark_unavailable(self, agent_id: str) -> None:
        self._agents[agent_id].status = "unavailable"

    def mark_available(self, agent_id: str) -> None:
        agent = self._agents[agent_id]
        agent.status = "available"
        agent.consecutive_failures = 0

    # ---------- 查询 ----------

    def get(self, agent_id: str) -> RegisteredAgent:
        return self._agents[agent_id]

    def get_adapter(self, agent_id: str) -> AgentAdapter:
        return self._agents[agent_id].adapter

    @property
    def agents(self) -> dict[str, RegisteredAgent]:
        return self._agents

    def available_agents(self) -> list[RegisteredAgent]:
        return [a for a in self._agents.values() if a.available]

    # ---------- 配置驱动批量注册（N 个 agent 的场景） ----------

    @classmethod
    def from_config(
        cls,
        config: Union[str, dict],
        adapter_factory: Optional[Callable[[dict], AgentAdapter]] = None,
    ) -> "AgentRegistry":
        """从配置文件 / dict 批量注册 N 个 agent（免逐行 register()）。

        config 支持三种形态：
        - dict —— 直接传入配置
        - str 且以 .yaml/.yml 结尾 —— 读 YAML 文件（需 pyyaml）
        - str 且以 .json 结尾 —— 读 JSON 文件

        配置结构（能力/资源/约束声明一条都不用配——那是 info_request
        collect() 问出来的；这里只写"agent 在哪、叫什么模型"）：

            max_consecutive_failures: 3        # 可选：连续失败摘除阈值
            default_agent: translator          # 可选：降级目标 agent_id
            agents:
              - agent_id: translator           # 可选，缺省自动生成
                base_url: http://agent-1:8000/v1
                model: qwen2.5-7b
                api_key: sk-xxx                # 可选（二选一）
                api_key_env: TRANSLATOR_KEY    # 可选：从环境变量取 key
                template_mode: full            # 可选：full | simple

        默认 adapter_factory：DeepSeekAdapter（OpenAI 兼容端点——base_url
        指向任意 OpenAI 兼容服务即接入，零代码；与框架"配置即接入"一致）。
        api_key 优先级：条目 api_key > api_key_env 环境变量 > 环境变量
        DEEPSEEK_API_KEY（DeepSeekAdapter 默认行为）。

        自定义传输（如进程内函数）可传 adapter_factory 覆盖：
            AgentRegistry.from_config(cfg, adapter_factory=lambda e:
                InProcessAdapter(my_fn, model=e["model"]))
        """
        data = cls._load_config(config)
        registry = cls(
            max_consecutive_failures=int(
                data.get("max_consecutive_failures", 3)
            )
        )
        factory = adapter_factory or cls._default_adapter_factory
        for entry in data.get("agents") or []:
            adapter = factory(entry)
            agent_id = registry.register(
                adapter, agent_id=entry.get("agent_id")
            )
            if data.get("default_agent") == agent_id:
                registry.register_default(agent_id)
        return registry

    @staticmethod
    def _load_config(config: Union[str, dict]) -> dict:
        if isinstance(config, dict):
            return config
        path = config
        if path.endswith((".yaml", ".yml")):
            try:
                import yaml
            except ImportError:
                raise RegistryError(
                    f"解析 YAML 需安装 pyyaml：pip install pyyaml（{path}）"
                ) from None
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        if path.endswith(".json"):
            import json

            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        raise RegistryError(
            f"不支持的配置文件格式（支持 .yaml/.yml/.json）：{path}"
        )

    @staticmethod
    def _default_adapter_factory(entry: dict) -> AgentAdapter:
        """默认工厂：OpenAI 兼容端点（DeepSeekAdapter 只认 base_url+api_key+model）。"""
        from .adapters.deepseek import DeepSeekAdapter

        api_key = entry.get("api_key")
        if not api_key and entry.get("api_key_env"):
            import os

            api_key = os.environ.get(entry["api_key_env"])
        return DeepSeekAdapter(
            model=entry.get("model", "deepseek-chat"),
            base_url=entry.get("base_url", "https://api.deepseek.com"),
            api_key=api_key,
            template_mode=entry.get("template_mode", "full"),
        )


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
