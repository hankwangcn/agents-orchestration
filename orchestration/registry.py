"""Agent 注册表——**组合门面**（架构 §3.2 规划层 × 调度层；阶段三起公开 API）。

层间切分（本次详细设计落地）：
- **规划层「资源统计器」** = `agent_pool.AgentPool`
  注册登记 / info_request 采集 / 声明解析入库 / 刷新（TTL + 决策点校验）/ 画像查询
- **调度层「资源协调器」** = `allocator.Allocator`
  三级分配（exact → capability → degraded）/ 多实例轮询 / 连续失败摘除

`AgentRegistry` 只是把两者组装起来并**零逻辑转发**（组合根），保持阶段三以来的
公开 API 不变——调度器 / 网关 / CLI / 审计 / 成本核算均按此接口调用，无需改动。
需要哪一半能力，也可直接依赖 `AgentPool` / `Allocator`。

核心设计不变：**能力是"问"出来的，不是配出来的**——配置只写接入三要素
（base_url / model / api_key），能力 / 限制仍由 info_request 采集。
"""
from __future__ import annotations

from typing import Callable, Optional, Union

from .adapters.base import AgentAdapter
from .agent_pool import (  # noqa: F401 (对外 re-export)
    INFO_QUESTIONS,
    VOLATILE_SCOPES,
    AgentPool,
    RegisteredAgent,
    RegistryError,
)
from .allocator import Allocator
from .models import Assignment  # noqa: F401 (对外 re-export)

__all__ = [
    "AgentRegistry",
    "AgentPool",
    "Allocator",
    "RegisteredAgent",
    "RegistryError",
    "INFO_QUESTIONS",
    "VOLATILE_SCOPES",
    "Assignment",
]


class AgentRegistry:
    """agent 档案库门面：注册 → 采集 → 分配 → 摘除（= AgentPool + Allocator）。

    default_agent_id：降级目标（通用 LLM），注册时第一个注册的可用 agent。

    刷新策略（TTL + 决策点校验）：
    - 静态画像带 last_collected_at；超过 collect_ttl_seconds 视为过期
    - 池级 `ensure_fresh()`：读时惰性刷新（仅对过期 agent 重新采集，
      保证三级分配看到的池级数据不陈旧）
    - 决策点 `validate_before_dispatch()`：派发前对选中的目标 agent
      复核易变维度（resource/constraint），把校验锚定在真正要用数据的时刻
    """

    def __init__(
        self,
        max_consecutive_failures: int = 3,
        collect_ttl_seconds: float = 300.0,
        validate_on_dispatch: bool = True,
    ):
        self._pool = AgentPool(
            collect_ttl_seconds=collect_ttl_seconds,
            validate_on_dispatch=validate_on_dispatch,
        )
        self._allocator = Allocator(
            self._pool, max_consecutive_failures=max_consecutive_failures
        )

    # ---------- 门面属性（层内实例可直达） ----------

    @property
    def pool(self) -> AgentPool:
        """规划层「资源统计器」实例。"""
        return self._pool

    @property
    def allocator(self) -> Allocator:
        """调度层「资源协调器」实例。"""
        return self._allocator

    @property
    def max_consecutive_failures(self) -> int:
        return self._allocator.max_consecutive_failures

    @max_consecutive_failures.setter
    def max_consecutive_failures(self, value: int) -> None:
        self._allocator.max_consecutive_failures = value

    @property
    def collect_ttl_seconds(self) -> float:
        return self._pool.collect_ttl_seconds

    @collect_ttl_seconds.setter
    def collect_ttl_seconds(self, value: float) -> None:
        self._pool.collect_ttl_seconds = value

    @property
    def validate_on_dispatch(self) -> bool:
        return self._pool.validate_on_dispatch

    @validate_on_dispatch.setter
    def validate_on_dispatch(self, value: bool) -> None:
        self._pool.validate_on_dispatch = value

    @property
    def _agents(self) -> dict[str, RegisteredAgent]:
        return self._pool.agents

    @property
    def _default_id(self) -> Optional[str]:
        return self._pool.default_id

    @_default_id.setter
    def _default_id(self, value: Optional[str]) -> None:
        self._pool._default_id = value

    # ---------- 规划层：注册 / 采集 / 刷新 / 画像 ----------

    def register(
        self,
        adapter: AgentAdapter,
        agent_id: Optional[str] = None,
    ) -> str:
        return self._pool.register(adapter, agent_id=agent_id)

    def register_default(self, agent_id: str) -> None:
        self._pool.register_default(agent_id)

    def collect(
        self,
        agent_id: Optional[str] = None,
        scope: Optional[str] = None,
    ) -> list[dict]:
        return self._pool.collect(agent_id=agent_id, scope=scope)

    def is_stale(self, agent: RegisteredAgent) -> bool:
        return self._pool.is_stale(agent)

    def ensure_fresh(self, agent_id: Optional[str] = None) -> list[dict]:
        return self._pool.ensure_fresh(agent_id=agent_id)

    async def aensure_fresh(self, agent_id: Optional[str] = None) -> list[dict]:
        return await self._pool.aensure_fresh(agent_id=agent_id)

    def validate_before_dispatch(self, agent_id: str) -> list[dict]:
        return self._pool.validate_before_dispatch(agent_id)

    async def avalidate_before_dispatch(self, agent_id: str) -> list[dict]:
        return await self._pool.avalidate_before_dispatch(agent_id)

    def get(self, agent_id: str) -> RegisteredAgent:
        return self._pool.get(agent_id)

    def get_adapter(self, agent_id: str) -> AgentAdapter:
        return self._pool.get_adapter(agent_id)

    @property
    def agents(self) -> dict[str, RegisteredAgent]:
        return self._pool.agents

    def available_agents(self) -> list[RegisteredAgent]:
        return self._pool.available_agents()

    # ---------- 调度层：分配 / 健康度 ----------

    def assign(self, task) -> tuple[Assignment, AgentAdapter]:
        return self._allocator.assign(task)

    def record_success(self, agent_id: str) -> None:
        self._allocator.record_success(agent_id)

    def record_failure(self, agent_id: str) -> None:
        self._allocator.record_failure(agent_id)

    def mark_unavailable(self, agent_id: str) -> None:
        self._allocator.mark_unavailable(agent_id)

    def mark_available(self, agent_id: str) -> None:
        self._allocator.mark_available(agent_id)

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
