"""Agent 池 + 资源统计（架构 §3.2 **规划层「资源统计器」**；阶段三）。

核心设计（对话共识）：**能力是"问"出来的，不是配出来的**。
- 只登记 agent 实例的存在（agent_id + model + adapter 引用）
- 能力 / 资源 / 约束声明通过 info_request 采集（协议 §4.2 scope=
  capability/resource/constraint），解析为结构化声明入库，可刷新
- 刷新策略 = **TTL 惰性刷新 + 决策点校验**：池级 `ensure_fresh()` 读时过期
  即刷（仅对过期 agent 重采，新鲜零开销）；决策点 `validate_before_dispatch()`
  在派发前对选中 agent 复核易变维度（resource/constraint）
- 采集受**框架侧 wall-clock 上限**约束（`info_timeout_seconds`，与任务执行 #34
  对称）：采集卡死不能无封顶地拖住调用方，超时按单点失败处理、保留上次画像

**层职责边界**：本模块只做"把池的画像弄准、给出去"（注册 / 采集 / 声明解析
/ 刷新 / 画像查询）。**任务分配与故障摘除不在此**——那是调度层「资源协调器」
的职责（`allocator.Allocator`）。二者由 `registry.AgentRegistry` 组合成门面。
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .adapters.base import AgentAdapter
from .timeouts import call_with_timeout

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
        "你有哪些功能限制？如禁止事项，用分号分隔；没有则回答 无",
        "你支持哪些语言？用逗号分隔",
    ],
}

# 派发决策真正依赖、且易变的声明维度（决策点校验只复核这两类）：
# 能力标签（capability）变化慢，交给池级 TTL 即可
VOLATILE_SCOPES: tuple[str, ...] = ("resource", "constraint")


# ---------------------------------------------------------------------------
# 池内数据结构
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
    # 资源声明（resource scope）——性能限制
    max_concurrency: int = 1
    rate_limit_per_min: int = 0
    budget_limit_usd: float = 0.0
    # 约束声明（constraint scope）——功能限制
    forbidden: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    # 运行时状态（调度层维护）
    status: str = "available"  # available | unavailable
    consecutive_failures: int = 0
    last_collected_at: Optional[str] = None

    @property
    def available(self) -> bool:
        return self.status == "available"


class RegistryError(Exception):
    """注册表异常：重复注册 / 不存在的 agent / 采集失败等。"""


def _split_tags(text: str) -> list[str]:
    """把逗号/分号/顿号分隔的标签文本切成干净列表。"""
    tags = re.split(r"[,;，；、\n]+", text or "")
    return [t.strip() for t in tags if t.strip()]


def _looks_like_time_window(tag: str) -> bool:
    """识别时间窗描述（如"工作时间 9点~18点"）。

    时间窗字段（原 `time_windows`）已废弃——采集但无消费方，故删除。
    此判定仅用于把误入 constraint q1 的时间窗文本从 forbidden 中剔除，
    避免污染约束匹配/审计/学习规则的输入口径。
    """
    t = (tag or "").lower()
    return "~" in t or "点" in t or "window" in t or "时间窗" in t


def _first_number(text: str) -> float:
    """从文本提取第一个数字（容错：agent 可能回答"5 个"）。"""
    m = re.search(r"\d+(?:\.\d+)?", text or "")
    return float(m.group()) if m else 0.0


def _parse_ts(s: Optional[str]) -> Optional[float]:
    """ISO 时间戳字符串 → epoch 秒（无法解析返回 None）。"""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# 资源统计器：Agent 池
# ---------------------------------------------------------------------------

class AgentPool:
    """agent 画像库（规划层）：注册 → 采集 → 声明解析 → 刷新 → 查询。

    default_agent_id：降级目标（通用 LLM）——注册时第一个注册的可用 agent，
    可经 `register_default()` 显式指定（由调度层资源协调器消费）。
    """

    def __init__(
        self,
        collect_ttl_seconds: float = 300.0,
        validate_on_dispatch: bool = True,
        info_timeout_seconds: float = 30.0,
    ):
        self._agents: dict[str, RegisteredAgent] = {}
        self._default_id: Optional[str] = None
        self.collect_ttl_seconds = collect_ttl_seconds
        self.validate_on_dispatch = validate_on_dispatch
        # 框架侧 wall-clock 上限（与任务执行 #34 对称）：采集同样会卡死，
        # 不能无封顶地拖住调用方（run 起始的池级刷新 / 派发前的决策点校验）。
        # <=0 表示不设超时。
        self.info_timeout_seconds = info_timeout_seconds

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
        """对单个 agent 发一次 info_request 并解析入库。

        采集受框架侧 wall-clock 上限约束（info_timeout_seconds）——与任务执行
        #34 对称：采集卡死同样不能无封顶地拖住调用方；超时按单点失败处理。
        """
        request_id = f"info:{agent.agent_id}:{scope}:{_ts()}"
        timeout = self.info_timeout_seconds

        def _call():
            return agent.adapter.run_info(
                scope=scope,
                questions=INFO_QUESTIONS[scope],
                request_id=request_id,
            )

        try:
            if timeout and timeout > 0:
                result = call_with_timeout(_call, timeout)
            else:
                result = _call()
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
        except TimeoutError:
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": False,
                "error": f"info 采集超时（>{timeout}s，框架侧 wall-clock）",
            }
        except Exception as e:  # 适配器异常 / 解析失败 → 单点失败不中断
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": False,
                "error": str(e),
            }

    async def _acollect_one(self, agent: RegisteredAgent, scope: str) -> dict:
        """异步版单 agent 采集（AsyncScheduler 决策点校验用，不阻塞事件循环）。

        同样受框架侧 wall-clock 上限约束（asyncio.wait_for）。
        """
        request_id = f"info:{agent.agent_id}:{scope}:{_ts()}"
        timeout = self.info_timeout_seconds
        try:
            call = agent.adapter.arun_info(
                scope=scope,
                questions=INFO_QUESTIONS[scope],
                request_id=request_id,
            )
            if timeout and timeout > 0:
                result = await asyncio.wait_for(call, timeout=timeout)
            else:
                result = await call
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
        except (asyncio.TimeoutError, TimeoutError):
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": False,
                "error": f"info 采集超时（>{timeout}s，框架侧 wall-clock）",
            }
        except Exception as e:  # 单点失败隔离：保留上次已知值，不中断整体
            return {
                "agent_id": agent.agent_id,
                "scope": scope,
                "ok": False,
                "error": str(e),
            }

    # ---------- 刷新（TTL 惰性刷新 + 决策点校验） ----------

    def is_stale(self, agent: RegisteredAgent) -> bool:
        """画像是否超过 TTL（从未采集过视为过期）。"""
        ts = _parse_ts(agent.last_collected_at)
        if ts is None:
            return True
        return (time.time() - ts) >= self.collect_ttl_seconds

    def ensure_fresh(self, agent_id: Optional[str] = None) -> list[dict]:
        """池级 TTL 惰性刷新：仅对过期 agent 重新采集（新鲜则零网络开销）。

        在"读"数据时调用（如池级匹配前），保证三级分配看到的池级画像不陈旧。
        """
        targets = [
            a for a in self._agents.values()
            if agent_id is None or a.agent_id == agent_id
        ]
        return [
            self._collect_one(a, sc)
            for a in targets if self.is_stale(a)
            for sc in INFO_QUESTIONS
        ]

    async def aensure_fresh(self, agent_id: Optional[str] = None) -> list[dict]:
        """异步版池级 TTL 刷新（AsyncScheduler 在 run 开始时调用）。"""
        targets = [
            a for a in self._agents.values()
            if agent_id is None or a.agent_id == agent_id
        ]
        out: list[dict] = []
        for a in targets:
            if not self.is_stale(a):
                continue
            for sc in INFO_QUESTIONS:
                out.append(await self._acollect_one(a, sc))
        return out

    def validate_before_dispatch(self, agent_id: str) -> list[dict]:
        """决策点校验（同步）：派发前复核选中目标 agent 的易变维度。

        只问 resource/constraint（派发决策真正依赖、且易变）；capability
        变化慢且信息量大，交给池级 TTL。失败不阻塞派发——保留上次已知值。
        """
        if not self.validate_on_dispatch:
            return []
        agent = self._agents[agent_id]
        return [self._collect_one(agent, sc) for sc in VOLATILE_SCOPES]

    async def avalidate_before_dispatch(self, agent_id: str) -> list[dict]:
        """决策点校验（异步）：AsyncScheduler 派发前调用，不阻塞事件循环。"""
        if not self.validate_on_dispatch:
            return []
        agent = self._agents[agent_id]
        return [await self._acollect_one(agent, sc) for sc in VOLATILE_SCOPES]

    def _apply_declaration(
        self,
        agent: RegisteredAgent,
        scope: str,
        output: object,
    ) -> None:
        """把 info 响应的 output 解析为结构化字段（容错：宽松解析）。

        注册记录口径（对话裁定）：**agent / 能力 / 限制（功能 + 性能）**——
        capability → 能力；resource → 性能限制（并发/限速/预算）；constraint
        → 功能限制（forbidden / languages）。合规限制归后期安全层，本轮不做。
        """
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
            # q1 为自由文本（如"禁止访问外网；输出必须是JSON"）：逐标签入库；
            # "无"（含空白变体）与空回答不计入。时间窗字段已废弃（无消费方，
            # 见 _looks_like_time_window），误入的时间窗文本在此剔除——forbidden
            # 是约束匹配/审计/学习规则的输入，混入时间窗会污染判定口径
            agent.forbidden = [
                t for t in _split_tags(q(1))
                if t != "无" and not _looks_like_time_window(t)
            ]
            agent.languages = _split_tags(q(2))

    # ---------- 查询（画像出参） ----------

    def get(self, agent_id: str) -> RegisteredAgent:
        return self._agents[agent_id]

    def get_adapter(self, agent_id: str) -> AgentAdapter:
        return self._agents[agent_id].adapter

    @property
    def agents(self) -> dict[str, RegisteredAgent]:
        return self._agents

    def available_agents(self) -> list[RegisteredAgent]:
        return [a for a in self._agents.values() if a.available]

    @property
    def default_id(self) -> Optional[str]:
        return self._default_id
