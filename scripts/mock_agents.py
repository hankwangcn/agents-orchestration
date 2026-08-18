"""本地 OpenAI 兼容 mock agent 服务（全流程测试档三：真实 HTTP 往返，无需真实 LLM）。

用途：以"本地 HTTP 端点"模拟真实 agent——框架（客户端）通过 DeepSeekAdapter
走真实 HTTP 调到这里（服务端），完整验证：
协议装配 → HTTP 传输 → 解析兜底 → 调度行为，链路与真实 agent 完全一致。

与真实 agent 的唯一差异是"业务智能"：mock 不真思考，它解析请求中注入的
协议 REQUEST JSON，按 agent 配置脚本化应答（协议响应模拟器）。框架侧对
"业务能力"无感知，协议/传输/解析层面完全等价。

多 agent 并存：按 OpenAI 兼容请求的 model 字段区分角色（注册表里
adapter.model = agent_id 即角色名）。

故障注入（确定性，非概率——保证冒烟可复现）：
- http500_first_n    前 N 次 run 返回 HTTP 500（传输层故障 → adapter_error）
- nonsense_first_n   前 N 次返回杂文（非 JSON → 触发框架解析层重试，§7.3）
- fail_first_n       前 N 次返回合法 JSON 但 success=false（业务失败 →
                      调度器重试 → 重试耗尽 → 失败传播/摘除）

观测：per-agent 活跃请求峰值（验证调度器 per-agent 并发上限真实生效）。

两种运行方式：
1. 独立进程：.venv/bin/python scripts/mock_agents.py   # 常驻 127.0.0.1:8701
2. 进程内（冒烟用）：server = MockAgentServer(configs)
   await server.start(); ...; await server.stop()
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Optional

from fastapi import FastAPI, HTTPException

REQUEST_MARKER = "=== REQUEST ==="


def _extract_request(messages: list[dict]) -> Optional[dict]:
    """从协议注入的消息里提取 REQUEST JSON（§7.6 模板/REQUEST/INPUT 拼接）。"""
    for msg in messages:
        content = msg.get("content") or ""
        if not isinstance(content, str) or REQUEST_MARKER not in content:
            continue
        rest = content.split(REQUEST_MARKER, 1)[1].strip()
        for candidate in (rest.split("\n", 1)[0], rest):  # json.dumps 单行；兜底整段
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    return None


@dataclass
class MockAgentConfig:
    """一个 mock agent 角色的完整配置（agent_id 同时作为端点的 model 字段）。"""
    agent_id: str
    capabilities: list[str] = field(default_factory=list)
    description: str = ""
    max_concurrency: int = 1
    rate_limit_per_min: int = 0
    budget_limit_usd: float = 0.0
    languages: list[str] = field(default_factory=lambda: ["中文", "English"])
    forbidden: list[str] = field(default_factory=list)
    output_kind: str = "generic"  # generic | translator | coder | analyst
    latency_ms: int = 0           # 每次 run 固定延迟（模拟真实耗时，验证并发）
    http500_first_n: int = 0      # 前 N 次 run 返回 HTTP 500
    nonsense_first_n: int = 0     # 前 N 次 run 返回杂文（非 JSON）
    fail_first_n: int = 0         # 前 N 次 run 返回 success=false

    # -- 协议应答 ------------------------------------------------------

    def info_answer(self, scope: str) -> dict:
        """info_request 应答（协议 §4.2：questions 逐条 q1/q2/q3）。"""
        if scope == "capability":
            return {
                "q1": ", ".join(self.capabilities) or "general",
                "q2": self.description,
            }
        if scope == "resource":
            return {
                "q1": str(self.max_concurrency),
                "q2": str(self.rate_limit_per_min),
                "q3": str(self.budget_limit_usd),
            }
        if scope == "constraint":
            return {
                "q1": "; ".join(self.forbidden) if self.forbidden else "无",
                "q2": ", ".join(self.languages),
            }
        return {}

    def task_output(self, task_id: str, task_desc: str) -> object:
        """task_request run 应答的 output（按角色生成，业务内容不影响框架行为）。"""
        if self.output_kind == "translator":
            return {"translation": f"translated: {task_desc[:60]}", "lang": "en"}
        if self.output_kind == "coder":
            return {
                "code": f"# {task_id}\ndef solve():\n    return 'ok'",
                "language": "python",
            }
        if self.output_kind == "analyst":
            return {"analysis": f"analyzed: {task_desc[:60]}", "confidence": 0.9}
        return {"result": f"{task_id}: done"}


class MockAgentServer:
    """进程内 OpenAI 兼容 mock 服务（单端口多角色）。"""

    def __init__(self, configs: list[MockAgentConfig], host: str = "127.0.0.1",
                 port: int = 0):
        self.configs: dict[str, MockAgentConfig] = {c.agent_id: c for c in configs}
        self.host = host
        self.port = port  # 0 = 自动分配（start 后回读实际端口）
        self.base_url = ""
        self.calls: dict[str, int] = {}       # agent_id -> 累计 run 调用
        self.active: dict[str, int] = {}      # 当前 in-flight run
        self.active_peak: dict[str, int] = {}  # 并发峰值（验证限流）
        self._server = None
        self._task: Optional[asyncio.Task] = None

        self.app = FastAPI(title="mock-agents")
        # openai SDK 请求路径 = base_url + "/chat/completions"；
        # 同时挂 /v1 前缀兼容不同 base_url 习惯
        for path in ("/chat/completions", "/v1/chat/completions"):
            self.app.post(path)(self._chat_completions)

    # -- 生命周期 ------------------------------------------------------

    async def start(self) -> str:
        import uvicorn

        cfg = uvicorn.Config(self.app, host=self.host, port=self.port,
                             log_level="warning")
        self._server = uvicorn.Server(cfg)
        self._task = asyncio.create_task(self._server.serve())
        for _ in range(500):
            if self._server.started:
                break
            await asyncio.sleep(0.01)
        if not self._server.started:
            raise RuntimeError("mock agent 服务启动失败")
        if self.port == 0:
            sock = self._server.servers[0].sockets[0]
            self.port = sock.getsockname()[1]
        self.base_url = f"http://{self.host}:{self.port}"
        return self.base_url

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            try:
                await self._task
            except Exception:
                pass
        self._server, self._task = None, None

    # -- OpenAI 兼容端点 ------------------------------------------------

    async def _chat_completions(self, body: dict) -> dict:
        model = body.get("model") or ""
        cfg = self.configs.get(model)
        if cfg is None:
            raise HTTPException(status_code=404, detail=f"unknown model: {model}")
        req = _extract_request(body.get("messages") or [])
        if req is None:
            raise HTTPException(status_code=400, detail="缺少 === REQUEST === JSON")
        rtype = req.get("type")
        if rtype == "info_request":
            scope = req.get("scope", "")
            payload = {
                "request_id": req.get("request_id"),
                # info 请求无 task_id（协议特性），回一个稳定字符串即可（§4.2）
                "task_id": req.get("task_id") or f"info-{scope}",
                "success": True,
                "output": cfg.info_answer(scope),
                "error": None,
                "usage": {"tokens_in": 0, "tokens_out": 0, "cost": 0.0},
                "duration_ms": 0,
                "side_effect_report": "",
            }
            content = json.dumps(payload, ensure_ascii=False)
        elif rtype == "task_request" and req.get("action") == "cancel":
            content = json.dumps(
                {"request_id": req.get("request_id"), "task_id": req.get("task_id"),
                 "success": True, "output": None, "error": None,
                 "usage": {"tokens_in": 0, "tokens_out": 0, "cost": 0.0},
                 "duration_ms": 0, "side_effect_report": ""},
                ensure_ascii=False,
            )
        elif rtype == "task_request":
            content = await self._task_run(cfg, req)
        else:
            raise HTTPException(status_code=400, detail=f"未知请求类型: {rtype}")
        return self._openai_response(model, content, body)

    async def _task_run(self, cfg: MockAgentConfig, req: dict) -> str:
        """task_request(action=run)：故障注入 → 正常执行（延迟 + 并发观测）。"""
        n = self.calls.get(cfg.agent_id, 0)
        self.calls[cfg.agent_id] = n + 1

        if n < cfg.http500_first_n:
            raise HTTPException(status_code=500, detail="injected http 500")
        if n < cfg.http500_first_n + cfg.nonsense_first_n:
            return ("这个任务我知道了，我会认真完成它的！请放心。"
                    "（非 JSON 杂文，故意触发框架解析层重试）")
        if n < cfg.http500_first_n + cfg.nonsense_first_n + cfg.fail_first_n:
            return self._result_json(req, success=False, code="fail_injected",
                                     message="injected business failure")

        # 正常执行路径：延迟区间内统计并发（验证 per-agent 上限）
        if cfg.latency_ms:
            self.active[cfg.agent_id] = self.active.get(cfg.agent_id, 0) + 1
            self.active_peak[cfg.agent_id] = max(
                self.active_peak.get(cfg.agent_id, 0),
                self.active[cfg.agent_id],
            )
        try:
            if cfg.latency_ms:
                await asyncio.sleep(cfg.latency_ms / 1000)
            output = cfg.task_output(req.get("task_id", "?"),
                                     req.get("task_desc", ""))
            return self._result_json(req, success=True, output=output)
        finally:
            if cfg.latency_ms:
                self.active[cfg.agent_id] = self.active.get(cfg.agent_id, 0) - 1

    def _result_json(self, req: dict, success: bool, output: object = None,
                     code: str = "", message: str = "") -> str:
        """协议 Result 契约应答（request_id/task_id 原样带回，§7.7 强制）。"""
        payload = {
            "request_id": req.get("request_id"),
            "task_id": req.get("task_id"),
            "success": success,
            "output": output,
            "error": {"code": code, "message": message} if not success else None,
            "usage": {"tokens_in": 0, "tokens_out": 0, "cost": 0.0},
            "duration_ms": 0,
            "side_effect_report": "",
        }
        return json.dumps(payload, ensure_ascii=False)

    def _openai_response(self, model: str, content: str, body: dict) -> dict:
        """OpenAI 兼容响应（usage 按文本量估算——框架会真实回填）。"""
        user_text = " ".join(
            str(m.get("content") or "") for m in (body.get("messages") or [])
        )
        prompt_tokens = max(10, len(user_text) // 4)
        completion_tokens = max(10, len(content) // 4)
        return {
            "id": f"chatcmpl-mock-{int(time.time() * 1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
        }


# ---------------------------------------------------------------------------
# 默认角色阵容（与 smoke_multiagent.py 保持一致）
# ---------------------------------------------------------------------------

def default_configs() -> list[MockAgentConfig]:
    return [
        MockAgentConfig(
            agent_id="translator", capabilities=["translation"],
            description="中英互译 agent", max_concurrency=2,
            rate_limit_per_min=60, budget_limit_usd=5.0,
            languages=["中文", "English"], output_kind="translator",
            latency_ms=150,
        ),
        MockAgentConfig(
            agent_id="coder", capabilities=["code_gen", "code_review"],
            description="代码生成与评审 agent", max_concurrency=1,
            rate_limit_per_min=30, languages=["中文", "English"],
            output_kind="coder", latency_ms=80,
        ),
        MockAgentConfig(
            agent_id="analyst", capabilities=["data_analysis", "report_writing"],
            description="数据分析与报告 agent", max_concurrency=1,
            rate_limit_per_min=20, output_kind="analyst",
            latency_ms=100, nonsense_first_n=1,
        ),
        MockAgentConfig(
            agent_id="flaky", capabilities=["general"],
            description="易故障 agent（故障注入演示）", max_concurrency=1,
            output_kind="generic", fail_first_n=9,
        ),
    ]


def main() -> None:
    """独立运行：常驻 127.0.0.1:8701。"""

    async def _serve() -> None:
        server = MockAgentServer(default_configs(), port=8701)
        await server.start()
        print(f"mock agents 已启动: {server.base_url}")
        print(f"角色: {', '.join(server.configs)}（Ctrl+C 退出）")
        try:
            await asyncio.Event().wait()
        finally:
            await server.stop()

    try:
        asyncio.run(_serve())
    except KeyboardInterrupt:
        print("\nmock agents 已停止")


if __name__ == "__main__":
    main()
