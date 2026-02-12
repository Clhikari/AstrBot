import json
import re
import traceback
from collections import defaultdict
from typing import Any

from quart import jsonify, request

from astrbot.core import logger
from astrbot.core.agent.handoff import HandoffTool
from astrbot.core.core_lifecycle import AstrBotCoreLifecycle
from astrbot.core.provider.entities import ProviderType

from .route import Response, Route, RouteContext


class SubAgentRoute(Route):
    AGENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
    MAX_AGENT_COUNT = 8
    MAX_TOOLS_PER_AGENT = 12
    DEFAULT_AUTO_TOOLS_PER_AGENT = 4

    def __init__(
        self,
        context: RouteContext,
        core_lifecycle: AstrBotCoreLifecycle,
    ) -> None:
        super().__init__(context)
        self.core_lifecycle = core_lifecycle
        self._persona_mgr = core_lifecycle.persona_mgr
        self.routes = [
            ("/subagent/config", ("GET", self.get_config)),
            ("/subagent/config", ("POST", self.update_config)),
            ("/subagent/available-tools", ("GET", self.get_available_tools)),
            ("/subagent/auto-plan", ("POST", self.auto_plan)),
            ("/subagent/auto-apply", ("POST", self.auto_apply)),
        ]
        self.register_routes()

    async def get_config(self):
        try:
            cfg = self.core_lifecycle.astrbot_config
            data = cfg.get("subagent_orchestrator")

            if not isinstance(data, dict):
                data = {
                    "main_enable": False,
                    "remove_main_duplicate_tools": False,
                    "agents": [],
                }

            if (
                isinstance(data, dict)
                and "main_enable" not in data
                and "enable" in data
            ):
                data["main_enable"] = bool(data.get("enable", False))

            data.setdefault("main_enable", False)
            data.setdefault("remove_main_duplicate_tools", False)
            data.setdefault("agents", [])

            if isinstance(data.get("agents"), list):
                for agent in data["agents"]:
                    if isinstance(agent, dict):
                        agent.setdefault("provider_id", None)
                        agent.setdefault("persona_id", None)
            return jsonify(Response().ok(data=data).__dict__)
        except Exception as exc:
            logger.error(traceback.format_exc())
            return jsonify(
                Response().error(f"获取 subagent 配置失败: {exc!s}").__dict__
            )

    async def update_config(self):
        try:
            data = await request.json
            if not isinstance(data, dict):
                return jsonify(Response().error("配置必须为 JSON 对象").__dict__)

            await self._persist_config(data)
            return jsonify(Response().ok(message="保存成功").__dict__)
        except Exception as exc:
            logger.error(traceback.format_exc())
            return jsonify(
                Response().error(f"保存 subagent 配置失败: {exc!s}").__dict__
            )

    async def get_available_tools(self):
        try:
            tools = self._list_available_tools()
            return jsonify(Response().ok(data=tools).__dict__)
        except Exception as exc:
            logger.error(traceback.format_exc())
            return jsonify(Response().error(f"获取可用工具失败: {exc!s}").__dict__)

    async def auto_plan(self):
        try:
            payload = await request.json
            if not isinstance(payload, dict):
                return jsonify(Response().error("请求体必须为 JSON 对象").__dict__)

            goal = str(payload.get("goal", "")).strip()
            if not goal:
                return jsonify(Response().error("goal 不能为空").__dict__)

            max_agents = payload.get("max_agents", 2)
            if not isinstance(max_agents, int):
                max_agents = 2
            max_agents = max(1, min(max_agents, self.MAX_AGENT_COUNT))

            tools = self._list_available_tools()
            few_shots = payload.get("few_shots", [])
            planner_meta: dict[str, Any] = {
                "engine": "llm",
                "reason": "pending",
                "few_shot_count": len(few_shots) if isinstance(few_shots, list) else 0,
            }

            draft_config, planner_meta = await self._build_llm_plan_config(
                goal=goal,
                tools=tools,
                max_agents=max_agents,
                payload=payload,
            )
            if draft_config is None:
                resp = Response().error("LLM 规划失败，未生成可用配置")
                resp.data = {"planner": planner_meta}
                return jsonify(resp.__dict__)

            validation = self._validate_config(draft_config, tools)

            data = {
                "goal": goal,
                "draft_config": validation["normalized_config"],
                "warnings": validation["warnings"],
                "errors": validation["errors"],
                "profiles": draft_config.get("_auto_profiles", []),
                "planner": planner_meta,
            }
            return jsonify(Response().ok(data=data).__dict__)
        except Exception as exc:
            logger.error(traceback.format_exc())
            return jsonify(Response().error(f"自动规划失败: {exc!s}").__dict__)

    async def auto_apply(self):
        try:
            payload = await request.json
            if not isinstance(payload, dict):
                return jsonify(Response().error("请求体必须为 JSON 对象").__dict__)

            config = payload.get("config")
            if config is None:
                config = payload
            if not isinstance(config, dict):
                return jsonify(Response().error("config 必须为 JSON 对象").__dict__)

            allow_warnings = bool(payload.get("allow_warnings", False))
            auto_create_persona = bool(payload.get("auto_create_persona", True))

            created_personas: list[str] = []
            if auto_create_persona:
                created_personas = await self._ensure_personas_for_config(config)

            tools = self._list_available_tools()
            validation = self._validate_config(config, tools)

            if validation["errors"]:
                resp = Response().error("配置校验失败，请先修复 errors")
                resp.data = validation
                return jsonify(resp.__dict__)

            if validation["warnings"] and not allow_warnings:
                resp = Response().error(
                    "配置存在 warnings，请确认后带 allow_warnings=true 再提交"
                )
                resp.data = validation
                return jsonify(resp.__dict__)

            normalized_config = validation["normalized_config"]
            await self._persist_config(normalized_config)

            return jsonify(
                Response()
                .ok(
                    data={
                        "applied": True,
                        "warnings": validation["warnings"],
                        "created_personas": created_personas,
                        "config": normalized_config,
                    },
                    message="自动配置已应用",
                )
                .__dict__
            )
        except Exception as exc:
            logger.error(traceback.format_exc())
            return jsonify(Response().error(f"自动应用失败: {exc!s}").__dict__)

    async def _persist_config(self, config: dict[str, Any]) -> None:
        cfg = self.core_lifecycle.astrbot_config
        cfg["subagent_orchestrator"] = config
        cfg.save_config()

        orch = getattr(self.core_lifecycle, "subagent_orchestrator", None)
        if orch is not None:
            await orch.reload_from_config(config)

    async def _ensure_personas_for_config(self, config: dict[str, Any]) -> list[str]:
        agents = config.get("agents", [])
        if not isinstance(agents, list):
            return []

        created: list[str] = []
        existing = self._build_persona_tool_index()
        all_tool_names = [tool.get("name") for tool in self._list_available_tools()]
        normalized_tool_names = [
            str(name).strip() for name in all_tool_names if str(name).strip()
        ]

        for agent in agents:
            if not isinstance(agent, dict):
                continue
            persona_id = self._normalize_optional_str(agent.get("persona_id"))
            if not persona_id or persona_id == "default" or persona_id in existing:
                continue

            raw_tools = agent.get("tools", [])
            tools: list[str] = []
            if isinstance(raw_tools, list):
                tools = [str(tool).strip() for tool in raw_tools if str(tool).strip()]
            if not tools:
                tools = normalized_tool_names[
                    : min(self.DEFAULT_AUTO_TOOLS_PER_AGENT, self.MAX_TOOLS_PER_AGENT)
                ]

            system_prompt = str(agent.get("system_prompt", "")).strip()
            if not system_prompt:
                system_prompt = str(agent.get("public_description", "")).strip()
            if not system_prompt:
                system_prompt = "You are a helpful and friendly assistant."

            try:
                await self.core_lifecycle.persona_mgr.create_persona(
                    persona_id=persona_id,
                    system_prompt=system_prompt,
                    begin_dialogs=None,
                    tools=tools,
                    skills=None,
                )
                created.append(persona_id)
                existing[persona_id] = tools
            except ValueError:
                existing[persona_id] = tools
            except Exception as exc:
                logger.warning(f"Failed to auto-create persona {persona_id}: {exc!s}")

        return created

    def _list_available_tools(self) -> list[dict[str, Any]]:
        tool_mgr = self.core_lifecycle.provider_manager.llm_tools
        tools: list[dict[str, Any]] = []
        for tool in tool_mgr.func_list:
            if isinstance(tool, HandoffTool):
                continue
            if tool.handler_module_path == "core.subagent_orchestrator":
                continue
            tools.append(
                {
                    "name": tool.name,
                    "description": str(tool.description or ""),
                    "parameters": tool.parameters,
                    "active": tool.active,
                    "handler_module_path": tool.handler_module_path,
                }
            )
        return tools

    async def _build_llm_plan_config(
        self,
        goal: str,
        tools: list[dict[str, Any]],
        max_agents: int,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        few_shots = payload.get("few_shots", [])
        planner_model = str(payload.get("planner_model", "")).strip() or None
        planner_provider_id = (
            str(payload.get("planner_provider_id", "")).strip() or None
        )
        planner_meta: dict[str, Any] = {
            "engine": "llm",
            "model": planner_model,
            "provider_id": planner_provider_id,
            "few_shot_count": len(few_shots) if isinstance(few_shots, list) else 0,
        }

        provider = None
        if planner_provider_id:
            provider = await self.core_lifecycle.provider_manager.get_provider_by_id(
                planner_provider_id
            )
            if provider is None:
                planner_meta["reason"] = "planner_provider_not_found"
                return None, planner_meta
        else:
            provider = self.core_lifecycle.provider_manager.get_using_provider(
                ProviderType.CHAT_COMPLETION
            )

        if provider is None:
            planner_meta["reason"] = "chat_provider_unavailable"
            return None, planner_meta
        if not hasattr(provider, "text_chat"):
            planner_meta["reason"] = "planner_provider_not_chat_completion"
            return None, planner_meta

        max_tools_per_agent = self._normalize_max_tools_per_agent(
            payload.get("max_tools_per_agent")
        )
        planner_meta["max_tools_per_agent"] = max_tools_per_agent

        prompt = self._build_llm_planner_prompt(
            goal,
            tools,
            max_agents,
            max_tools_per_agent,
            few_shots,
        )

        try:
            llm_resp = await provider.text_chat(prompt=prompt, model=planner_model)
            completion = str(getattr(llm_resp, "completion_text", "") or "").strip()
        except TimeoutError as exc:
            logger.warning(f"LLM planner timed out: {exc!s}")
            planner_meta["reason"] = f"llm_timeout: {exc!s}"
            return None, planner_meta
        except Exception as exc:
            logger.warning(f"LLM planner failed: {exc!s}")
            planner_meta["reason"] = f"llm_planner_exception: {exc!s}"
            return None, planner_meta

        if not completion:
            planner_meta["reason"] = "llm_empty_response"
            return None, planner_meta

        parsed = self._try_parse_llm_plan_payload(completion)
        if parsed is None:
            planner_meta["reason"] = "llm_invalid_json"
            return None, planner_meta

        config = self._normalize_llm_plan_payload(
            payload=parsed,
            tools=tools,
            max_agents=max_agents,
            planner_payload=payload,
        )
        if not config.get("agents"):
            planner_meta["reason"] = "llm_plan_no_valid_agents"
            return None, planner_meta

        planner_meta["reason"] = "llm_planner_success"
        planner_meta["raw_agent_count"] = len(config.get("agents", []))
        return config, planner_meta

    def _build_llm_planner_prompt(
        self,
        goal: str,
        tools: list[dict[str, Any]],
        max_agents: int,
        max_tools_per_agent: int,
        few_shots: Any,
    ) -> str:
        active_tools = [tool for tool in tools if bool(tool.get("active", True))]
        tool_lines = []
        for tool in active_tools:
            tool_lines.append(
                f"- name={tool['name']}; description={tool.get('description', '')}"
            )

        persona_lines = []
        for persona_id, persona_tools in sorted(
            self._build_persona_tool_index().items()
        ):
            if persona_tools is None:
                persona_lines.append(f"- {persona_id}: all tools")
            else:
                persona_lines.append(f"- {persona_id}: {len(persona_tools)} tools")

        few_shot_lines: list[str] = []
        if isinstance(few_shots, list):
            for index, item in enumerate(few_shots[:3]):
                if not isinstance(item, dict):
                    continue
                sample_goal = str(item.get("goal", "")).strip()
                sample_subagents = item.get("subagents", [])
                if sample_goal and isinstance(sample_subagents, list):
                    few_shot_lines.append(
                        f"Example {index + 1}: goal={sample_goal}; subagents={sample_subagents}"
                    )

        few_shot_section = "\n".join(few_shot_lines) if few_shot_lines else "(none)"
        return (
            "You are a planner for AstrBot subagents. "
            "Return JSON only without markdown. "
            "You must return an object with key `subagents`, where each item includes "
            "`name`, `public_description`, `system_prompt`, and `persona_id`; "
            "`provider_id` is optional.\n"
            f"Goal: {goal}\n"
            f"Max agents: {max_agents}\n"
            f"Max tools per agent: {max_tools_per_agent}\n"
            "Available tools:\n"
            f"{chr(10).join(tool_lines)}\n"
            "Available personas:\n"
            f"{chr(10).join(persona_lines)}\n"
            "Few-shots:\n"
            f"{few_shot_section}\n"
            "Constraints:\n"
            "1) Keep agent names in snake_case.\n"
            "2) Prefer selecting persona_id from available personas, because tools are inherited from persona.\n"
            "   Avoid using `default` persona unless user explicitly requests it.\n"
            "3) Do not include fields outside required schema.\n"
            "4) Prefer 1~3 agents with clear responsibility split.\n"
            "5) If uncertain about provider/persona, leave them empty and system defaults will be filled.\n"
            f"6) Select only essential tools per agent, at most {max_tools_per_agent}."
        )

    def _try_parse_llm_plan_payload(self, completion: str) -> dict[str, Any] | None:
        text = completion.strip()
        if not text:
            return None

        # Fast path: response is a single JSON object.
        parsed = self._try_json_object(text)
        if parsed is not None:
            return parsed

        # Second path: prefer fenced JSON code blocks.
        code_blocks = re.findall(r"```(?:json)?\s*(.*?)\s*```", text, re.S)
        for block in code_blocks:
            parsed = self._try_json_object(block.strip())
            if parsed is not None:
                return parsed

        # Last path: scan all object starts and try raw_decode.
        decoder = json.JSONDecoder()
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                candidate, _ = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict):
                return candidate
        return None

    def _try_json_object(self, raw: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _normalize_llm_plan_payload(
        self,
        payload: dict[str, Any],
        tools: list[dict[str, Any]],
        max_agents: int,
        planner_payload: dict[str, Any],
    ) -> dict[str, Any]:
        candidates = payload.get("subagents")
        if not isinstance(candidates, list):
            maybe_agents = payload.get("agents")
            candidates = maybe_agents if isinstance(maybe_agents, list) else []

        default_provider_id = self._resolve_default_subagent_provider_id(
            planner_payload
        )
        max_tools_per_agent = self._normalize_max_tools_per_agent(
            planner_payload.get("max_tools_per_agent")
        )
        default_persona_id = self._normalize_optional_str(
            planner_payload.get("default_persona_id")
        )
        prefer_new_persona = bool(planner_payload.get("prefer_new_persona", True))

        tool_name_map = {
            str(tool.get("name")).lower(): str(tool.get("name"))
            for tool in tools
            if str(tool.get("name", "")).strip()
        }
        all_tool_names = list(tool_name_map.values())
        persona_tool_index = self._build_persona_tool_index()

        agents: list[dict[str, Any]] = []
        auto_profiles: list[dict[str, Any]] = []
        used_names: dict[str, int] = defaultdict(int)

        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue

            raw_name = str(candidate.get("name", "")).strip().lower()
            normalized_name = re.sub(r"[^a-z0-9_]", "_", raw_name)
            normalized_name = re.sub(r"_+", "_", normalized_name).strip("_")
            if not normalized_name:
                normalized_name = "agent"
            if not normalized_name[0].isalpha():
                normalized_name = f"a_{normalized_name}"

            used_names[normalized_name] += 1
            final_name = (
                normalized_name
                if used_names[normalized_name] == 1
                else f"{normalized_name}_{used_names[normalized_name]}"
            )

            if prefer_new_persona and not default_persona_id:
                persona_id = f"{final_name}_persona"
            else:
                persona_id = self._normalize_optional_str(candidate.get("persona_id"))
                if persona_id == "default" and not default_persona_id:
                    persona_id = None
                if not persona_id:
                    if default_persona_id:
                        persona_id = default_persona_id
                    else:
                        persona_id = f"{final_name}_persona"

            raw_tools = candidate.get("tools", [])
            if not isinstance(raw_tools, list):
                raw_tools = []
            resolved_tools: list[str] = []
            for raw_tool_name in raw_tools:
                key = str(raw_tool_name).strip().lower()
                exact_name = tool_name_map.get(key)
                if exact_name and exact_name not in resolved_tools:
                    resolved_tools.append(exact_name)

            persona_bound_tools = self._resolve_persona_bound_tools(
                persona_id=persona_id,
                all_tool_names=all_tool_names,
                persona_tool_index=persona_tool_index,
            )
            if persona_bound_tools is not None:
                resolved_tools = persona_bound_tools

            if not resolved_tools:
                # Keep planner usable even when LLM does not output tools.
                resolved_tools = list(tool_name_map.values())[:max_tools_per_agent]
            if not resolved_tools:
                continue

            agents.append(
                {
                    "enabled": bool(candidate.get("enabled", True)),
                    "name": final_name,
                    "public_description": str(
                        candidate.get("public_description")
                        or candidate.get("description")
                        or ""
                    ).strip(),
                    "system_prompt": str(candidate.get("system_prompt", "")).strip(),
                    "provider_id": self._normalize_optional_str(
                        candidate.get("provider_id")
                    )
                    or default_provider_id,
                    "persona_id": persona_id,
                    "tools": resolved_tools[:max_tools_per_agent],
                }
            )
            auto_profiles.append(
                {
                    "name": final_name,
                    "score": 0,
                    "keyword_score": 0,
                    "source": "llm",
                    "matched_tools": resolved_tools[:max_tools_per_agent],
                }
            )

            if len(agents) >= max_agents:
                break

        return {
            "main_enable": True,
            "remove_main_duplicate_tools": True,
            "agents": agents,
            "_auto_profiles": auto_profiles,
        }

    def _validate_config(
        self,
        config: dict[str, Any],
        tools: list[dict[str, Any]],
    ) -> dict[str, Any]:
        errors: list[str] = []
        warnings: list[str] = []
        tool_names = {
            str(tool.get("name")) for tool in tools if str(tool.get("name", "")).strip()
        }

        normalized = {
            "main_enable": bool(config.get("main_enable", False)),
            "remove_main_duplicate_tools": bool(
                config.get("remove_main_duplicate_tools", False)
            ),
            "agents": [],
        }

        agents = config.get("agents", [])
        if not isinstance(agents, list):
            errors.append("agents 必须为数组")
            agents = []

        if len(agents) > self.MAX_AGENT_COUNT:
            warnings.append(
                f"SubAgent 数量超过 {self.MAX_AGENT_COUNT}，将仅保留前 {self.MAX_AGENT_COUNT} 个"
            )
            agents = agents[: self.MAX_AGENT_COUNT]

        seen_names: set[str] = set()
        sorted_tool_names = sorted(tool_names)
        persona_tool_index = self._build_persona_tool_index()

        for index, agent in enumerate(agents):
            if not isinstance(agent, dict):
                errors.append(f"第 {index + 1} 个 SubAgent 不是对象")
                continue

            raw_name = str(agent.get("name", "")).strip()
            if not raw_name:
                errors.append(f"第 {index + 1} 个 SubAgent 缺少 name")
                continue
            if not self.AGENT_NAME_RE.match(raw_name):
                errors.append(
                    f"SubAgent 名称不合法: {raw_name}，仅允许英文小写字母/数字/下划线，且需以字母开头"
                )
            if raw_name in seen_names:
                errors.append(f"SubAgent 名称重复: {raw_name}")
            seen_names.add(raw_name)

            persona_id = self._normalize_optional_str(agent.get("persona_id"))

            raw_tools = agent.get("tools", [])
            if raw_tools is None:
                raw_tools = []
            if not isinstance(raw_tools, list):
                errors.append(f"SubAgent {raw_name} 的 tools 必须为数组")
                raw_tools = []

            valid_tools: list[str] = []
            for tool in raw_tools:
                tool_name = str(tool).strip()
                if not tool_name:
                    continue
                if tool_name not in tool_names:
                    warnings.append(
                        f"SubAgent {raw_name} 包含未注册工具: {tool_name}，已忽略"
                    )
                    continue
                valid_tools.append(tool_name)

            persona_bound_tools = self._resolve_persona_bound_tools(
                persona_id=persona_id,
                all_tool_names=sorted_tool_names,
                persona_tool_index=persona_tool_index,
            )
            if persona_bound_tools is not None:
                if valid_tools and valid_tools != persona_bound_tools:
                    warnings.append(
                        f"SubAgent {raw_name} 已绑定 persona {persona_id}，tools 以 persona 配置为准"
                    )
                valid_tools = persona_bound_tools

            if len(valid_tools) > self.MAX_TOOLS_PER_AGENT:
                warnings.append(
                    f"SubAgent {raw_name} 工具数量超过 {self.MAX_TOOLS_PER_AGENT}，将截断"
                )
                valid_tools = valid_tools[: self.MAX_TOOLS_PER_AGENT]

            normalized["agents"].append(
                {
                    "enabled": bool(agent.get("enabled", True)),
                    "name": raw_name,
                    "public_description": str(
                        agent.get("public_description", "")
                    ).strip(),
                    "system_prompt": str(agent.get("system_prompt", "")).strip(),
                    "provider_id": self._normalize_optional_str(
                        agent.get("provider_id")
                    ),
                    "persona_id": persona_id,
                    "tools": valid_tools,
                }
            )

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "normalized_config": normalized,
        }

    def _build_persona_tool_index(self) -> dict[str, list[str] | None]:
        index: dict[str, list[str] | None] = {}
        for persona in self._persona_mgr.persona_v3_config:
            if not isinstance(persona, dict):
                continue
            persona_id = self._normalize_optional_str(persona.get("name"))
            if not persona_id:
                continue
            tools = persona.get("tools")
            if tools is None:
                index[persona_id] = None
                continue
            if isinstance(tools, list):
                index[persona_id] = [
                    str(tool).strip() for tool in tools if str(tool).strip()
                ]
        index.setdefault("default", None)
        return index

    def _resolve_persona_bound_tools(
        self,
        persona_id: str | None,
        all_tool_names: list[str],
        persona_tool_index: dict[str, list[str] | None] | None = None,
    ) -> list[str] | None:
        if not persona_id:
            return None
        if persona_tool_index is None:
            persona_tool_index = self._build_persona_tool_index()
        persona_tools = persona_tool_index.get(persona_id)
        if persona_tools is None:
            if persona_id == "default":
                return list(dict.fromkeys(all_tool_names))
            return None
        return list(dict.fromkeys(persona_tools))

    def _resolve_default_subagent_provider_id(
        self,
        planner_payload: dict[str, Any],
    ) -> str | None:
        preferred = self._normalize_optional_str(
            planner_payload.get("default_provider_id")
        )
        if preferred:
            return preferred

        provider_settings = self.core_lifecycle.astrbot_config.get(
            "provider_settings", {}
        )
        if not isinstance(provider_settings, dict):
            return None
        return self._normalize_optional_str(
            provider_settings.get("default_provider_id")
        )

    def _normalize_max_tools_per_agent(self, value: Any) -> int:
        if value is None:
            return min(self.DEFAULT_AUTO_TOOLS_PER_AGENT, self.MAX_TOOLS_PER_AGENT)
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return min(self.DEFAULT_AUTO_TOOLS_PER_AGENT, self.MAX_TOOLS_PER_AGENT)
        return max(1, min(parsed, self.MAX_TOOLS_PER_AGENT))

    def _normalize_optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
