"""Socket客户端处理器

负责处理单个Socket客户端连接。
"""

import asyncio
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING
from urllib import error
from urllib import request as urlrequest

from astrbot import logger
from astrbot.core.message.message_event_result import MessageChain

from ..interfaces import IHandler, IMessageConverter, ISessionManager, ITokenValidator
from ..message.response_builder import ResponseBuilder

if TYPE_CHECKING:
    from astrbot.core.platform.platform_metadata import PlatformMetadata

    from ..cli_event import CLIMessageEvent


class SocketClientHandler:
    """Socket客户端处理器

    处理单个客户端连接，不实现IHandler（因为它不是独立运行的模式）。

    I/O契约:
        Input: socket连接
        Output: None (发送JSON响应到客户端)
    """

    RECV_BUFFER_SIZE = 4096
    MAX_REQUEST_SIZE = 1024 * 1024  # 1MB 最大请求大小
    RESPONSE_TIMEOUT = 120.0

    def __init__(
        self,
        token_manager: ITokenValidator,
        message_converter: IMessageConverter,
        session_manager: ISessionManager,
        platform_meta: "PlatformMetadata",
        output_queue: asyncio.Queue,
        event_committer: Callable[["CLIMessageEvent"], None],
        use_isolated_sessions: bool = False,
        data_path: str | None = None,
    ):
        """初始化Socket客户端处理器"""
        self.token_manager = token_manager
        self.message_converter = message_converter
        self.session_manager = session_manager
        self.platform_meta = platform_meta
        self.output_queue = output_queue
        self.event_committer = event_committer
        self.use_isolated_sessions = use_isolated_sessions
        self.data_path = data_path or os.path.join(os.getcwd(), "data")
        self._dashboard_token_lock = threading.Lock()
        self._dashboard_token_cache: dict[str, str | float | None] = {
            "token": None,
            "expires_at": 0.0,
            "base_url": None,
            "username": None,
        }

    async def handle(self, client_socket) -> None:
        """处理单个客户端连接"""
        try:
            loop = asyncio.get_running_loop()

            # 接收请求（带大小限制）
            data = await self._recv_with_limit(loop, client_socket)
            if not data:
                return

            # 解析并验证请求
            request = self._parse_request(data)
            if request is None:
                await self._send_response(
                    loop,
                    client_socket,
                    ResponseBuilder.build_error("Invalid JSON format"),
                )
                return

            request_id = request.get("request_id", str(uuid.uuid4()))
            auth_token = request.get("auth_token", "")
            action = request.get("action", "")

            # Token验证（所有请求都需要token）
            if not self.token_manager.validate(auth_token):
                error_msg = (
                    "Unauthorized: missing token"
                    if not auth_token
                    else "Unauthorized: invalid token"
                )
                await self._send_response(
                    loop,
                    client_socket,
                    ResponseBuilder.build_error(error_msg, request_id, "AUTH_FAILED"),
                )
                return

            # 处理请求
            if action == "get_logs":
                response = await self._get_logs(request, request_id)
            elif action == "subagent_auto_plan":
                response = await self._subagent_auto_plan(request, request_id)
            elif action == "subagent_auto_apply":
                response = await self._subagent_auto_apply(request, request_id)
            else:
                message_text = request.get("message", "")
                response = await self._process_message(message_text, request_id)

            await self._send_response(loop, client_socket, response)

        except Exception as e:
            logger.error("Socket handler error: %s", e, exc_info=True)
        finally:
            try:
                client_socket.close()
            except Exception as e:
                logger.warning("Failed to close socket: %s", e)

    async def _recv_with_limit(self, loop, client_socket) -> bytes:
        """接收数据，带大小限制防止DoS攻击"""
        chunks = []
        total_size = 0

        while True:
            chunk = await loop.sock_recv(client_socket, self.RECV_BUFFER_SIZE)
            if not chunk:
                break

            total_size += len(chunk)
            if total_size > self.MAX_REQUEST_SIZE:
                logger.warning(
                    "Request too large: %d bytes, limit: %d",
                    total_size,
                    self.MAX_REQUEST_SIZE,
                )
                return b""

            chunks.append(chunk)

            # 检查是否接收完整（JSON以}结尾）
            if chunk.rstrip().endswith(b"}"):
                break

        return b"".join(chunks)

    def _parse_request(self, data: bytes) -> dict | None:
        """解析JSON请求"""
        try:
            return json.loads(data.decode("utf-8"))
        except json.JSONDecodeError:
            return None

    async def _send_response(self, loop, client_socket, response: str) -> None:
        """发送响应"""
        await loop.sock_sendall(client_socket, response.encode("utf-8"))

    async def _process_message(self, message_text: str, request_id: str) -> str:
        """处理消息并返回JSON响应"""
        from ..cli_event import CLIMessageEvent

        response_future = asyncio.Future()

        message = self.message_converter.convert(
            message_text,
            request_id=request_id,
            use_isolated_session=self.use_isolated_sessions,
        )

        self.session_manager.register(message.session_id)

        message_event = CLIMessageEvent(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.platform_meta,
            session_id=message.session_id,
            output_queue=self.output_queue,
            response_future=response_future,
        )

        self.event_committer(message_event)

        try:
            message_chain = await asyncio.wait_for(
                response_future, timeout=self.RESPONSE_TIMEOUT
            )
            if message_chain is None:
                # 管道完成但没有产生任何回复（被白名单/频率限制等拦截）
                return ResponseBuilder.build_success(MessageChain([]), request_id)
            return ResponseBuilder.build_success(message_chain, request_id)
        except asyncio.TimeoutError:
            return ResponseBuilder.build_error("Request timeout", request_id, "TIMEOUT")

    async def _get_logs(self, request: dict, request_id: str) -> str:
        """获取日志

        Args:
            request: 请求字典，支持参数:
                - lines: 返回最近N行日志（默认100）
                - level: 过滤日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
                - pattern: 过滤包含指定字符串的日志
            request_id: 请求ID

        Returns:
            JSON格式的响应字符串
        """
        # 日志级别映射：完整名称 -> 日志文件中的缩写
        LEVEL_MAP = {
            "DEBUG": "DEBUG",
            "INFO": "INFO",
            "WARNING": "WARN",
            "WARN": "WARN",
            "ERROR": "ERRO",
            "CRITICAL": "CRIT",
        }

        try:
            # 获取参数
            lines = min(request.get("lines", 100), 1000)  # 最多1000行
            level_filter = request.get("level", "").upper()
            # 映射到日志文件中的缩写
            level_filter = LEVEL_MAP.get(level_filter, level_filter)
            pattern = request.get("pattern", "")
            use_regex = request.get("regex", False)  # 是否使用正则表达式

            logger.debug(
                f"[LogFilter] lines={lines}, level={level_filter}, pattern={repr(pattern)}, regex={use_regex}"
            )

            # 日志文件路径
            log_path = os.path.join(self.data_path, "logs", "astrbot.log")

            if not os.path.exists(log_path):  # noqa: ASYNC240
                return json.dumps(
                    {
                        "status": "success",
                        "response": "",
                        "message": "日志文件未找到。请在配置中启用 log_file_enable 来记录日志到文件。",
                        "request_id": request_id,
                    },
                    ensure_ascii=False,
                )

            # 读取日志文件（从末尾开始）
            logs = []
            try:
                with open(log_path, encoding="utf-8", errors="ignore") as f:
                    # 读取所有行
                    all_lines = f.readlines()

                # 从末尾开始筛选
                for line in reversed(all_lines):
                    # 跳过空行
                    if not line.strip():
                        continue

                    # 级别过滤（匹配 [级别] 格式）
                    if level_filter:
                        # 匹配 [级别] 格式，例如 [ERRO], [WARN], [INFO]
                        if not re.search(rf"\[{level_filter}\]", line):
                            continue

                    # 模式过滤（支持正则表达式）
                    if pattern:
                        if use_regex:
                            try:
                                if not re.search(pattern, line):
                                    continue
                            except re.error:
                                # 正则表达式错误，回退到子串匹配
                                if pattern not in line:
                                    continue
                        else:
                            if pattern not in line:
                                continue

                    logs.append(line.rstrip())

                    if len(logs) >= lines:
                        break

            except OSError as e:
                logger.warning("Failed to read log file: %s", e)
                return ResponseBuilder.build_error(
                    f"Failed to read log file: {e}", request_id
                )

            # 反转回来（使时间顺序正确）
            logs.reverse()

            # 构建响应
            log_text = "\n".join(logs)
            return json.dumps(
                {
                    "status": "success",
                    "response": log_text,
                    "message": f"Retrieved {len(logs)} log lines",
                    "request_id": request_id,
                },
                ensure_ascii=False,
            )

        except Exception as e:
            logger.exception("Error getting logs")
            return ResponseBuilder.build_error(f"Error getting logs: {e}", request_id)

    async def _subagent_auto_plan(self, request: dict, request_id: str) -> str:
        timeout = self._normalize_timeout(request.get("timeout", 20.0))
        payload = {
            "goal": request.get("goal", ""),
            "max_agents": request.get("max_agents", 2),
            "max_tools_per_agent": request.get("max_tools_per_agent", 4),
        }
        if request.get("default_provider_id"):
            payload["default_provider_id"] = request.get("default_provider_id")
        if request.get("default_persona_id"):
            payload["default_persona_id"] = request.get("default_persona_id")

        return await self._proxy_subagent_request(
            endpoint="/api/subagent/auto-plan",
            payload=payload,
            request_id=request_id,
            timeout=timeout,
        )

    async def _subagent_auto_apply(self, request: dict, request_id: str) -> str:
        timeout = self._normalize_timeout(request.get("timeout", 20.0))
        payload = {
            "config": request.get("config", {}),
            "allow_warnings": bool(request.get("allow_warnings", False)),
            "auto_create_persona": bool(request.get("auto_create_persona", True)),
        }
        return await self._proxy_subagent_request(
            endpoint="/api/subagent/auto-apply",
            payload=payload,
            request_id=request_id,
            timeout=timeout,
        )

    async def _proxy_subagent_request(
        self,
        endpoint: str,
        payload: dict,
        request_id: str,
        timeout: float,
    ) -> str:
        try:
            response = await asyncio.to_thread(
                self._request_subagent_api,
                endpoint,
                payload,
                timeout,
            )
            response["request_id"] = request_id
            return json.dumps(response, ensure_ascii=False)
        except Exception as e:
            logger.exception("Subagent socket proxy failed")
            return ResponseBuilder.build_error(
                f"Subagent request failed: {e}",
                request_id,
            )

    def _request_subagent_api(
        self, endpoint: str, payload: dict, timeout: float
    ) -> dict:
        dashboard = self._load_dashboard_config()
        username = str(dashboard.get("username", "")).strip()
        password = str(dashboard.get("password", "")).strip()
        host = str(dashboard.get("host", "127.0.0.1") or "127.0.0.1").strip()
        port = dashboard.get("port", 6185)

        if not username or not password:
            raise RuntimeError("Dashboard credentials are not configured")

        if host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"

        base_url = f"http://{host}:{port}"
        token = self._get_dashboard_token(base_url, username, password, timeout)
        response = self._http_json_request(
            "POST",
            f"{base_url}{endpoint}",
            payload,
            token=token,
            timeout=timeout,
        )
        if self._is_dashboard_auth_error(response):
            self._clear_dashboard_token_cache()
            retry_token = self._get_dashboard_token(
                base_url, username, password, timeout
            )
            response = self._http_json_request(
                "POST",
                f"{base_url}{endpoint}",
                payload,
                token=retry_token,
                timeout=timeout,
            )
        return response

    def _load_dashboard_config(self) -> dict:
        config_path = os.path.join(self.data_path, "cmd_config.json")
        try:
            config = None
            for encoding in ("utf-8", "utf-8-sig"):
                try:
                    with open(config_path, encoding=encoding) as f:
                        config = json.load(f)
                    break
                except json.JSONDecodeError:
                    continue

            if config is None:
                raise RuntimeError("Invalid config JSON")

            if isinstance(config, dict):
                dashboard = config.get("dashboard", {})
                if isinstance(dashboard, dict):
                    return dashboard
        except FileNotFoundError:
            raise RuntimeError(f"Config file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid config JSON: {e}")
        except OSError as e:
            raise RuntimeError(f"Failed to read config: {e}")
        return {}

    def _normalize_timeout(self, timeout: object) -> float:
        try:
            value = float(timeout)
        except (TypeError, ValueError):
            return 20.0
        return max(5.0, min(value, 300.0))

    def _clear_dashboard_token_cache(self) -> None:
        with self._dashboard_token_lock:
            self._dashboard_token_cache = {
                "token": None,
                "expires_at": 0.0,
                "base_url": None,
                "username": None,
            }

    def _get_dashboard_token(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float,
    ) -> str:
        now = time.time()
        with self._dashboard_token_lock:
            token = self._dashboard_token_cache.get("token")
            expires_at = float(self._dashboard_token_cache.get("expires_at") or 0.0)
            cached_base = self._dashboard_token_cache.get("base_url")
            cached_username = self._dashboard_token_cache.get("username")
            if (
                isinstance(token, str)
                and token
                and now < expires_at
                and cached_base == base_url
                and cached_username == username
            ):
                return token

            login_resp = self._http_json_request(
                "POST",
                f"{base_url}/api/auth/login",
                {
                    "username": username,
                    "password": password,
                },
                timeout=timeout,
            )
            if login_resp.get("status") != "ok":
                message = str(login_resp.get("message") or "dashboard login failed")
                raise RuntimeError(message)

            data = login_resp.get("data")
            refreshed_token = data.get("token") if isinstance(data, dict) else None
            if not refreshed_token:
                raise RuntimeError("Dashboard login did not return token")

            token_ttl = min(600.0, max(30.0, self._normalize_timeout(timeout) * 3.0))
            self._dashboard_token_cache = {
                "token": str(refreshed_token),
                "expires_at": now + token_ttl,
                "base_url": base_url,
                "username": username,
            }
            return str(refreshed_token)

    def _is_dashboard_auth_error(self, response: dict) -> bool:
        if not isinstance(response, dict):
            return False
        if str(response.get("status", "")).lower() != "error":
            return False
        message = str(response.get("message", "")).lower()
        return "token" in message or "未授权" in message

    def _http_json_request(
        self,
        method: str,
        url: str,
        payload: dict,
        token: str | None = None,
        timeout: float = 20.0,
    ) -> dict:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        req = urlrequest.Request(url, data=data, headers=headers, method=method)
        try:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    return parsed
                raise RuntimeError("Unexpected dashboard response")
        except error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code}: {body}")
        except error.URLError as e:
            raise RuntimeError(f"Cannot connect to dashboard: {e.reason}")


class SocketModeHandler(IHandler):
    """Socket模式处理器

    管理Socket服务器的生命周期，实现IHandler接口。
    """

    def __init__(
        self,
        server,
        client_handler: SocketClientHandler,
        connection_info_writer: Callable[[dict, str], None],
        data_path: str,
    ):
        """初始化Socket模式处理器

        Args:
            server: Socket服务器实例
            client_handler: 客户端处理器
            connection_info_writer: 连接信息写入函数
            data_path: 数据目录路径
        """
        self.server = server
        self.client_handler = client_handler
        self.connection_info_writer = connection_info_writer
        self.data_path = data_path
        self._running = False

    async def run(self) -> None:
        """运行Socket服务器"""
        self._running = True

        try:
            await self.server.start()
            logger.info("Socket server started: %s", type(self.server).__name__)

            # 写入连接信息
            connection_info = self.server.get_connection_info()
            self.connection_info_writer(connection_info, self.data_path)

            # 接受连接循环
            while self._running:
                try:
                    client_socket, _ = await self.server.accept_connection()
                    asyncio.create_task(self.client_handler.handle(client_socket))
                except Exception as e:
                    if self._running:
                        logger.error("Socket accept error: %s", e)
                    await asyncio.sleep(0.1)

        finally:
            await self.server.stop()

    def stop(self) -> None:
        """停止Socket服务器"""
        self._running = False
