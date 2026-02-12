import asyncio
import json
import os
import uuid

import pytest
import pytest_asyncio
from quart import Quart

from astrbot.core import LogBroker
from astrbot.core.core_lifecycle import AstrBotCoreLifecycle
from astrbot.core.db.sqlite import SQLiteDatabase
from astrbot.core.star.star import star_registry
from astrbot.core.star.star_handler import star_handlers_registry
from astrbot.dashboard.server import AstrBotDashboard


@pytest_asyncio.fixture(scope="module")
async def core_lifecycle_td(tmp_path_factory):
    """Creates and initializes a core lifecycle instance with a temporary database."""
    tmp_db_path = tmp_path_factory.mktemp("data") / "test_data_v3.db"
    db = SQLiteDatabase(str(tmp_db_path))
    log_broker = LogBroker()
    core_lifecycle = AstrBotCoreLifecycle(log_broker, db)
    await core_lifecycle.initialize()
    try:
        yield core_lifecycle
    finally:
        # 优先停止核心生命周期以释放资源（包括关闭 MCP 等后台任务）
        try:
            _stop_res = core_lifecycle.stop()
            if asyncio.iscoroutine(_stop_res):
                await _stop_res
        except Exception:
            # 停止过程中如有异常，不影响后续清理
            pass


@pytest.fixture(scope="module")
def app(core_lifecycle_td: AstrBotCoreLifecycle):
    """Creates a Quart app instance for testing."""
    shutdown_event = asyncio.Event()
    # The db instance is already part of the core_lifecycle_td
    server = AstrBotDashboard(core_lifecycle_td, core_lifecycle_td.db, shutdown_event)
    return server.app


@pytest_asyncio.fixture(scope="module")
async def authenticated_header(app: Quart, core_lifecycle_td: AstrBotCoreLifecycle):
    """Handles login and returns an authenticated header."""
    test_client = app.test_client()
    response = await test_client.post(
        "/api/auth/login",
        json={
            "username": core_lifecycle_td.astrbot_config["dashboard"]["username"],
            "password": core_lifecycle_td.astrbot_config["dashboard"]["password"],
        },
    )
    data = await response.get_json()
    assert data["status"] == "ok"
    token = data["data"]["token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_auth_login(app: Quart, core_lifecycle_td: AstrBotCoreLifecycle):
    """Tests the login functionality with both wrong and correct credentials."""
    test_client = app.test_client()
    response = await test_client.post(
        "/api/auth/login",
        json={"username": "wrong", "password": "password"},
    )
    data = await response.get_json()
    assert data["status"] == "error"

    response = await test_client.post(
        "/api/auth/login",
        json={
            "username": core_lifecycle_td.astrbot_config["dashboard"]["username"],
            "password": core_lifecycle_td.astrbot_config["dashboard"]["password"],
        },
    )
    data = await response.get_json()
    assert data["status"] == "ok" and "token" in data["data"]


@pytest.mark.asyncio
async def test_get_stat(app: Quart, authenticated_header: dict):
    test_client = app.test_client()
    response = await test_client.get("/api/stat/get")
    assert response.status_code == 401
    response = await test_client.get("/api/stat/get", headers=authenticated_header)
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok" and "platform" in data["data"]


@pytest.mark.asyncio
async def test_plugins(app: Quart, authenticated_header: dict):
    test_client = app.test_client()
    # 已经安装的插件
    response = await test_client.get("/api/plugin/get", headers=authenticated_header)
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"

    # 插件市场
    response = await test_client.get(
        "/api/plugin/market_list",
        headers=authenticated_header,
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"

    # 插件安装
    response = await test_client.post(
        "/api/plugin/install",
        json={"url": "https://github.com/Soulter/astrbot_plugin_essential"},
        headers=authenticated_header,
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    exists = False
    for md in star_registry:
        if md.name == "astrbot_plugin_essential":
            exists = True
            break
    assert exists is True, "插件 astrbot_plugin_essential 未成功载入"

    # 插件更新
    response = await test_client.post(
        "/api/plugin/update",
        json={"name": "astrbot_plugin_essential"},
        headers=authenticated_header,
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"

    # 插件卸载
    response = await test_client.post(
        "/api/plugin/uninstall",
        json={"name": "astrbot_plugin_essential"},
        headers=authenticated_header,
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    exists = False
    for md in star_registry:
        if md.name == "astrbot_plugin_essential":
            exists = True
            break
    assert exists is False, "插件 astrbot_plugin_essential 未成功卸载"
    exists = False
    for md in star_handlers_registry:
        if "astrbot_plugin_essential" in md.handler_module_path:
            exists = True
            break
    assert exists is False, "插件 astrbot_plugin_essential 未成功卸载"


@pytest.mark.asyncio
async def test_commands_api(app: Quart, authenticated_header: dict):
    """Tests the command management API endpoints."""
    test_client = app.test_client()

    # GET /api/commands - list commands
    response = await test_client.get("/api/commands", headers=authenticated_header)
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    assert "items" in data["data"]
    assert "summary" in data["data"]
    summary = data["data"]["summary"]
    assert "total" in summary
    assert "disabled" in summary
    assert "conflicts" in summary

    # GET /api/commands/conflicts - list conflicts
    response = await test_client.get(
        "/api/commands/conflicts", headers=authenticated_header
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    # conflicts is a list
    assert isinstance(data["data"], list)


@pytest.mark.asyncio
async def test_subagent_auto_plan_validate_apply(
    app: Quart,
    authenticated_header: dict,
    core_lifecycle_td: AstrBotCoreLifecycle,
    monkeypatch,
):
    test_client = app.test_client()

    tools_resp = await test_client.get(
        "/api/subagent/available-tools",
        headers=authenticated_header,
    )
    assert tools_resp.status_code == 200
    tools_data = await tools_resp.get_json()
    assert tools_data["status"] == "ok"
    tools = tools_data["data"]
    assert isinstance(tools, list)
    if tools:
        first_tool = tools[0]
        assert "name" in first_tool
        assert "description" in first_tool

    selected_tools = [
        str(item.get("name", "")).strip()
        for item in tools[:2]
        if str(item.get("name", "")).strip()
    ]

    class _FakePlannerProvider:
        async def text_chat(self, *args, **kwargs):
            class _Resp:
                completion_text = json.dumps(
                    {
                        "subagents": [
                            {
                                "name": "doc_search_agent",
                                "public_description": "search and summarize",
                                "system_prompt": "focus on retrieval and summary",
                                "persona_id": "doc_search_agent_persona",
                                "tools": selected_tools,
                            }
                        ]
                    },
                    ensure_ascii=False,
                )

            return _Resp()

    monkeypatch.setattr(
        core_lifecycle_td.provider_manager,
        "get_using_provider",
        lambda provider_type, umo=None: _FakePlannerProvider(),
    )

    plan_resp = await test_client.post(
        "/api/subagent/auto-plan",
        headers=authenticated_header,
        json={"goal": "网页搜索和文档总结", "max_agents": 2},
    )
    assert plan_resp.status_code == 200
    plan_data = await plan_resp.get_json()
    assert plan_data["status"] == "ok"
    planner = plan_data["data"].get("planner", {})
    assert planner.get("engine") == "llm"

    draft_config = plan_data["data"]["draft_config"]
    assert isinstance(draft_config, dict)
    assert isinstance(draft_config.get("agents"), list)
    assert len(draft_config["agents"]) >= 1

    apply_resp = await test_client.post(
        "/api/subagent/auto-apply",
        headers=authenticated_header,
        json={
            "config": draft_config,
            "allow_warnings": True,
        },
    )
    assert apply_resp.status_code == 200
    apply_data = await apply_resp.get_json()
    assert apply_data["status"] == "ok"
    assert apply_data["data"]["applied"] is True

    cfg_resp = await test_client.get(
        "/api/subagent/config", headers=authenticated_header
    )
    assert cfg_resp.status_code == 200
    cfg_data = await cfg_resp.get_json()
    assert cfg_data["status"] == "ok"
    assert cfg_data["data"]["main_enable"] is True
    assert isinstance(cfg_data["data"]["agents"], list)
    assert len(cfg_data["data"]["agents"]) >= 1


@pytest.mark.asyncio
async def test_subagent_auto_apply_auto_creates_missing_persona(
    app: Quart,
    authenticated_header: dict,
    core_lifecycle_td: AstrBotCoreLifecycle,
    monkeypatch,
):
    test_client = app.test_client()

    tools_resp = await test_client.get(
        "/api/subagent/available-tools",
        headers=authenticated_header,
    )
    assert tools_resp.status_code == 200
    tools_data = await tools_resp.get_json()
    assert tools_data["status"] == "ok"
    tools = tools_data["data"]
    assert isinstance(tools, list)
    assert len(tools) > 0
    tool_name = str(tools[0]["name"])

    new_persona_id = f"auto_persona_{uuid.uuid4().hex[:8]}"

    class _FakePlannerProvider:
        async def text_chat(self, *args, **kwargs):
            class _Resp:
                completion_text = json.dumps(
                    {
                        "subagents": [
                            {
                                "name": "auto_persona_agent",
                                "public_description": "test",
                                "system_prompt": "follow goal",
                                "persona_id": new_persona_id,
                                "tools": [tool_name],
                            }
                        ]
                    },
                    ensure_ascii=False,
                )

            return _Resp()

    fake_provider = _FakePlannerProvider()
    monkeypatch.setattr(
        core_lifecycle_td.provider_manager,
        "get_using_provider",
        lambda provider_type, umo=None: fake_provider,
    )

    plan_resp = await test_client.post(
        "/api/subagent/auto-plan",
        headers=authenticated_header,
        json={"goal": "自动创建人格并应用", "max_agents": 1},
    )
    assert plan_resp.status_code == 200
    plan_data = await plan_resp.get_json()
    assert plan_data["status"] == "ok"

    draft_config = plan_data["data"]["draft_config"]
    apply_resp = await test_client.post(
        "/api/subagent/auto-apply",
        headers=authenticated_header,
        json={
            "config": draft_config,
            "allow_warnings": True,
            "auto_create_persona": True,
        },
    )
    assert apply_resp.status_code == 200
    apply_data = await apply_resp.get_json()
    assert apply_data["status"] == "ok"
    assert new_persona_id in apply_data["data"]["created_personas"]

    created_persona = await core_lifecycle_td.persona_mgr.get_persona(new_persona_id)
    assert created_persona.persona_id == new_persona_id


@pytest.mark.asyncio
async def test_subagent_auto_apply_warn_and_error(
    app: Quart, authenticated_header: dict
):
    test_client = app.test_client()

    warning_config = {
        "main_enable": True,
        "remove_main_duplicate_tools": True,
        "agents": [
            {
                "name": "warn_agent",
                "enabled": True,
                "public_description": "warn",
                "system_prompt": "",
                "provider_id": None,
                "persona_id": None,
                "tools": ["tool_not_exists"],
            }
        ],
    }

    warn_resp = await test_client.post(
        "/api/subagent/auto-apply",
        headers=authenticated_header,
        json={"config": warning_config, "allow_warnings": False},
    )
    assert warn_resp.status_code == 200
    warn_data = await warn_resp.get_json()
    assert warn_data["status"] == "error"
    assert "warnings" in warn_data["data"]
    assert len(warn_data["data"]["warnings"]) >= 1

    error_config = {
        "main_enable": True,
        "remove_main_duplicate_tools": True,
        "agents": [
            {
                "name": "dup_agent",
                "enabled": True,
                "public_description": "a",
                "system_prompt": "",
                "provider_id": None,
                "persona_id": None,
                "tools": [],
            },
            {
                "name": "dup_agent",
                "enabled": True,
                "public_description": "b",
                "system_prompt": "",
                "provider_id": None,
                "persona_id": None,
                "tools": [],
            },
        ],
    }

    error_resp = await test_client.post(
        "/api/subagent/auto-apply",
        headers=authenticated_header,
        json={"config": error_config, "allow_warnings": True},
    )
    assert error_resp.status_code == 200
    error_data = await error_resp.get_json()
    assert error_data["status"] == "error"
    assert "errors" in error_data["data"]
    assert len(error_data["data"]["errors"]) >= 1


@pytest.mark.asyncio
async def test_check_update(app: Quart, authenticated_header: dict):
    test_client = app.test_client()
    response = await test_client.get("/api/update/check", headers=authenticated_header)
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "success"


@pytest.mark.asyncio
async def test_do_update(
    app: Quart,
    authenticated_header: dict,
    core_lifecycle_td: AstrBotCoreLifecycle,
    monkeypatch,
    tmp_path_factory,
):
    test_client = app.test_client()

    # Use a temporary path for the mock update to avoid side effects
    temp_release_dir = tmp_path_factory.mktemp("release")
    release_path = temp_release_dir / "astrbot"

    async def mock_update(*args, **kwargs):
        """Mocks the update process by creating a directory in the temp path."""
        os.makedirs(release_path, exist_ok=True)

    async def mock_download_dashboard(*args, **kwargs):
        """Mocks the dashboard download to prevent network access."""
        return

    async def mock_pip_install(*args, **kwargs):
        """Mocks pip install to prevent actual installation."""
        return

    monkeypatch.setattr(core_lifecycle_td.astrbot_updator, "update", mock_update)
    monkeypatch.setattr(
        "astrbot.dashboard.routes.update.download_dashboard",
        mock_download_dashboard,
    )
    monkeypatch.setattr(
        "astrbot.dashboard.routes.update.pip_installer.install",
        mock_pip_install,
    )

    response = await test_client.post(
        "/api/update/do",
        headers=authenticated_header,
        json={"version": "v3.4.0", "reboot": False},
    )
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    assert os.path.exists(release_path)
