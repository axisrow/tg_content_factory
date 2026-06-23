"""Architecture tests for the Telegram action contract."""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

from src.agent.tools import build_agent_tool_registry
from src.services.telegram_action_inventory import TELEGRAM_ACTION_INVENTORY

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_TELETHON_ALLOWED = {
    Path("src/telegram/backends.py"),
    Path("src/telegram/auth.py"),
}

ENTRYPOINT_ROOTS = (
    Path("src/cli"),
    Path("src/web"),
    Path("src/agent/tools"),
    Path("src/services/pipeline_nodes"),
)
ENTRYPOINT_FILES = {
    Path("src/services/telegram_command_dispatcher.py"),
    # Per-domain dispatcher mixins (#1047) must keep delegating Telegram actions
    # through TelegramActionService, same as the facade they were split out of.
    Path("src/services/dispatcher/dialogs_mixin.py"),
    Path("src/services/dispatcher/channels_mixin.py"),
    Path("src/services/dispatcher/accounts_mixin.py"),
    Path("src/services/dispatcher/auth_mixin.py"),
    Path("src/services/dispatcher/scheduler_mixin.py"),
    Path("src/services/dispatcher/notifications_mixin.py"),
    Path("src/services/dispatcher/photo_mixin.py"),
    Path("src/services/dispatcher/search_mixin.py"),
    Path("src/services/dispatcher/moderation_mixin.py"),
}
ENTRYPOINT_FORBIDDEN_ACTION_CALLS = {
    "send_message",
    "edit_message",
    "delete_messages",
    "forward_messages",
    "pin_message",
    "unpin_message",
    "get_participants",
    "get_broadcast_stats",
    "edit_admin",
    "edit_permissions",
    "kick_participant",
    "send_read_acknowledge",
    "edit_folder",
    "download_media",
    "leave_channels",
    "join_channel",
    "import_chat_invite",
    "send_reaction",
}


def test_telegram_action_inventory_has_unique_complete_actions():
    actions = [item.action for item in TELEGRAM_ACTION_INVENTORY]
    assert len(actions) == len(set(actions))
    assert "send_reaction" in actions
    assert "create_channel" in actions
    assert "download_media" in actions
    assert "join_channel" in actions
    assert "leave_dialogs" in actions
    for item in TELEGRAM_ACTION_INVENTORY:
        assert item.backend_method, item.action
        assert any((item.cli, item.web_command, item.agent_tool, item.pipeline_node)), item.action


def test_telegram_action_inventory_agent_tools_are_registered():
    registered = {
        tool.name for tool in build_agent_tool_registry(MagicMock(), client_pool=MagicMock(), wrap_session_gate=False)
    }
    missing = [
        f"{item.action}:{item.agent_tool}"
        for item in TELEGRAM_ACTION_INVENTORY
        if item.agent_tool and item.agent_tool not in registered
    ]
    assert missing == []


def _is_entrypoint_path(relative: Path) -> bool:
    return relative in ENTRYPOINT_FILES or any(relative.is_relative_to(root) for root in ENTRYPOINT_ROOTS)


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_telegram_action_service_receiver(node: ast.AST) -> bool:
    if isinstance(node, ast.Name) and node.id == "action_service":
        return True
    if isinstance(node, ast.Call):
        return _call_name(node.func) == "TelegramActionService"
    return False


def test_raw_telethon_function_imports_stay_in_backend_allowlist():
    offenders: list[str] = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative = path.relative_to(PROJECT_ROOT)
        if relative in RAW_TELETHON_ALLOWED:
            continue
        tree = ast.parse(path.read_text(), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("telethon.tl.functions"):
                offenders.append(f"{relative}:{node.lineno} imports {node.module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("telethon.tl.functions"):
                        offenders.append(f"{relative}:{node.lineno} imports {alias.name}")
    assert offenders == []


def test_raw_telethon_request_constructors_stay_in_backend_allowlist():
    offenders: list[str] = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative = path.relative_to(PROJECT_ROOT)
        if relative in RAW_TELETHON_ALLOWED:
            continue
        tree = ast.parse(path.read_text(), filename=str(relative))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = _call_name(node.func)
                if name and name != "Request" and name.endswith("Request"):
                    offenders.append(f"{relative}:{node.lineno} constructs {name}")
    assert offenders == []


def test_entrypoints_delegate_telegram_actions_to_service_contract():
    offenders: list[str] = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative = path.relative_to(PROJECT_ROOT)
        if not _is_entrypoint_path(relative):
            continue
        tree = ast.parse(path.read_text(), filename=str(relative))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if _is_telegram_action_service_receiver(node.func.value):
                continue
            if node.func.attr in ENTRYPOINT_FORBIDDEN_ACTION_CALLS:
                offenders.append(f"{relative}:{node.lineno} calls .{node.func.attr}() directly")
    assert offenders == []
