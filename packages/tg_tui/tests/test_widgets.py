"""Widget edge cases of the agent TUI (moved from tests/test_migrations_worker_bootstrap_paths.py, #1720)."""

from __future__ import annotations

from unittest.mock import MagicMock


def test_message_bubble_compose_user():
    """Covers MessageBubble compose for user role."""
    from tg_tui.app import MessageBubble

    bubble = MessageBubble("user", "hello")
    assert "user-bubble" in bubble.classes
    assert bubble.border_title == "Вы"


def test_message_bubble_compose_assistant():
    """Covers MessageBubble compose for assistant role."""
    from tg_tui.app import MessageBubble

    bubble = MessageBubble("assistant", "response")
    assert "assistant-bubble" in bubble.classes
    assert bubble.border_title == "Агент"


def test_streaming_message_replace_pending_status():
    """Covers replace_pending_status without flushing."""
    from tg_tui.app import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget.replace_pending_status("countdown 5s")
    assert widget._pending_status == "countdown 5s"
    assert widget._status_label == "countdown 5s"
    assert widget._tool_start_time == 0.0


def test_streaming_message_set_pending_status_duplicate():
    """Covers set_pending_status skipping duplicate labels."""
    from tg_tui.app import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._activity_log = MagicMock()
    widget.set_pending_status("Жду ответ")
    widget.set_pending_status("Жду ответ")  # duplicate -- should skip
    assert widget._pending_status == "Жду ответ"


def test_streaming_message_do_render():
    """Covers _do_render updates markdown widget."""
    from tg_tui.app import StreamingMessage

    widget = StreamingMessage()
    widget._pending_render = True
    mock_md = MagicMock()
    widget._md = mock_md
    widget._content = "rendered text"
    widget._do_render()
    assert widget._pending_render is False
    mock_md.update.assert_called_once_with("rendered text")


def test_streaming_message_tick_elapsed_loading_false():
    """Covers _tick_elapsed when loading is False (early return)."""
    from tg_tui.app import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._loading = False
    widget._tick_elapsed()
    widget._elapsed_label.update.assert_not_called()


def test_streaming_message_tick_elapsed_default():
    """Covers _tick_elapsed with default status (no status label, no tool)."""
    import time as _time

    from tg_tui.app import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._loading = True
    widget._status_label = ""
    widget._tool_start_time = 0.0
    widget._start_time = _time.monotonic()
    widget._tick_elapsed()
    call_arg = widget._elapsed_label.update.call_args[0][0]
    assert "(0s)" in call_arg


def test_chat_input_class_exists():
    """Covers ChatInput import and class definition."""
    from tg_tui.app import ChatInput

    assert ChatInput is not None


def test_permission_dialog_class():
    """Covers PermissionDialog class definition and compose."""
    from tg_tui.app import PermissionDialog

    dialog = PermissionDialog("test_tool", "+123456")
    assert dialog._tool_name == "test_tool"
    assert dialog._phone == "+123456"


def test_permission_dialog_no_phone():
    """Covers PermissionDialog with empty phone."""
    from tg_tui.app import PermissionDialog

    dialog = PermissionDialog("test_tool", "")
    assert dialog._phone == ""


def test_thread_sidebar_class():
    """Covers ThreadSidebar class definition."""
    from tg_tui.app import ThreadSidebar

    assert ThreadSidebar is not None


def test_thread_selected_message():
    """Covers ThreadSelected message."""
    from tg_tui.app import ThreadSelected

    msg = ThreadSelected(42)
    assert msg.thread_id == 42
