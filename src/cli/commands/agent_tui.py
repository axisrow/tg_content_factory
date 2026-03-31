from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from textual import events, on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Button, Footer, Header, Label, ListItem, ListView, Markdown, Static, TextArea
from textual.worker import WorkerState

if TYPE_CHECKING:
    from src.agent.manager import AgentManager
    from src.config import AppConfig
    from src.database import Database

logger = logging.getLogger(__name__)

CSS_PATH = Path(__file__).parent / "agent_tui.tcss"


class ThreadItem(Widget):
    """Clickable thread item in sidebar."""

    DEFAULT_CSS = ""

    def __init__(self, thread_id: int, title: str, active: bool = False) -> None:
        super().__init__()
        self.thread_id = thread_id
        self._title = title
        self.tooltip = "Ctrl+D — удалить тред"
        if active:
            self.add_class("active")

    def compose(self) -> ComposeResult:
        yield Label(self._title)

    def on_click(self) -> None:
        self.app.post_message(ThreadSelected(self.thread_id))

    def set_active(self, active: bool) -> None:
        if active:
            self.add_class("active")
        else:
            self.remove_class("active")


from textual.message import Message  # noqa: E402


class ThreadSelected(Message):
    def __init__(self, thread_id: int) -> None:
        super().__init__()
        self.thread_id = thread_id


class MessageBubble(Static):
    """Static message bubble (user or assistant)."""

    def __init__(self, role: str, content: str) -> None:
        css_class = "user-bubble" if role == "user" else "assistant-bubble"
        border_title = "Вы" if role == "user" else "Агент"
        super().__init__(classes=css_class)
        self.border_title = border_title
        self._role = role
        self._content = content

    def compose(self) -> ComposeResult:
        if self._role == "assistant":
            yield Markdown(self._content)
        else:
            yield Label(self._content)


class StreamingMessage(Static):
    """Message bubble that accumulates streamed text with throttled Markdown updates."""

    def __init__(self) -> None:
        super().__init__(classes="streaming-bubble")
        self.border_title = "Агент"
        self._content = ""
        self._md: Markdown | None = None
        self._elapsed_label: Label | None = None
        self._activity_log: Static | None = None
        self._log_lines: list[str] = []
        self._pending_status: str = ""
        self._last_log_time: float = 0.0
        self._tick_timer = None
        self._start_time: float = 0.0
        self._render_timer: asyncio.TimerHandle | None = None
        self._pending_render = False
        self._loading = True
        self._status_label: str = ""
        self._tool_start_time: float = 0.0

    def compose(self) -> ComposeResult:
        import time as _time

        self._start_time = _time.monotonic()
        log = Static("", classes="activity-log")
        self._activity_log = log
        yield log
        label = Label("⏳ (0s)")
        self._elapsed_label = label
        yield label
        md = Markdown("")
        md.display = False
        self._md = md
        yield md

    def on_mount(self) -> None:
        self._tick_timer = self.set_interval(1.0, self._tick_elapsed)

    def _tick_elapsed(self) -> None:
        if not self._loading:
            return
        import time as _time

        if self._tool_start_time > 0:
            elapsed = round(_time.monotonic() - self._tool_start_time, 1)
            text = f"🔧 {self._status_label}... ({elapsed}s)"
        elif self._status_label:
            elapsed = int(_time.monotonic() - self._start_time)
            text = f"{self._status_label} ({elapsed}s)"
        else:
            elapsed = int(_time.monotonic() - self._start_time)
            text = f"⏳ ({elapsed}s)"
        if self._elapsed_label is not None:
            self._elapsed_label.update(text)

    def _flush_pending(self) -> None:
        """Complete the pending status into the log with elapsed time."""
        import time as _time

        if self._pending_status:
            now = _time.monotonic()
            elapsed = round(now - self._last_log_time, 1) if self._last_log_time else 0
            self._log_lines.append(f"  {self._pending_status} ({elapsed}s)")
            self._last_log_time = now
            self._pending_status = ""

    def _append_log(self, line: str) -> None:
        """Add a completed line to the activity log."""
        self._flush_pending()
        self._log_lines.append(line)
        if self._activity_log is not None:
            self._activity_log.update("\n".join(self._log_lines))

    def set_pending_status(self, label: str) -> None:
        """Set a pending status — shown only in elapsed label, added to log when next event arrives."""
        import time as _time

        # Skip duplicate
        if label == self._pending_status:
            return
        self._flush_pending()
        self._pending_status = label
        self._last_log_time = _time.monotonic()
        # Update elapsed label (ticking timer shows this)
        self._status_label = label
        self._tool_start_time = 0.0
        if self._elapsed_label is not None:
            self._elapsed_label.update(f"⏳ {label}")
        if self._activity_log is not None:
            self._activity_log.update("\n".join(self._log_lines))

    def replace_pending_status(self, label: str) -> None:
        """Replace pending status text WITHOUT flushing old one to log.

        Used by countdown events to update the elapsed label in-place
        instead of creating a new log line for each tick.
        """
        self._pending_status = label
        self._status_label = label
        self._tool_start_time = 0.0
        if self._elapsed_label is not None:
            self._elapsed_label.update(f"⏳ {label}")

    def _stop_loading(self) -> None:
        self._loading = False
        if self._tick_timer is not None:
            self._tick_timer.stop()
            self._tick_timer = None
        if self._elapsed_label is not None:
            self._elapsed_label.display = False
        # Finalize pending status into log
        self._flush_pending()
        if self._activity_log is not None and self._log_lines:
            self._activity_log.update("\n".join(self._log_lines))
        if self._md is not None:
            self._md.display = True

    def append_text(self, text: str) -> None:
        if self._loading:
            self._stop_loading()
        self._content += text
        if not self._pending_render:
            self._pending_render = True
            self.set_timer(0.03, self._do_render)

    def _do_render(self) -> None:
        self._pending_render = False
        if self._md is not None:
            self._md.update(self._content)

    def set_error(self, error: str) -> None:
        self._pending_render = False
        self._stop_loading()
        self.add_class("user-bubble")
        self.remove_class("streaming-bubble")
        self.border_title = "Ошибка"
        if self._md is not None:
            self._md.update(f"**Ошибка:** {error}")

    def finalize(self) -> None:
        """Convert to static assistant bubble after streaming completes."""
        self._pending_render = False
        self._stop_loading()
        self.remove_class("streaming-bubble")
        self.add_class("assistant-bubble")
        self.border_title = "Агент"
        if self._md is not None and self._content:
            self._md.update(self._content)


class ChatInput(TextArea):
    """TextArea that sends on Enter; inserts newline on Shift+Enter / Alt+Enter."""

    async def _on_key(self, event: events.Key) -> None:
        if event.key == "enter":
            event.prevent_default()
            event.stop()
            await self.app.action_send_message()
        elif event.key in ("shift+enter", "meta+enter", "alt+enter", "escape+enter"):
            event.prevent_default()
            event.stop()
            self.insert("\n")


class ThreadSidebar(Container):
    """Left sidebar with thread list."""

    def compose(self) -> ComposeResult:
        yield Label("Треды", id="sidebar-title")
        yield Button("+ Новый тред", id="new-thread-btn", variant="primary")
        yield Container(id="thread-list")

    async def refresh_threads(self, threads: list[dict], active_id: int | None) -> None:
        thread_list = self.query_one("#thread-list")
        await thread_list.remove_children()
        for t in threads:
            item = ThreadItem(t["id"], t["title"], active=t["id"] == active_id)
            await thread_list.mount(item)

    def set_active(self, thread_id: int) -> None:
        for item in self.query(ThreadItem):
            item.set_active(item.thread_id == thread_id)


class PermissionDialog(ModalScreen):
    """Bottom-docked permission request menu in Claude Code style.

    Shows when the agent needs access to a restricted tool.
    Returns "once", "session", or "deny" via self.dismiss().
    """

    DEFAULT_CSS = """
    PermissionDialog {
        align: center bottom;
    }
    #permission-box {
        background: $surface;
        border: solid $primary;
        width: 70;
        height: auto;
        padding: 1 2;
        dock: bottom;
    }
    #permission-header {
        color: $text-muted;
        margin-bottom: 1;
    }
    #permission-tool {
        color: $accent;
        text-style: bold;
        margin-bottom: 1;
    }
    #permission-list {
        height: auto;
        margin-bottom: 1;
    }
    #permission-hint {
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("escape", "deny", "Отмена"),
        Binding("1", "choose_once", show=False),
        Binding("2", "choose_session", show=False),
        Binding("3", "choose_deny", show=False),
    ]

    def __init__(self, tool_name: str, phone: str) -> None:
        super().__init__()
        self._tool_name = tool_name
        self._phone = phone

    def compose(self) -> ComposeResult:
        phone_part = f" ({self._phone})" if self._phone else ""
        with Container(id="permission-box"):
            yield Label("Агент хочет использовать:", id="permission-header")
            yield Label(f"{self._tool_name}{phone_part}", id="permission-tool")
            with ListView(id="permission-list"):
                yield ListItem(Label("> 1. Разрешить один раз"), id="item-once")
                yield ListItem(Label("  2. Разрешить в этой сессии"), id="item-session")
                yield ListItem(Label("  3. Запретить"), id="item-deny")
            yield Label("Esc · ↑↓ навигация · Enter выбор", id="permission-hint")

    def on_mount(self) -> None:
        self.query_one(ListView).focus()

    @on(ListView.Selected)
    def on_list_selected(self, event: ListView.Selected) -> None:
        item_id = event.item.id
        if item_id == "item-once":
            self.dismiss("once")
        elif item_id == "item-session":
            self.dismiss("session")
        else:
            self.dismiss("deny")

    def action_deny(self) -> None:
        self.dismiss("deny")

    def action_choose_once(self) -> None:
        self.dismiss("once")

    def action_choose_session(self) -> None:
        self.dismiss("session")

    def action_choose_deny(self) -> None:
        self.dismiss("deny")


class AgentTuiApp(App):
    """Interactive TUI chat with agent."""

    CSS_PATH = str(CSS_PATH)
    ALLOW_SELECT = True  # built-in text selection; ctrl+c / super+c (Cmd+C) copies

    # ------------------------------------------------------------------
    # Native system clipboard (pbcopy/pbpaste on macOS, xclip/xsel on Linux)
    # Textual's default uses OSC 52 which doesn't work in Terminal.app
    # and has patchy Linux support (e.g. Gnome Terminal).
    # ------------------------------------------------------------------

    def copy_to_clipboard(self, text: str) -> None:
        """Write *text* to the system clipboard via native OS tools."""
        self._clipboard = text
        if sys.platform == "darwin":
            try:
                subprocess.run(["pbcopy"], input=text.encode(), check=True, timeout=2)
                return
            except Exception:
                pass
        else:
            for cmd in (
                ["xclip", "-selection", "clipboard"],
                ["xsel", "--clipboard", "--input"],
            ):
                try:
                    subprocess.run(cmd, input=text.encode(), check=True, timeout=2)
                    return
                except (FileNotFoundError, subprocess.SubprocessError):
                    continue
        super().copy_to_clipboard(text)

    @property
    def clipboard(self) -> str:
        """Read from the system clipboard so Ctrl+V pastes external content too."""
        if sys.platform == "darwin":
            try:
                r = subprocess.run(["pbpaste"], capture_output=True, text=True, check=True, timeout=2)
                return r.stdout
            except Exception:
                pass
        else:
            for cmd in (
                ["xclip", "-selection", "clipboard", "-o"],
                ["xsel", "--clipboard", "--output"],
            ):
                try:
                    r = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=2)
                    return r.stdout
                except (FileNotFoundError, subprocess.SubprocessError):
                    continue
        return self._clipboard

    BINDINGS = [
        Binding("ctrl+n", "new_thread", "Новый тред", show=True),
        Binding("ctrl+d", "delete_thread", "Удалить тред", show=True),
        Binding("ctrl+t", "toggle_sidebar", "Sidebar", show=True),
        Binding("escape", "cancel_stream", "Отмена", show=True),
        Binding("ctrl+q", "quit", "Выход", show=True),
        Binding("ctrl+s", "send_message", "Отправить", show=False),
    ]

    current_thread_id: reactive[int | None] = reactive(None)

    def __init__(self, db: "Database", config: "AppConfig", agent_manager: "AgentManager") -> None:
        super().__init__()
        self.db = db
        self.config = config
        self.agent_manager = agent_manager
        self._stream_worker = None
        self._session_id = str(uuid.uuid4())
        # Activate interactive permission dialogs for TUI mode
        self.agent_manager.enable_permission_gate()

    def on_unmount(self) -> None:
        self.agent_manager.disable_permission_gate()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with ThreadSidebar(id="sidebar"):
                pass
            with Vertical(id="chat-area"):
                with VerticalScroll(id="messages"):
                    pass
                with Horizontal(id="input-bar"):
                    yield ChatInput(id="input")
                    yield Button("Отправить", id="send-btn", variant="success")
        yield Footer()

    async def on_mount(self) -> None:
        threads = await self.db.get_agent_threads()
        if threads:
            await self._switch_thread(threads[0]["id"])
        else:
            tid = await self.db.create_agent_thread("Новый тред")
            await self._switch_thread(tid)
        await self._refresh_sidebar()

    async def _refresh_sidebar(self) -> None:
        threads = await self.db.get_agent_threads()
        sidebar = self.query_one(ThreadSidebar)
        await sidebar.refresh_threads(threads, self.current_thread_id)

    async def _switch_thread(self, thread_id: int) -> None:
        self.action_cancel_stream()
        self.current_thread_id = thread_id
        messages_container = self.query_one("#messages", VerticalScroll)
        await messages_container.remove_children()
        msgs = await self.db.get_agent_messages(thread_id)
        for msg in msgs:
            bubble = MessageBubble(msg["role"], msg["content"])
            await messages_container.mount(bubble)
        messages_container.scroll_end(animate=False)
        try:
            sidebar = self.query_one(ThreadSidebar)
            sidebar.set_active(thread_id)
        except NoMatches:
            pass

    @on(ThreadSelected)
    async def on_thread_selected(self, message: ThreadSelected) -> None:
        if message.thread_id != self.current_thread_id:
            await self._switch_thread(message.thread_id)

    @on(Button.Pressed, "#new-thread-btn")
    async def on_new_thread_btn(self) -> None:
        await self.action_new_thread()

    @on(Button.Pressed, "#send-btn")
    async def on_send_btn(self) -> None:
        if self._stream_worker is not None and self._stream_worker.state in (WorkerState.PENDING, WorkerState.RUNNING):
            self.action_cancel_stream()
        else:
            await self.action_send_message()

    async def action_new_thread(self) -> None:
        tid = await self.db.create_agent_thread("Новый тред")
        await self._switch_thread(tid)
        await self._refresh_sidebar()

    async def action_delete_thread(self) -> None:
        if self.current_thread_id is None:
            return
        await self.db.delete_agent_thread(self.current_thread_id)
        threads = await self.db.get_agent_threads()
        if threads:
            await self._switch_thread(threads[0]["id"])
        else:
            tid = await self.db.create_agent_thread("Новый тред")
            await self._switch_thread(tid)
        await self._refresh_sidebar()

    async def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", ThreadSidebar)
        sidebar.display = not sidebar.display

    def action_cancel_stream(self) -> None:
        if self._stream_worker is not None and not self._stream_worker.is_cancelled:
            self._stream_worker.cancel()

    def _set_button_streaming(self, streaming: bool) -> None:
        btn = self.query_one("#send-btn", Button)
        if streaming:
            btn.label = "■ Стоп (esc)"
            btn.variant = "error"
        else:
            btn.label = "Отправить"
            btn.variant = "success"

    async def action_send_message(self) -> None:
        if self._stream_worker is not None and self._stream_worker.state in (WorkerState.PENDING, WorkerState.RUNNING):
            return  # prevent double-send while streaming to avoid delete_last_agent_exchange deleting next user message
        input_area = self.query_one("#input", ChatInput)
        message = input_area.text.strip()
        if not message or self.current_thread_id is None:
            return
        input_area.clear()

        thread_id = self.current_thread_id
        await self.db.save_agent_message(thread_id, "user", message)

        messages_container = self.query_one("#messages", VerticalScroll)
        await messages_container.mount(MessageBubble("user", message))
        messages_container.scroll_end(animate=False)

        # Auto-rename thread on first user message
        thread = await self.db.get_agent_thread(thread_id)
        if thread and thread["title"] == "Новый тред":
            new_title = message[:60]
            await self.db.rename_agent_thread(thread_id, new_title)
            await self._refresh_sidebar()

        streaming_msg = StreamingMessage()
        await messages_container.mount(streaming_msg)
        messages_container.scroll_end(animate=False)

        self._stream_worker = self.run_worker(
            self._stream_response(thread_id, message, streaming_msg),
            exclusive=True,
            group="chat",
            thread=False,
        )
        self._set_button_streaming(True)

    async def _stream_response(self, thread_id: int, message: str, widget: StreamingMessage) -> None:
        model = None  # let AgentManager pick default

        messages_container = self.query_one("#messages", VerticalScroll)
        full_text = ""
        try:
            async for chunk in self.agent_manager.chat_stream(
                thread_id, message, model=model, session_id=self._session_id
            ):
                raw = chunk.removeprefix("data: ").strip()
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                event_type = payload.get("type")
                if event_type == "permission_request":
                    # Show interactive permission menu; tool handler is awaiting the Future
                    dialog = PermissionDialog(payload["tool"], payload.get("phone", ""))
                    choice = await self.push_screen_wait(dialog)
                    self.agent_manager.permission_gate.resolve(payload["request_id"], choice)
                    continue
                if event_type == "thinking":
                    widget.set_pending_status("Думает...")
                    continue
                if event_type == "tool_start":
                    import time as _time

                    tool_name = payload.get("tool", "tool")
                    widget._flush_pending()
                    widget._append_log(f"  🔧 {tool_name}...")
                    widget._status_label = tool_name
                    widget._tool_start_time = _time.monotonic()
                    continue
                if event_type == "tool_end":
                    tool = payload.get("tool", "tool")
                    dur = payload.get("duration", 0)
                    icon = "❌" if payload.get("is_error") else "✅"
                    summary = payload.get("summary", "")
                    log_line = f"    {icon} {tool} ({dur}s)"
                    if summary:
                        log_line += f" — {summary}"
                    widget._append_log(log_line)
                    continue
                if event_type == "tool_result":
                    if payload.get("is_error"):
                        tool = payload.get("tool", "tool")
                        summary = payload.get("summary", "")
                        widget._append_log(f"    ❌ {tool}: {summary}")
                    continue
                if event_type == "status":
                    widget.set_pending_status(payload.get("text", ""))
                    continue
                if event_type == "countdown":
                    widget.replace_pending_status(payload.get("text", ""))
                    continue
                if "text" in payload:
                    full_text += payload["text"]
                    widget.append_text(payload["text"])
                    messages_container.scroll_end(animate=False)
                if payload.get("done"):
                    break
                if "error" in payload:
                    widget.set_error(payload["error"])
                    await self.db.delete_last_agent_exchange(thread_id)
                    return
            if full_text:
                await self.db.save_agent_message(thread_id, "assistant", full_text)
            widget.finalize()
        except asyncio.CancelledError:
            await self.db.delete_last_agent_exchange(thread_id)
            if widget.is_attached:
                await widget.remove()
        except Exception as exc:
            logger.exception("Unexpected error in stream worker")
            widget.set_error(str(exc))
            await self.db.delete_last_agent_exchange(thread_id)
        finally:
            self._set_button_streaming(False)
