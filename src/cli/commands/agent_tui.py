from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Button, Footer, Header, Label, Markdown, Static, TextArea
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
        self.border_title = "Агент (печатает…)"
        self._content = ""
        self._md: Markdown | None = None
        self._render_timer: asyncio.TimerHandle | None = None
        self._pending_render = False

    def compose(self) -> ComposeResult:
        md = Markdown("")
        self._md = md
        yield md

    def append_text(self, text: str) -> None:
        self._content += text
        if not self._pending_render:
            self._pending_render = True
            self.set_timer(0.1, self._do_render)

    def _do_render(self) -> None:
        self._pending_render = False
        if self._md is not None:
            self._md.update(self._content)

    def set_error(self, error: str) -> None:
        self._pending_render = False
        self.add_class("user-bubble")
        self.remove_class("streaming-bubble")
        self.border_title = "Ошибка"
        if self._md is not None:
            self._md.update(f"**Ошибка:** {error}")

    def finalize(self) -> None:
        """Convert to static assistant bubble after streaming completes."""
        self._pending_render = False
        self.remove_class("streaming-bubble")
        self.add_class("assistant-bubble")
        self.border_title = "Агент"
        if self._md is not None and self._content:
            self._md.update(self._content)


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


class AgentTuiApp(App):
    """Interactive TUI chat with agent."""

    CSS_PATH = str(CSS_PATH)

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

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with ThreadSidebar(id="sidebar"):
                pass
            with Vertical(id="chat-area"):
                with VerticalScroll(id="messages"):
                    pass
                with Horizontal(id="input-bar"):
                    yield TextArea(id="input")
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

    async def action_send_message(self) -> None:
        if self._stream_worker is not None and self._stream_worker.state in (WorkerState.PENDING, WorkerState.RUNNING):
            return  # prevent double-send while streaming to avoid delete_last_agent_exchange deleting next user message
        input_area = self.query_one("#input", TextArea)
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

    async def _stream_response(self, thread_id: int, message: str, widget: StreamingMessage) -> None:
        model = None  # let AgentManager pick default

        messages_container = self.query_one("#messages", VerticalScroll)
        full_text = ""
        try:
            async for chunk in self.agent_manager.chat_stream(thread_id, message, model=model):
                raw = chunk.removeprefix("data: ").strip()
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
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
