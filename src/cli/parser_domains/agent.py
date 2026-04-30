from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    agent_parser = subparsers.add_parser("agent", help="Agent chat management")
    agent_sub = agent_parser.add_subparsers(dest="agent_action")

    agent_sub.add_parser("threads", help="List agent threads")

    agent_create = agent_sub.add_parser("thread-create", help="Create new thread")
    agent_create.add_argument("--title", default=None, help="Thread title")

    agent_delete = agent_sub.add_parser("thread-delete", help="Delete thread")
    agent_delete.add_argument("thread_id", type=int, help="Thread ID")

    agent_chat = agent_sub.add_parser("chat", help="Interactive TUI chat or one-shot message (with -p)")
    agent_chat.add_argument("-p", "--prompt", default=None, dest="prompt", help="Message text (non-interactive mode)")
    agent_chat.add_argument("--thread-id", type=int, default=None, dest="thread_id")
    agent_chat.add_argument("--model", default=None, help="Model name")

    agent_rename = agent_sub.add_parser("thread-rename", help="Rename thread")
    agent_rename.add_argument("thread_id", type=int, help="Thread ID")
    agent_rename.add_argument("title", help="New title")

    agent_msgs = agent_sub.add_parser("messages", help="Show thread messages")
    agent_msgs.add_argument("thread_id", type=int, help="Thread ID")
    agent_msgs.add_argument("--limit", type=int, default=None, help="Last N messages")

    agent_ctx = agent_sub.add_parser("context", help="Inject channel context into thread")
    agent_ctx.add_argument("thread_id", type=int, help="Thread ID")
    agent_ctx.add_argument("--channel-id", type=int, required=True, dest="channel_id")
    agent_ctx.add_argument("--limit", type=int, default=100000, help="Max messages")
    agent_ctx.add_argument("--topic-id", type=int, default=None, dest="topic_id")

    agent_sub.add_parser("test-escaping", help="Test agent with special characters")
    agent_sub.add_parser("test-tools", help="Test that agent tool calls produce tool_start/tool_end events")
