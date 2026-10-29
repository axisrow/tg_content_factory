"""Offline Telegram implementation for the real multi-process worker scenario."""

import asyncio
import json
import sys
from pathlib import Path

from tg_agent.gateway.runtime import serve


class TelegramStub:
    def __init__(self, store):
        self.store = store
        self.connected = set()

    def account_status(self, account):
        return {"connected": True}

    async def execute(self, project, operation):
        self.connected.add(str(operation.account_id))
        (self.store.root / "test-connections.json").write_text(json.dumps(sorted(self.connected)))
        if operation.kind == "dialogs":
            return {"dialogs": []}
        if operation.kind == "send_message":
            (self.store.root / "test-sent.json").write_text(json.dumps({"project": project,
                "text": operation.arguments.text}))
            return {"phone": str(operation.account_id), "message_id": 88}
        if operation.kind == "message_page":
            return {"messages": [{"channel_id": 123, "message_id": mid, "text": f"post {mid}",
                                  "date": "2026-10-29T00:00:00Z"} for mid in (7, 5)],
                    "reaction_users": {}, "forward_ids": [], "urls": {}}
        if operation.kind != "history":
            raise ValueError("Unexpected operation in collection integration test")
        rows = [{"channel_id": 123, "message_id": mid, "text": f"post {mid}",
                 "date": "2026-10-29T00:00:00Z"} for mid in (5, 7) if mid > operation.after_id]
        return {"messages": rows, "next_id": max((row["message_id"] for row in rows),
                                                  default=operation.after_id)}

    async def close(self):
        pass


if __name__ == "__main__":
    asyncio.run(serve(Path(sys.argv[1]), access_factory=TelegramStub))
