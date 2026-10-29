"""Durable artifact identities tied to a project's send intent."""

import asyncio
import hashlib
import mimetypes
from pathlib import Path
from urllib.parse import quote
from uuid import UUID, uuid4

from tg_agent.gateway.protocol import Publish


async def upload_file(pool, path, intent):
    path = Path(path)
    content = await asyncio.to_thread(path.read_bytes)
    if len(content) > 128 * 1024 * 1024:
        raise ValueError("File exceeds gateway's 128 MiB limit")
    fingerprint = hashlib.sha256(content).hexdigest()
    async with pool._db.transaction() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS gateway_uploads("
                           "intent TEXT PRIMARY KEY,artifact_id TEXT NOT NULL,fingerprint TEXT NOT NULL,filename TEXT)")
        await conn.execute("INSERT OR IGNORE INTO gateway_uploads VALUES (?,?,?,?)",
                           (intent, str(uuid4()), fingerprint, path.name))
        async with conn.execute("SELECT artifact_id,fingerprint,filename FROM gateway_uploads WHERE intent=?",
                                (intent,)) as cursor:
            row = await cursor.fetchone()
        if row[1] != fingerprint or row[2] != path.name:
            raise ValueError("File content changed for an existing Telegram send intent")
        artifact = row[0]
    response = await pool.client.http.put(f"/v1/artifacts/{artifact}", content=content,
        headers={"X-Filename": quote(path.name), "Content-Type": mimetypes.guess_type(path.name)[0]
                 or "application/octet-stream"})
    response.raise_for_status()
    return UUID(artifact)


async def publish_files(pool, *, phone, peer, files, intent, caption=None, schedule=None, reply_to=None,
                        force_document=False, peer_type="auto"):
    account = pool.select(phone)
    if account["phone"] != phone:
        raise PermissionError("Publishing account is not granted to this project")
    artifacts = [await upload_file(pool, path, f"{intent}:file:{index}") for index, path in enumerate(files)]
    operation = Publish(kind="publish", account_id=UUID(account["id"]), peer=peer, peer_type=peer_type,
                        artifact_ids=artifacts, caption=caption, schedule=schedule, reply_to=reply_to,
                        force_document=force_document)
    return await pool.request(operation, intent)
