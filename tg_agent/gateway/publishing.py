"""Publish pipeline runs through durable gateway intents."""

import asyncio
import tempfile
from pathlib import Path
from uuid import UUID

import httpx

from tg_agent.gateway.files import publish_files
from tg_agent.gateway.pool import GatewayUncertainError
from tg_agent.gateway.protocol import Send


async def publish_target(pool, run, target):
    from tg_agent.services.publish_service import PublishResult

    intent = f"generation:{run.id}:target:{target.phone}:{target.dialog_id}"
    reply_to = (run.metadata or {}).get("reply_to_message_id") if (run.metadata or {}).get("publish_reply") else None
    try:
        if run.image_url:
            from tg_agent.services.s3_store import refresh_s3_url

            image_url = await refresh_s3_url(run.image_url)
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
                response = await http.get(image_url)
                response.raise_for_status()
            with tempfile.TemporaryDirectory() as directory:
                # A stable filename keeps the upload identity independent of expiring URL signatures.
                path = Path(directory) / "image.jpg"
                await asyncio.to_thread(path.write_bytes, response.content)
                result = await publish_files(pool, phone=target.phone, peer=target.dialog_id, files=[path],
                                             intent=intent, caption=run.generated_text, reply_to=reply_to)
                message_id = result["message_ids"][0]
        else:
            account = pool.select(target.phone)
            if account["phone"] != target.phone:
                raise PermissionError("Publishing account is not granted to this project")
            result = await pool.request(Send(kind="send", account_id=UUID(account["id"]), peer=target.dialog_id,
                                             text=run.generated_text, reply_to=reply_to), intent)
            message_id = result["message_id"]
        return PublishResult(success=True, message_id=message_id, phone=target.phone, dialog_id=target.dialog_id)
    except GatewayUncertainError:
        return PublishResult(success=False, uncertain=True, error="Gateway delivery unconfirmed; verify manually",
                             phone=target.phone, dialog_id=target.dialog_id)
    except asyncio.CancelledError:
        # Keep the existing publish claim protected when shutdown interrupts the receipt wait.
        await pool._db.repos.generation_runs.set_metadata(run.id,
            {"unconfirmed_targets": [f"{target.phone}:{target.dialog_id}"]})
        raise
    except Exception as exc:
        return PublishResult(success=False, error=f"Gateway publication deferred or failed: {type(exc).__name__}",
                             phone=target.phone, dialog_id=target.dialog_id)
