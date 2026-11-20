"""DM reply-draft command handler (#1429).

Domain: ``dm.send_reply`` — worker-only delivery of an owner-approved reply
draft. The claim/act/commit order mirrors the moderation publish path: the
atomic DB claim (``status='sending'``) is the one gate against a double send,
the irreversible Telegram write happens only after it wins, and a failed send
releases the claim without overwriting a later state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from telethon_floodgate import TelegramRateLimitedError

from tg_agent.services.dispatcher._errors import TelegramCommandRetryLaterError
from tg_agent.services.telegram_actions import TelegramActionClientUnavailableError, TelegramActionService

if TYPE_CHECKING:
    from tg_agent.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object

# Повтор команды, чей клейм держит чужой потребитель: живой конкурент успеет
# дойти до mark_sent, и ретрай увидит already_sent.
_CLAIM_CONFLICT_RETRY_SEC = 60


class DmCommandsMixin(_Base):
    """``dm.*`` command handlers."""

    async def _handle_dm_send_reply(self, payload: dict[str, Any]) -> dict[str, Any]:
        drafts = self._db.repos.dm_reply_drafts
        draft_id = int(payload["draft_id"])
        draft = await drafts.get(draft_id)
        if draft is None:
            raise RuntimeError("draft_not_found")
        # Идемпотентный повтор команды (ретрай очереди после crash между
        # mark_sent и признанием команды): черновик уже доставлен.
        if draft.status == "sent":
            return {"draft_id": draft_id, "already_sent": True}
        if draft.status == "rejected":
            raise RuntimeError("draft_rejected")
        # Атомарный клейм: второй конкурентный потребитель получает False —
        # двойная отправка невозможна (#1429). Не «успех»: команда уходит в
        # отложенный ретрай, иначе чужой live-клейм завершал бы команду
        # благополучно, а черновик ждал бы живого отправителя.
        if not await drafts.claim_for_send(draft_id):
            raise TelegramCommandRetryLaterError(
                run_after=datetime.now(timezone.utc)
                + timedelta(seconds=_CLAIM_CONFLICT_RETRY_SEC),
                reason="draft already claimed by another consumer",
                result_payload={"draft_id": draft_id, "state": "claim_conflict"},
            )
        try:
            result = await TelegramActionService(self._pool).send_message(
                phone=draft.phone,
                recipient=draft.chat_id,
                text=draft.reply_text,
                is_user=True,  # DM-диалог: не пробовать канал первым (#1830)
            )
        except BaseException as exc:
            await drafts.release_claim(draft_id)
            if isinstance(exc, TelegramRateLimitedError):
                retry_at = datetime.now(timezone.utc) + timedelta(seconds=max(0, exc.retry_after_sec))
            elif isinstance(exc, TelegramActionClientUnavailableError):
                retry_at = await self._account_flood_until(draft.phone)
            else:
                raise
            if retry_at is None:
                raise
            raise TelegramCommandRetryLaterError(
                run_after=retry_at + timedelta(seconds=1),
                reason=f"send from {draft.phone} delayed until {retry_at.isoformat()}",
                result_payload={
                    "state": "waiting_flood_wait",
                    "phone": draft.phone,
                    "next_available_at_utc": retry_at.isoformat(),
                },
            ) from exc
        marked = await drafts.mark_sent(draft_id, result.message_id)
        return {
            "draft_id": draft_id,
            "phone": result.phone,
            "message_id": result.message_id,
            "sent": marked,
        }
