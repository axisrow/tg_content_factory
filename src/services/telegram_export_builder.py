"""Telegram-Desktop-compatible chat/channel export builder (issue #834).

Pure formatting layer: turns collected ``Channel`` + ``Message`` rows into a
Telegram Desktop-style ``result.json`` and ``messages*.html`` tree, plus a
sidecar ``export_manifest.json`` recording skipped/not-included media.

The builder knows nothing about Telegram or downloading. Media is supplied by a
``MediaResolver`` callback: the offline path (CLI/Web) returns "not included"
artifacts, while the worker path (PR-3) returns real downloaded files / size
skips. This keeps the module ``telethon``-free (import-linter clean) and unit
testable without a live client.
"""

from __future__ import annotations

import asyncio
import html
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from src.models import Channel, Message
from src.telegram.reactions import parse_reactions_json

# Exact Telegram Desktop placeholder strings for media that is not on disk. The
# issue (#834) requires these so JSON/HTML stay readable and never break on
# missing links.
MEDIA_NOT_INCLUDED = "(File not included. Change data exporting settings to download.)"
MEDIA_TOO_BIG = "(File exceeds maximum size. Change data exporting settings to download.)"

# Skip reasons recorded in the manifest / carried on MediaArtifact.
REASON_NOT_INCLUDED = "not_included"
REASON_EXCEEDS_MAX_SIZE = "exceeds_max_size"

DEFAULT_HTML_PAGE_SIZE = 1000
DEFAULT_MAX_FILE_SIZE_MB = 3

RESULT_JSON_NAME = "result.json"
MANIFEST_JSON_NAME = "export_manifest.json"

# channel_type (our taxonomy) -> Telegram Desktop ``type`` field. The
# public/private split is decided by whether the channel exposes a @username.
_PUBLIC_TYPE = {
    "channel": "public_channel",
    "supergroup": "public_supergroup",
    "gigagroup": "public_supergroup",
    "group": "public_supergroup",
}
_PRIVATE_TYPE = {
    "channel": "private_channel",
    "supergroup": "private_supergroup",
    "gigagroup": "private_supergroup",
    "group": "private_group",
}


@dataclass(frozen=True)
class MediaArtifact:
    """How a single message's media is represented in the export.

    - ``skipped=False`` with ``rel_path`` set → media is on disk; JSON/HTML link
      to ``rel_path`` (relative to the export root).
    - ``skipped=True`` with ``reason`` set → media is not on disk; JSON/HTML show
      the matching Telegram placeholder string and the manifest records it.
    """

    kind: str  # "photo" | "video" | "voice" | "file" | ...
    rel_path: str | None = None
    size_bytes: int | None = None
    skipped: bool = False
    reason: str | None = None


# Resolver maps a message to its media artifact, or None when it has no media.
MediaResolver = Callable[[Message], "MediaArtifact | None"]


@dataclass
class ExportSummary:
    out_dir: str
    files: list[str] = field(default_factory=list)
    message_count: int = 0
    media_included: int = 0
    media_skipped: int = 0
    skipped: list[dict] = field(default_factory=list)
    truncated: bool = False


def offline_media_resolver(message: Message) -> MediaArtifact | None:
    """Resolver for the no-download path: any media is marked "not included"."""
    if not message.media_type:
        return None
    return MediaArtifact(kind=_media_kind(message.media_type), skipped=True, reason=REASON_NOT_INCLUDED)


def html_page_name(index: int) -> str:
    """messages.html, messages2.html, messages3.html … (Telegram convention)."""
    return "messages.html" if index == 0 else f"messages{index + 1}.html"


def telegram_chat_type(channel: Channel) -> str:
    ctype = (channel.channel_type or "").lower()
    table = _PUBLIC_TYPE if channel.username else _PRIVATE_TYPE
    return table.get(ctype, "private_channel" if not channel.username else "public_channel")


def _media_kind(media_type: str | None) -> str:
    mt = (media_type or "").lower()
    if "photo" in mt:
        return "photo"
    if "voice" in mt or "audio" in mt:
        return "voice"
    if "video" in mt:
        return "video"
    return "file"


def _fmt_date(dt: datetime | None) -> tuple[str, str]:
    """Return (ISO ``YYYY-MM-DDTHH:MM:SS``, unix-seconds string)."""
    if dt is None:
        return "", ""
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    iso = aware.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
    return iso, str(int(aware.timestamp()))


def _sender_identity(channel: Channel, message: Message) -> tuple[str, str | None]:
    """Return Telegram-style (from, from_id) for a message."""
    name = message.sender_name
    if not name:
        parts = [message.sender_first_name, message.sender_last_name]
        name = " ".join(p for p in parts if p) or message.sender_username
    if message.sender_id:
        prefix = "channel" if message.sender_kind == "channel" else "user"
        return (name or channel.title or ""), f"{prefix}{message.sender_id}"
    # Channel broadcast posts have no sender → attribute to the channel itself.
    return (name or channel.title or ""), f"channel{channel.channel_id}"


def _reactions(message: Message) -> list[dict]:
    return [
        {"type": "emoji", "count": item["count"], "emoji": item["emoji"]}
        for item in parse_reactions_json(message.reactions_json)
    ]


class TelegramExportBuilder:
    """Builds Telegram-Desktop-compatible export artifacts from DB rows."""

    def build_result_json(
        self,
        channel: Channel,
        messages: Sequence[Message],
        artifacts: dict[int, MediaArtifact | None],
    ) -> dict:
        return {
            "name": channel.title or (channel.username or str(channel.channel_id)),
            "type": telegram_chat_type(channel),
            "id": channel.channel_id,
            "messages": [self._message_json(channel, m, artifacts.get(m.message_id)) for m in messages],
        }

    def _message_json(self, channel: Channel, message: Message, artifact: MediaArtifact | None) -> dict:
        iso, unix = _fmt_date(message.date)
        sender, sender_id = _sender_identity(channel, message)
        text = message.text or ""
        obj: dict = {
            "id": message.message_id,
            "date": iso,
            "date_unixtime": unix,
        }
        if message.service_action_semantic or message.service_action_raw:
            obj["type"] = "service"
            obj["actor"] = sender
            obj["actor_id"] = sender_id
            obj["action"] = message.service_action_semantic or message.service_action_raw
            obj["text"] = ""
            obj["text_entities"] = []
            return obj
        obj["type"] = "message"
        obj["from"] = sender
        obj["from_id"] = sender_id
        if message.forward_from_channel_id:
            obj["forwarded_from"] = (
                channel.title
                if message.forward_from_channel_id == channel.channel_id
                else f"channel{message.forward_from_channel_id}"
            )
        obj["text"] = text
        obj["text_entities"] = [{"type": "plain", "text": text}] if text else []
        reactions = _reactions(message)
        if reactions:
            obj["reactions"] = reactions
        self._apply_media_json(obj, artifact)
        return obj

    @staticmethod
    def _apply_media_json(obj: dict, artifact: MediaArtifact | None) -> None:
        if artifact is None:
            return
        field_name = "photo" if artifact.kind == "photo" else "file"
        if artifact.skipped:
            obj[field_name] = MEDIA_TOO_BIG if artifact.reason == REASON_EXCEEDS_MAX_SIZE else MEDIA_NOT_INCLUDED
        else:
            obj[field_name] = artifact.rel_path
        if artifact.kind != "photo":
            obj["media_type"] = artifact.kind
        if artifact.size_bytes is not None:
            obj["file_size"] = artifact.size_bytes

    def iter_html_pages(
        self,
        channel: Channel,
        messages: Sequence[Message],
        artifacts: dict[int, MediaArtifact | None],
        page_size: int = DEFAULT_HTML_PAGE_SIZE,
    ):
        """Yield (filename, html) one page at a time.

        Generating lazily keeps only one rendered page in memory at a time — a
        100k-message export would otherwise buffer hundreds of MB of HTML strings
        before the first write (Claude review on #937).
        """
        page_size = max(1, int(page_size))
        total_pages = max(1, (len(messages) + page_size - 1) // page_size)
        for index in range(total_pages):
            chunk = messages[index * page_size : (index + 1) * page_size]
            blocks = "\n".join(self._message_html(channel, m, artifacts.get(m.message_id)) for m in chunk)
            yield html_page_name(index), self._html_document(channel, blocks, index, total_pages)

    def build_html_pages(
        self,
        channel: Channel,
        messages: Sequence[Message],
        artifacts: dict[int, MediaArtifact | None],
        page_size: int = DEFAULT_HTML_PAGE_SIZE,
    ) -> list[tuple[str, str]]:
        return list(self.iter_html_pages(channel, messages, artifacts, page_size))

    def _html_document(self, channel: Channel, blocks: str, index: int, total_pages: int) -> str:
        name = html.escape(channel.title or (channel.username or str(channel.channel_id)))
        nav = self._html_nav(index, total_pages)
        return (
            "<!DOCTYPE html>\n"
            '<html lang="en"><head><meta charset="utf-8">\n'
            f"<title>{name}</title>\n"
            "<style>body{font-family:sans-serif;max-width:780px;margin:0 auto;padding:1rem;}"
            ".message{border-bottom:1px solid #eee;padding:.5rem 0;}"
            ".from{font-weight:bold;}.date{color:#888;font-size:.8rem;}"
            ".text{white-space:pre-wrap;}.media{color:#37a;}.reactions{color:#666;font-size:.9rem;}"
            ".service{color:#888;font-style:italic;}.pagination{margin:1rem 0;}</style>\n"
            "</head><body>\n"
            f'<div class="page-header"><h1>{name}</h1>'
            f'<div class="info">{html.escape(telegram_chat_type(channel))} · id {channel.channel_id}</div></div>\n'
            f'{nav}<div class="history">\n{blocks}\n</div>{nav}'
            "\n</body></html>\n"
        )

    @staticmethod
    def _html_nav(index: int, total_pages: int) -> str:
        if total_pages <= 1:
            return ""
        links = []
        if index > 0:
            links.append(f'<a href="{html_page_name(index - 1)}">← Previous</a>')
        if index < total_pages - 1:
            links.append(f'<a href="{html_page_name(index + 1)}">Next →</a>')
        return f'<div class="pagination">{" · ".join(links)}</div>\n'

    def _message_html(self, channel: Channel, message: Message, artifact: MediaArtifact | None) -> str:
        iso, _ = _fmt_date(message.date)
        sender, _ = _sender_identity(channel, message)
        if message.service_action_semantic or message.service_action_raw:
            action = html.escape(message.service_action_semantic or message.service_action_raw or "")
            return (
                f'<div class="message service" id="message{message.message_id}">'
                f'<span class="date">{html.escape(iso)}</span> '
                f"{html.escape(sender)} — {action}</div>"
            )
        rows = [
            f'<div class="from">{html.escape(sender)}</div>',
            f'<div class="date">{html.escape(iso)}</div>',
        ]
        if message.text:
            rows.append(f'<div class="text">{html.escape(message.text)}</div>')
        media_html = self._media_html(artifact)
        if media_html:
            rows.append(media_html)
        reactions = parse_reactions_json(message.reactions_json)
        if reactions:
            rendered = "  ".join(f'{html.escape(r["emoji"])} {r["count"]}' for r in reactions)
            rows.append(f'<div class="reactions">{rendered}</div>')
        return f'<div class="message" id="message{message.message_id}">' + "".join(rows) + "</div>"

    @staticmethod
    def _media_html(artifact: MediaArtifact | None) -> str:
        if artifact is None:
            return ""
        if artifact.skipped:
            placeholder = MEDIA_TOO_BIG if artifact.reason == REASON_EXCEEDS_MAX_SIZE else MEDIA_NOT_INCLUDED
            return f'<div class="media">{html.escape(placeholder)}</div>'
        rel = artifact.rel_path or ""
        return f'<div class="media"><a href="{html.escape(rel)}">{html.escape(artifact.kind)}</a></div>'

    async def write_export(
        self,
        out_dir: str | Path,
        channel: Channel,
        messages: Sequence[Message],
        *,
        fmt: str = "json",
        media_resolver: MediaResolver = offline_media_resolver,
        page_size: int = DEFAULT_HTML_PAGE_SIZE,
        truncated: bool = False,
    ) -> ExportSummary:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        artifacts: dict[int, MediaArtifact | None] = {m.message_id: media_resolver(m) for m in messages}
        summary = ExportSummary(out_dir=str(out), message_count=len(messages), truncated=truncated)
        for message in messages:
            artifact = artifacts.get(message.message_id)
            if artifact is None:
                continue
            if artifact.skipped:
                summary.media_skipped += 1
                summary.skipped.append(
                    {
                        "message_id": message.message_id,
                        "media_type": message.media_type,
                        "reason": artifact.reason,
                        "original_size_bytes": artifact.size_bytes,
                    }
                )
            else:
                summary.media_included += 1

        if fmt in ("json", "both"):
            result = self.build_result_json(channel, messages, artifacts)
            await self._write_text(out / RESULT_JSON_NAME, json.dumps(result, ensure_ascii=False, indent=2))
            summary.files.append(RESULT_JSON_NAME)
        if fmt in ("html", "both"):
            # Stream pages so only one rendered page is resident at a time.
            for filename, content in self.iter_html_pages(channel, messages, artifacts, page_size):
                await self._write_text(out / filename, content)
                summary.files.append(filename)

        manifest = {
            "channel_id": channel.channel_id,
            "name": channel.title or (channel.username or str(channel.channel_id)),
            "format": fmt,
            "message_count": summary.message_count,
            "truncated": truncated,
            "media_included": summary.media_included,
            "media_skipped": summary.media_skipped,
            "skipped_files": summary.skipped,
        }
        await self._write_text(out / MANIFEST_JSON_NAME, json.dumps(manifest, ensure_ascii=False, indent=2))
        summary.files.append(MANIFEST_JSON_NAME)
        return summary

    @staticmethod
    async def _write_text(path: Path, content: str) -> None:
        # Offload the blocking write so the async web request / worker loop isn't
        # stalled per file (Claude review on #937).
        await asyncio.to_thread(path.write_text, content, encoding="utf-8")
