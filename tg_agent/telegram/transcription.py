"""Voice/video-note transcription: engines, SQLite cache, and batch budget.

Four engines behind one interface (see ``transcribe``):

* ``telegram`` - native Premium ``messages.transcribeAudio``. Free, but drops
  the last speech segment in roughly 2 of 3 real recordings (proven with
  per-segment timestamps against a local Whisper run on 2026-08-20 - see
  ``~/Workspace/AI Playground/docs/2026-08-20-telegram-voice-transcripts.md``).
  Async: a long recording comes back ``pending=True`` and must be polled.
* ``groq`` - Groq-hosted ``whisper-large-v3-turbo``. Does not drop the tail,
  but costs a download+upload per voice message and sends the audio to a
  third party. Primary engine by owner decision (2026-08-20); ``telegram``
  is the free/private fallback.
* ``openai`` - any OpenAI-compatible ``/audio/transcriptions`` endpoint
  (OpenAI itself, a self-hosted achetronic/parakeet or speaches server,
  LocalAI, a vLLM whisper, ...) at a configured base URL, with an optional
  API key.
* ``whisper`` - a local faster-whisper model loaded in this process. Never
  leaves the machine; needs the ``faster-whisper`` package installed.

``groq`` and ``openai`` share one HTTP path
(:func:`_transcribe_via_openai_compatible`); only their settings differ.

Every transcript is a machine reading, not a verbatim quote (proper names and
punctuation drift under both engines) - callers must surface ``source``
alongside ``text`` rather than presenting it as exact speech.
"""

# SPDX-License-Identifier: Apache-2.0
#
# Vendored from axisrow/telegram-mcp ``telegram_mcp/transcription.py``
# at commit e224762671897c1df53deadb51be84eaaaf2cea3 (ported 2026-10-16,
# tg_content_factory issue #1545). Upstream copyright: pathknower, Arnout
# van Westen, Omid Zaferi, Alexander Kulikov, 4erdenko (Apache-2.0).
# Project-local changes live in later commits and are summarized in
# THIRD_PARTY_NOTICES.md; the full license text is LICENSES/apache-2.0.txt.

import asyncio
import importlib.util
import io
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

from tg_agent.database import Database
from tg_agent.telegram.backends import transcribe_audio_raw
from tg_agent.utils.errors import error_text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENGINES = {"telegram", "groq", "openai", "whisper"}

GROQ_TRANSCRIBE_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_MODEL = "whisper-large-v3-turbo"

OPENAI_DEFAULT_MODEL = "whisper-1"

WHISPER_DEFAULT_MODEL = "small"

# Self-hosted servers on a CPU can take well over a minute for a long voice
# note, so the HTTP engines get more room than a hosted API would need.
HTTP_DEFAULT_TIMEOUT_SECONDS = 120.0

# Groq rejects an upload above its per-file limit, and the limit depends on
# the account tier (25 MB on the free tier at the time of writing, more on
# paid ones). Checked locally so an oversized recording fails with an
# actionable message instead of a 413 from the API after a full download.
GROQ_DEFAULT_MAX_MB = 25.0


def default_engine() -> str:
    """``TELEGRAM_TRANSCRIBE_ENGINE``: telegram | groq | openai | whisper
    (default groq).

    Groq is the primary engine (owner decision 2026-08-20: native Telegram
    transcription drops the last speech segment in ~2 of 3 recordings).
    Override to ``telegram`` for chats that should never leave the server,
    or when GROQ_API_KEY / quota is unavailable, or to one of the
    self-hosted engines. Per-call ``engine=`` on transcribe_voice always
    takes precedence over this default.
    """
    raw = os.getenv("TELEGRAM_TRANSCRIBE_ENGINE", "groq").strip().lower()
    if raw not in ENGINES:
        accepted = ", ".join(sorted(ENGINES))
        raise SystemExit(
            f"Invalid TELEGRAM_TRANSCRIBE_ENGINE '{raw}'. Expected one of: {accepted}."
        )
    return raw


def transcribe_language() -> Optional[str]:
    """``TELEGRAM_TRANSCRIBE_LANGUAGE``: ISO-639-1 hint (``nl``, ``en``, ...).

    Unset lets whisper-style engines detect the language themselves. Some
    OpenAI-compatible servers do not detect it: achetronic/parakeet assumes
    ``en`` when no language is sent, so non-English chats need this set.
    """
    raw = os.getenv("TELEGRAM_TRANSCRIBE_LANGUAGE", "").strip().lower()
    return raw or None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError:
        value = default
    return value if value > 0 else default


def _endpoint(base: str) -> str:
    """Accept either a base URL (``https://api.openai.com/v1``, like the
    OpenAI SDKs' base_url) or the full transcriptions URL."""
    url = base.strip().rstrip("/")
    if url.endswith("/audio/transcriptions"):
        return url
    return url + "/audio/transcriptions"


@dataclass(frozen=True)
class HttpEngineConfig:
    """Settings for one OpenAI-compatible transcription endpoint."""

    engine: str
    label: str
    url: Optional[str]
    api_key: Optional[str]
    model: str
    response_format: str
    max_bytes: Optional[int]
    max_mb_env: str
    timeout: float


def _http_timeout() -> float:
    return _env_float("TELEGRAM_TRANSCRIBE_TIMEOUT", HTTP_DEFAULT_TIMEOUT_SECONDS)


def http_engine_config(engine: str) -> Optional[HttpEngineConfig]:
    """Config for an HTTP engine, or None for telegram/whisper."""
    if engine == "groq":
        return HttpEngineConfig(
            engine="groq",
            label="Groq",
            url=GROQ_TRANSCRIBE_URL,
            api_key=os.getenv("GROQ_API_KEY") or None,
            model=GROQ_MODEL,
            response_format="verbose_json",
            max_bytes=groq_max_upload_bytes(),
            max_mb_env="TELEGRAM_TRANSCRIBE_GROQ_MAX_MB",
            timeout=_http_timeout(),
        )
    if engine == "openai":
        base = os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "").strip()
        return HttpEngineConfig(
            engine="openai",
            label="OpenAI-compatible",
            url=_endpoint(base) if base else None,
            api_key=os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_API_KEY") or None,
            model=os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_MODEL", "").strip()
            or OPENAI_DEFAULT_MODEL,
            # "json" is the one format every compatible server and every
            # OpenAI model (gpt-4o-transcribe included) accepts; verbose_json
            # is whisper-only.
            response_format="json",
            max_bytes=int(_env_float("TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB", 25.0) * 1048576),
            max_mb_env="TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB",
            timeout=_http_timeout(),
        )
    return None


def whisper_available() -> bool:
    return importlib.util.find_spec("faster_whisper") is not None


def engine_config_error(engine: str) -> Optional[str]:
    """Why ``engine`` cannot run with the current settings, or None.

    Checked before any download, so a misconfigured engine costs nothing.
    """
    if engine == "groq" and not os.getenv("GROQ_API_KEY"):
        return "GROQ_API_KEY is not configured on this server. Use another engine or set GROQ_API_KEY."
    if engine == "openai" and not os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "").strip():
        return (
            "TELEGRAM_TRANSCRIBE_OPENAI_URL is not configured on this server. Set it to the "
            "base URL of an OpenAI-compatible API (e.g. https://api.openai.com/v1)."
        )
    if engine == "whisper" and not whisper_available():
        return (
            "faster-whisper is not installed on this server. Install it with "
            "pip install faster-whisper."
        )
    return None


# ---------------------------------------------------------------------------
# SQLite cache (the project database, ``transcripts`` table)
# ---------------------------------------------------------------------------
#
# The upstream module kept a standalone plaintext SQLite file with mode-600
# directory handling; here the cache lives in the project's single SQLite
# database instead. Reads are lock-free SELECTs; writes go through
# ``db.execute_write`` so they hold the connection-wide write lock (#569).


def groq_max_upload_bytes() -> int:
    """Upload ceiling for the groq engine, in bytes.

    ``TELEGRAM_TRANSCRIBE_GROQ_MAX_MB`` raises or lowers it: a paid Groq tier
    accepts larger files than the free one, and hardcoding the free-tier number
    would refuse recordings the account can actually transcribe.
    """
    limit_mb = _env_float("TELEGRAM_TRANSCRIBE_GROQ_MAX_MB", GROQ_DEFAULT_MAX_MB)
    return int(limit_mb * 1024 * 1024)


def _too_large(size: int, limit: int, cfg: HttpEngineConfig) -> dict:
    return {
        "status": "error",
        "reason": "too_large",
        "error": (
            f"recording is {size / 1048576:.1f} MB, above the "
            f"{limit / 1048576:.0f} MB {cfg.label} upload limit. Transcribe it with "
            f"engine='telegram' or engine='whisper', or raise {cfg.max_mb_env} if the "
            f"{cfg.label} endpoint accepts larger uploads."
        ),
    }


async def get_cached_transcript(
    db: Database, chat_id: int, message_id: int, source: Optional[str] = None
) -> Optional[dict]:
    """Cached transcript for a message.

    ``source`` pins the engine: asking for groq must never be answered with a
    telegram transcript, because the native engine drops the recording's last
    segment and the loss is invisible in the text. Callers that only want to
    display whatever exists (listings, backfill skip checks) pass None and get
    the default engine's row when there is one, any row otherwise.
    """
    if source is not None:
        rows = await db.execute_fetchall(
            "SELECT source, text, duration, lang, created_at FROM transcripts "
            "WHERE chat_id = ? AND message_id = ? AND source = ?",
            (chat_id, message_id, source),
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT source, text, duration, lang, created_at FROM transcripts "
            "WHERE chat_id = ? AND message_id = ? "
            "ORDER BY source = ? DESC, created_at DESC LIMIT 1",
            (chat_id, message_id, default_engine()),
        )
    if not rows:
        return None
    row_source, text, duration, lang, created_at = rows[0]
    return {
        "source": row_source,
        "text": text,
        "duration": duration,
        "lang": lang,
        "created_at": created_at,
    }


async def save_transcript(
    db: Database,
    chat_id: int,
    message_id: int,
    source: str,
    text: str,
    *,
    duration: Optional[int] = None,
    lang: Optional[str] = None,
) -> None:
    await db.execute_write(
        "INSERT OR REPLACE INTO transcripts "
        "(chat_id, message_id, source, text, duration, lang, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            chat_id,
            message_id,
            source,
            text,
            duration,
            lang,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


# ---------------------------------------------------------------------------
# Voice/video-note detection and formatting helpers
# ---------------------------------------------------------------------------


def marked_chat_id(entity) -> int:
    """Telethon-compatible marked id for the transcript cache key
    (channels ``-100…``, basic chats ``-…``, users ``+…``)."""
    from telethon import utils

    return int(utils.get_peer_id(entity))


def is_transcribable(msg) -> bool:
    """True for voice notes and video notes (video circles) - both go through
    the same engine path."""
    return getattr(msg, "voice", None) is not None or getattr(msg, "video_note", None) is not None


def voice_duration(msg) -> Optional[int]:
    """Seconds, from the already-fetched message - no extra API call."""
    f = getattr(msg, "file", None)
    if f is None:
        return None
    d = getattr(f, "duration", None)
    return int(d) if d is not None else None


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "?:??"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# ---------------------------------------------------------------------------
# Premium detection
# ---------------------------------------------------------------------------


async def account_is_premium(client) -> bool:
    """Fresh Premium check at call time — Premium can expire or be bought anytime."""
    me = await client.get_me()
    return bool(getattr(me, "premium", False))


def is_premium_rpc_error(error: Exception) -> bool:
    """True when Telegram rejected a call because the account lacks Premium."""
    return "PREMIUM" in getattr(error, "message", str(error)).upper()


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

_POLL_ATTEMPTS = 10
_POLL_INTERVAL_SECONDS = 2.0


async def _transcribe_via_telegram(cl, entity, msg) -> dict:
    """Native Premium transcription. Polls while pending=True - proven live on
    2026-08-20: an 83s recording matured after 3 polls (~6.6s); without
    polling, long voice messages come back empty."""
    if not await account_is_premium(cl):
        return {"status": "premium_required"}
    peer = await cl.get_input_entity(entity)
    try:
        result = await transcribe_audio_raw(cl, peer, msg.id)
    except Exception as e:
        if is_premium_rpc_error(e):
            return {"status": "premium_required"}
        return {"status": "error", "error": error_text(e)}

    attempts = 0
    while getattr(result, "pending", False) and attempts < _POLL_ATTEMPTS:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        attempts += 1
        try:
            result = await transcribe_audio_raw(cl, peer, msg.id)
        except Exception as e:
            return {"status": "error", "error": error_text(e)}

    if getattr(result, "pending", False):
        return {"status": "pending"}
    return {"status": "ok", "text": result.text}


async def _download_audio(cl, msg, limit: Optional[int], cfg=None):
    """Voice note bytes, in memory only (never touches disk).

    Returns ``(data, None)`` or ``(None, error_dict)``. With a ``limit`` an
    oversized recording is refused before it is downloaded when Telegram
    states the size up front, and again after the download when it did not.
    """
    declared = getattr(getattr(msg, "file", None), "size", None)
    if limit is not None and isinstance(declared, int) and declared > limit:
        return None, _too_large(declared, limit, cfg)
    try:
        data = await cl.download_media(msg, file=bytes)
    except Exception as e:
        return None, {"status": "error", "error": f"download failed: {error_text(e)}"}
    if not data:
        return None, {"status": "error", "error": "empty media download"}
    # The declared size can be missing or stale; the bytes in hand cannot.
    if limit is not None and len(data) > limit:
        size = len(data)
        del data
        return None, _too_large(size, limit, cfg)
    return data, None


def _upload_name(msg) -> tuple:
    file_attr = getattr(msg, "file", None)
    ext = (getattr(file_attr, "ext", None) or "ogg").lstrip(".")
    # Telegram voice notes carry 'oga' (Opus-in-Ogg), which Groq (and OpenAI)
    # reject with 400 unsupported_audio_format. Same bytes, named .ogg.
    if ext.lower() == "oga":
        ext = "ogg"
    mime = getattr(file_attr, "mime_type", None) or "audio/ogg"
    return f"voice.{ext}", mime


async def _transcribe_via_openai_compatible(cl, msg, cfg: HttpEngineConfig) -> dict:
    """POST the recording to an OpenAI-style ``/audio/transcriptions``
    endpoint (groq, openai). Deletes the bytes as soon as the
    request returns."""
    if not cfg.url:
        return {"status": "error", "error": engine_config_error(cfg.engine)}

    data, error = await _download_audio(cl, msg, cfg.max_bytes, cfg)
    if error is not None:
        return error
    assert data is not None  # error is not None above exactly when data is None

    filename, mime = _upload_name(msg)
    form = {"model": cfg.model, "response_format": cfg.response_format}
    language = transcribe_language()
    if language:
        form["language"] = language
    headers = {"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {}

    try:
        async with httpx.AsyncClient(timeout=cfg.timeout) as client:
            resp = await client.post(
                cfg.url,
                headers=headers,
                data=form,
                files={"file": (filename, bytes(data), mime)},
            )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        return {"status": "error", "error": f"{cfg.engine} request failed: {error_text(e)}"}
    finally:
        del data

    text = (payload.get("text") or "").strip()
    if not text:
        return {"status": "error", "error": f"empty transcript from {cfg.engine}"}
    return {"status": "ok", "text": text, "lang": payload.get("language") or language}


async def _transcribe_via_groq(cl, msg) -> dict:
    """Groq whisper-large-v3-turbo."""
    if not os.getenv("GROQ_API_KEY"):
        return {"status": "error", "error": "GROQ_API_KEY is not configured"}
    return await _transcribe_via_openai_compatible(cl, msg, http_engine_config("groq"))


# Local faster-whisper. Loading a model takes seconds and hundreds of MB, so
# one instance per (model, device, compute type) lives for the process. The
# semaphore runs one inference at a time: two concurrent CPU transcriptions
# only slow each other down.
_WHISPER_MODELS: dict = {}
_WHISPER_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _whisper_settings() -> tuple:
    return (
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_MODEL", "").strip() or WHISPER_DEFAULT_MODEL,
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_DEVICE", "").strip() or "auto",
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_COMPUTE_TYPE", "").strip() or "default",
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_MODEL_DIR", "").strip() or None,
    )


def _load_whisper_model(settings: tuple):
    model = _WHISPER_MODELS.get(settings)
    if model is None:
        from faster_whisper import WhisperModel

        name, device, compute_type, download_root = settings
        model = WhisperModel(
            name, device=device, compute_type=compute_type, download_root=download_root
        )
        _WHISPER_MODELS[settings] = model
    return model


def _run_whisper(data: bytes, settings: tuple, language: Optional[str]) -> tuple:
    model = _load_whisper_model(settings)
    segments, info = model.transcribe(io.BytesIO(data), language=language, vad_filter=True)
    # segments is a lazy generator: the actual decoding happens here, so it
    # must be consumed inside the worker thread.
    text = "".join(segment.text for segment in segments).strip()
    return text, getattr(info, "language", None)


async def _transcribe_via_whisper(cl, msg) -> dict:
    """Local faster-whisper. The audio never leaves this machine."""
    global _WHISPER_SEMAPHORE
    error = engine_config_error("whisper")
    if error:
        return {"status": "error", "error": error}

    data, error = await _download_audio(cl, msg, None)
    if error is not None:
        return error
    assert data is not None  # error is not None above exactly when data is None

    if _WHISPER_SEMAPHORE is None:
        _WHISPER_SEMAPHORE = asyncio.Semaphore(1)
    language = transcribe_language()
    try:
        async with _WHISPER_SEMAPHORE:
            text, lang = await asyncio.to_thread(
                _run_whisper, bytes(data), _whisper_settings(), language
            )
    except Exception as e:
        return {"status": "error", "error": f"whisper failed: {error_text(e)}"}
    finally:
        del data

    if not text:
        return {"status": "error", "error": "empty transcript from whisper"}
    return {"status": "ok", "text": text, "lang": lang or language}


async def transcribe(cl, entity, msg, engine: str) -> dict:
    """Dispatch to one engine. Returns a status dict:
    {"status": "ok", "text": ..., "lang": ...} |
    {"status": "pending"} | {"status": "premium_required"} |
    {"status": "error", "error": ...}
    """
    if engine == "telegram":
        return await _transcribe_via_telegram(cl, entity, msg)
    if engine == "groq":
        return await _transcribe_via_groq(cl, msg)
    if engine == "whisper":
        return await _transcribe_via_whisper(cl, msg)
    cfg = http_engine_config(engine)
    if cfg is not None:
        return await _transcribe_via_openai_compatible(cl, msg, cfg)
    return {"status": "error", "error": f"Unknown engine '{engine}'"}


# ---------------------------------------------------------------------------
# One paid call per recording
# ---------------------------------------------------------------------------
#
# transcribe_cached does "miss the cache, transcribe, save". Two concurrent
# requests for the same uncached message would both miss, both download the
# audio and both pay Groq for it. The lock is keyed by (chat, message, engine)
# and the cache is re-read inside it, so the second caller returns the first
# one's transcript without a second call.
#
# Scope is one process: the runtime is a single asyncio process over one
# SQLite database. Running it as several worker processes would need an
# atomic claim in the database instead - the lock below would not see the
# other workers.

_INFLIGHT_LOCKS: dict = {}


@asynccontextmanager
async def _transcribe_lock(key):
    entry = _INFLIGHT_LOCKS.get(key)
    if entry is None:
        entry = [asyncio.Lock(), 0]
        _INFLIGHT_LOCKS[key] = entry
    entry[1] += 1
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        # Dropping the entry keeps the registry from growing with every
        # message ever transcribed, and keeps a lock from outliving the loop
        # it was first awaited on.
        if entry[1] <= 0:
            _INFLIGHT_LOCKS.pop(key, None)


def _cached_result(row: dict) -> dict:
    return {
        "status": "ok",
        "cached": True,
        "text": row["text"],
        "lang": row.get("lang"),
        "duration": row.get("duration"),
        "source": row.get("source"),
    }


async def transcribe_cached(
    cl, entity, msg, engine: str, db: Database, chat_id: int, duration: Optional[int] = None
) -> dict:
    """Cache-first transcription, at most one paid call per recording.

    Same status dict as :func:`transcribe`, plus ``cached``. Every caller that
    would otherwise write to the transcript cache should go through here.
    """
    message_id = getattr(msg, "id", None)
    if message_id is None:
        return {"status": "error", "error": "message has no id; cannot key a transcript"}
    hit = await get_cached_transcript(db, chat_id, message_id, source=engine)
    if hit is not None:
        return _cached_result(hit)

    async with _transcribe_lock((chat_id, message_id, engine)):
        # Re-read: a concurrent caller may have paid for this one while we
        # were waiting for the lock.
        hit = await get_cached_transcript(db, chat_id, message_id, source=engine)
        if hit is not None:
            return _cached_result(hit)

        if duration is None:
            duration = voice_duration(msg)
        result = await transcribe(cl, entity, msg, engine)
        if result.get("status") == "ok":
            await save_transcript(
                db,
                chat_id,
                message_id,
                engine,
                result["text"],
                duration=duration,
                lang=result.get("lang"),
            )
        out = dict(result)
        out["cached"] = False
        out.setdefault("duration", duration)
        out.setdefault("source", engine)
        return out


# ---------------------------------------------------------------------------
# Batch budget (for the future auto-transcription of listings)
# ---------------------------------------------------------------------------
#
# Groq is not a free call like native transcription: each voice message costs
# a download + upload (and a self-hosted engine costs CPU time). Without a
# budget, reading a batch of messages with a few dozen voice notes would turn
# into a few dozen downloads/uploads. See TELEGRAM_TRANSCRIBE_MAX_VOICES /
# _MAX_SECONDS.


class TranscribeBudget:
    def __init__(
        self,
        max_voices: Optional[int] = None,
        max_seconds: Optional[int] = None,
    ):
        self.max_voices = max_voices if max_voices is not None else _max_voices_env()
        self.max_seconds = max_seconds if max_seconds is not None else _max_seconds_env()
        self.used_voices = 0
        self.used_seconds = 0

    def can_afford(self, duration: Optional[int]) -> bool:
        if self.used_voices >= self.max_voices:
            return False
        if self.used_seconds + (duration or 0) > self.max_seconds:
            return False
        return True

    def charge(self, duration: Optional[int]) -> None:
        self.used_voices += 1
        self.used_seconds += duration or 0


def _max_voices_env() -> int:
    try:
        return int(os.getenv("TELEGRAM_TRANSCRIBE_MAX_VOICES", "5"))
    except ValueError:
        return 5


def _max_seconds_env() -> int:
    try:
        return int(os.getenv("TELEGRAM_TRANSCRIBE_MAX_SECONDS", "300"))
    except ValueError:
        return 300
