# Third-party notices

Vendored third-party code carried in this repository, with license and provenance.
Each vendored file carries its own `SPDX-License-Identifier` header and a
provenance comment listing the source repo, commit, date, and modifications.

## telegram-mcp (Apache-2.0)

- **Source:** [axisrow/telegram-mcp](https://github.com/axisrow/telegram-mcp), commit `e224762`
- **License:** Apache-2.0 (full text: [`LICENSES/apache-2.0.txt`](LICENSES/apache-2.0.txt))

- `tg_agent/telegram/schema_drift.py` — schema-drift detection (`_is_schema_drift` and
  the schema-drift message of `log_and_format_error`) extracted from
  `telegram_mcp/runtime.py` at commit `e224762671897c1df53deadb51be84eaaaf2cea3`
  (2026-10-13); the rest of the upstream file is not ported.
- `tg_agent/telegram/rich_text.py` — RichText/page-block flattening (`rich_text_to_str`,
  `_page_lines` and the `_RICH_TEXT_FIELDS`/`_PAGE_TEXT_FIELDS`/`_RICH_TEXT_TYPES`
  constants) extracted from `telegram_mcp/runtime.py` at commit
  `e224762671897c1df53deadb51be84eaaaf2cea3` (2026-10-16, #1547). Modifications:
  the block-join tail of upstream's `rich_message_text` lifted into a shared
  `_blocks_text`; `webpage_page_text` is our own addition (same walk applied to
  `MessageMediaWebPage` cached pages); upstream's `rich_message_text` wrapper
  itself is not ported — telethon 1.45 does not surface `msg.rich_message`.
- `tg_agent/telegram/session_lock.py` — per-session cross-process lock (`SessionLock`,
  `SessionLockError` and the fcntl/msvcrt primitives) from `telegram_mcp/singleton.py`
  at commit `e224762671897c1df53deadb51be84eaaaf2cea3` (2026-10-16, #1548).
  Modifications: docstrings/error wording recontextualised for tg-agent processes and
  the lock-directory default renamed; the process-level lock registry and the
  `acquire_session_lock()` wrapper are tg-agent wiring added on top of the vendored
  code in later commits.

- `tg_agent/utils/sanitize.py` — prompt-injection sanitizer (`sanitize_user_content`,
  `sanitize_name`, `sanitize_dict`) ported from `sanitize.py` at commit
  `e224762671897c1df53deadb51be84eaaaf2cea3` (2026-10-16). Modifications:
  module docstring adapted to this project; MCP-specific
  `format_tool_result`/`_json_default` dropped (this project feeds plain-text
  prompts, not JSON tool-result envelopes).

- `TelegramActionService.edit_message_media` (issue #1853) — the
  replace-attachment-without-repost behaviour (read the current message first and re-apply
  its caption text + formatting entities with `parse_mode=None`, `file=` on the edit call,
  omitted-caption preservation vs empty-caption clear) follows
  `telegram_mcp/tools/media.py::edit_message_media` from PR
  chigwell/telegram-mcp#398 (merged 2026-11-10). Modifications: implemented as a method of
  our own MIT-licensed `TelegramActionService` rather than a vendored file — the flow runs
  through the shared entity resolver, flood-wait wrapper and transport session, so no
  upstream code is carried verbatim; upstream's MP4 video-attribute parser and the
  MessageIDs append to send responses are not ported. Local-file existence check added.
  Long-term plan: treat this as already-rewritten (nothing left to relicense) — the entry
  records provenance only.

- `tg_agent/telegram/transcription.py` — vendored from `telegram_mcp/transcription.py`
  at the same upstream commit `e224762671897c1df53deadb51be84eaaaf2cea3`
  (2026-10-16, issue #1545).
  - Upstream copyright: pathknower, Arnout van Westen, Omid Zaferi,
    Alexander Kulikov, 4erdenko.
  - Local modifications (adaptation commit on top of the verbatim vendor commit):
    transcript cache moved from a standalone SQLite file into the project
    database (`transcripts` table) with async access via the shared `Database`
    facade and its connection-wide write lock; the upstream legacy-schema
    rebuild and listing/prefetch/render helpers dropped (no legacy schema exists
    here; auto-transcription of listings is a separate follow-up); premium
    detection inlined and the raw `messages.transcribeAudio` request issued
    through `tg_agent/telegram/backends.py` (`transcribe_audio_raw`) per the
    project's raw-Telethon allowlist; install hint renamed for this project.
  - Long-term plan: replace the vendored Apache-2.0 code with an original
    MIT-licensed implementation (algorithms are not licensable); once a file is
    fully rewritten it is marked MIT and removed from this notice.

## chigwell/telegram-mcp (Apache-2.0)

- **Source:** [chigwell/telegram-mcp](https://github.com/chigwell/telegram-mcp),
  PR #384, head commit `081a6db60d00c586f4837ed319fb57e4299d3402`
- **License:** Apache-2.0 (full text: [`LICENSES/apache-2.0.txt`](LICENSES/apache-2.0.txt))

- `tg_agent/services/chat_history_export.py` — Telegram-Desktop-style live
  history export (page walk newest→oldest, `{"chats": ...}` file schema,
  resume keyed by chat id, `limit`/`offset_date` bounds), adapted from
  upstream PR #384 (merged 2026-11-07, issue #1855). Modifications: pages go
  through the established `read_history` action (works for the gateway's
  RemoteActionService too) instead of a raw `get_messages` loop on a bare
  client; chat records reuse the project's `DialogMessage` mapping; resume
  loads/saves via the project conventions; `include_media_metadata` dropped
  (`media_type` is always present on records); chat_title carries the
  caller's identifier string (no extra entity fetch).
- `tests/test_chat_history_export.py` — behavioral scenarios (pagination,
  limit, offset-date, resume, empty chat, multi-chat) adapted from the
  upstream tests of the same PR.

The full Apache-2.0 license text is in `LICENSES/apache-2.0.txt`.

Per the project licensing strategy, the long-term goal is to replace this
vendored Apache-2.0 code with an original MIT implementation; when a file is
fully rewritten, its SPDX header becomes MIT and this entry is removed.
