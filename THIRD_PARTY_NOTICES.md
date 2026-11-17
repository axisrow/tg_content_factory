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

- `tg_agent/telegram/outgoing_text.py` and the drafts flow
  (`TelegramActionService.save_draft`/`list_drafts`, CLI `dialogs draft-save|draft-list`,
  agent tools `save_draft`/`get_drafts`, issue #1854) — the drafts-keep-formatting
  behaviour (html/md pre-parse into real entities via telethon.extensions, unknown
  parse_mode refused before any request, `message_html` = html.unparse of a draft's raw
  entities that round-trips through save_draft(parse_mode="html")) follows
  `telegram_mcp/tools/messages.py::save_draft`/`get_drafts` from PR
  chigwell/telegram-mcp#364 (merged 2026-11-07). Modifications: implemented in our own
  MIT-licensed layers (raw `SaveDraftRequest`/`GetAllDraftsRequest` in
  `tg_agent/telegram/backends.py` because telethon 1.45 has no client.edit_draft and its
  Draft wrapper drops entities; entity titles come from the same reply instead of a
  second resolve; sanitize_user_content replaces upstream's sanitizer). Upstream's
  unified formatting module (their PR #367) was still open at port time and is not
  ported. Long-term plan: treat this as already-rewritten (nothing left to relicense) —
  the entry records provenance only.

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

The full Apache-2.0 license text is in `LICENSES/apache-2.0.txt`.

Per the project licensing strategy, the long-term goal is to replace this
vendored Apache-2.0 code with an original MIT implementation; when a file is
fully rewritten, its SPDX header becomes MIT and this entry is removed.

## chigwell/telegram-mcp — agent tool floodgate boundary (#1851, 2026-11-17)

The **agent tool dispatch boundary** of `tg_agent/agent/tools/floodgate.py`
ports the design of
[chigwell/telegram-mcp#363](https://github.com/chigwell/telegram-mcp/pull/363)
(merged 2026-11-10, Apache-2.0): the defer/settle protocol around every tool
call, the reactive circuit breaker fed by real FloodWaitErrors, the
tool→gate-category registry declared next to each tool, the instructional
rate-limit reply agents schedule around, and the environment kill switch.
The behavioral specification in `tests/test_agent_floodgate_boundary.py` is
a port of the upstream `tests/test_floodgate_runtime.py`. No upstream code
is vendored verbatim: the gate/breaker primitives come from the MIT-
licensed `telethon-floodgate` PyPI dependency, composed over the pool-owned
instances; flood attribution rides the pool's flood reporters via
`tg_agent/telegram/tool_flood_scope.py` (this codebase's single funnel for
real FloodWaitErrors) instead of an error-formatter contextvar. The
boundary **peeks** the gate instead of charging it — the shared dispatch
guard (#1840) already charges the same buckets one boundary down. Per the
project licensing strategy, the ported behavior is planned to be rewritten
as an original MIT implementation once it settles.
