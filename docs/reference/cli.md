# CLI Reference

> `tg-agent` (ставится с пакетом) = `python -m tg_agent.main`; ниже команды показаны в `python -m`-форме. Каталог для агентов: `skills/tg-agent/reference.md`.

```
python -m tg_agent.main [--config CONFIG] <command> [subcommand] [options]
```

## serve (легаси — веб-панель)

```bash
python -m tg_agent.main serve [--web-pass PASS] [--no-worker]
```

By default, `serve` starts the web UI/API and an embedded Telegram worker in
the same process. Use `--no-worker` when a process manager starts
`python -m tg_agent.main worker` separately.

## stop / restart

```bash
python -m tg_agent.main stop
python -m tg_agent.main restart [--web-pass PASS]
```

## collect

```bash
python -m tg_agent.main collect [--channel-id ID]
python -m tg_agent.main collect sample CHANNEL_ID [--limit N]
```

## search

```bash
python -m tg_agent.main search "query" ["query2" ...] [--limit N] [--mode MODE] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD] [--format text|json|csv]
```

Modes (`--mode`): `local` (default), `semantic`, `hybrid`, `telegram`, `my_chats`, `channel`.

`--date-from`/`--date-to` work on `local`/`semantic`/`hybrid` (SQL window) and
`telegram`; `my_chats`/`channel` reject them. On `telegram` the window is a
**client-side filter** over the premium global search output — the RPC itself
carries no date bounds (schema layer 225), so period coverage is best-effort,
not exhaustive (#1750).

## messages

```bash
python -m tg_agent.main messages read IDENTIFIER [--limit N] [--live] [--phone PHONE] [--query TEXT] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD] [--topic-id ID] [--offset-id ID] [--format text|json|csv]
```

## channel

```bash
python -m tg_agent.main channel list
python -m tg_agent.main channel add IDENTIFIER
python -m tg_agent.main channel delete IDENTIFIER
python -m tg_agent.main channel toggle IDENTIFIER
python -m tg_agent.main channel collect IDENTIFIER
python -m tg_agent.main channel stats [IDENTIFIER] [--all]
python -m tg_agent.main channel refresh-types
python -m tg_agent.main channel refresh-meta [IDENTIFIER] [--all]
python -m tg_agent.main channel import FILE_OR_TEXT
python -m tg_agent.main channel add-bulk --phone PHONE --dialog-ids ID1,ID2,...
python -m tg_agent.main channel tag list
python -m tg_agent.main channel tag add NAME
python -m tg_agent.main channel tag delete NAME
python -m tg_agent.main channel tag set PK TAG1,TAG2
python -m tg_agent.main channel tag get PK
```

## filter

```bash
python -m tg_agent.main filter analyze
python -m tg_agent.main filter apply
python -m tg_agent.main filter reset
python -m tg_agent.main filter precheck
python -m tg_agent.main filter toggle PK
python -m tg_agent.main filter purge [--pks PK1,PK2]
python -m tg_agent.main filter purge-messages --channel-id ID [--yes]
python -m tg_agent.main filter hard-delete [--pks PK1,PK2] [--yes]
```

## search-query

```bash
python -m tg_agent.main search-query list
python -m tg_agent.main search-query get ID
python -m tg_agent.main search-query add "query" [--interval MINUTES] [--regex] [--fts] [--notify]
python -m tg_agent.main search-query edit ID [--query TEXT] [--interval MINUTES]
python -m tg_agent.main search-query delete ID
python -m tg_agent.main search-query toggle ID
python -m tg_agent.main search-query run ID
python -m tg_agent.main search-query stats ID [--days N]
```

## pipeline

```bash
python -m tg_agent.main pipeline list
python -m tg_agent.main pipeline show ID
python -m tg_agent.main pipeline add NAME --prompt-template TEXT --source CHANNEL_ID --target PHONE|DIALOG_ID [--source CHANNEL_ID ...] [--target PHONE|DIALOG_ID ...]
python -m tg_agent.main pipeline edit ID [--name TEXT] [--prompt-template TEXT]
python -m tg_agent.main pipeline delete ID
python -m tg_agent.main pipeline toggle ID
python -m tg_agent.main pipeline run ID [--preview | --publish]
python -m tg_agent.main pipeline generate ID [--model MODEL] [--preview]
python -m tg_agent.main pipeline runs ID [--limit N] [--status STATUS]
python -m tg_agent.main pipeline run-show RUN_ID
python -m tg_agent.main pipeline queue ID [--limit N]
python -m tg_agent.main pipeline moderation-list [--pipeline-id ID] [--limit N]
python -m tg_agent.main pipeline moderation-view RUN_ID
python -m tg_agent.main pipeline publish RUN_ID
python -m tg_agent.main pipeline approve RUN_ID
python -m tg_agent.main pipeline reject RUN_ID
python -m tg_agent.main pipeline bulk-approve RUN_ID [RUN_ID ...]
python -m tg_agent.main pipeline bulk-reject RUN_ID [RUN_ID ...]
python -m tg_agent.main pipeline refinement-steps ID [--set JSON]
python -m tg_agent.main pipeline filter set ID [--message-kind KIND ...] [--service-action ACTION ...] [--media-type TYPE ...] [--sender-kind KIND ...] [--keyword TEXT ...] [--regex PATTERN] [--forwarded true|false] [--has-text true|false]
python -m tg_agent.main pipeline filter show ID
python -m tg_agent.main pipeline filter clear ID
```

## image

```bash
python -m tg_agent.main image generate "prompt" [--model provider:model_id]
python -m tg_agent.main image models --provider PROVIDER [--query TEXT]
python -m tg_agent.main image providers
python -m tg_agent.main image generated [--limit N]
```

## account

```bash
python -m tg_agent.main account list
python -m tg_agent.main account info [--phone PHONE]
python -m tg_agent.main account toggle ID
python -m tg_agent.main account delete ID
python -m tg_agent.main account add --phone PHONE [--api-id ID] [--api-hash HASH]
python -m tg_agent.main account add --phone PHONE --code CODE [--password PASSWORD]
python -m tg_agent.main account flood-status
python -m tg_agent.main account flood-clear --phone PHONE

# SSO: перенос аккаунта между инстансами (session string = полный доступ!)
python -m tg_agent.main account export-session (--id ID | --phone PHONE) [--json]
python -m tg_agent.main account import --phone PHONE (--session-string STR | --session-string-stdin) [--force]
```

!!! danger "export-session / import раскрывают session string"
    Session string эквивалентна паролю аккаунта. Перенос и правила безопасности
    (HTTPS, audit, защита от перезаписи) — в гайде
    [Operators → SSO Session Transfer](../operators/sso-session-transfer.md).

## scheduler

```bash
python -m tg_agent.main scheduler start
python -m tg_agent.main scheduler trigger
python -m tg_agent.main scheduler status
python -m tg_agent.main scheduler stop
python -m tg_agent.main scheduler job-toggle JOB_ID
python -m tg_agent.main scheduler set-interval JOB_ID MINUTES
python -m tg_agent.main scheduler task-cancel TASK_ID
python -m tg_agent.main scheduler clear-pending
```

## dialogs

```bash
python -m tg_agent.main dialogs list [--phone PHONE]
python -m tg_agent.main dialogs refresh [--phone PHONE]
python -m tg_agent.main dialogs resolve IDENTIFIER [--phone PHONE]
python -m tg_agent.main dialogs leave DIALOG_ID [DIALOG_ID ...] [--phone PHONE] [--yes]
python -m tg_agent.main dialogs topics --channel-id ID [--phone PHONE]
python -m tg_agent.main dialogs cache-status
python -m tg_agent.main dialogs cache-clear [--phone PHONE]
python -m tg_agent.main dialogs send RECIPIENT TEXT [--phone PHONE] [--yes]
python -m tg_agent.main dialogs react CHAT_ID MESSAGE_ID EMOJI [--phone PHONE]
python -m tg_agent.main dialogs forward FROM_CHAT TO_CHAT MESSAGE_ID [MESSAGE_ID ...] [--phone PHONE] [--yes]
python -m tg_agent.main dialogs edit-message CHAT_ID MESSAGE_ID TEXT [--phone PHONE] [--yes]
python -m tg_agent.main dialogs delete-message CHAT_ID MESSAGE_ID [MESSAGE_ID ...] [--phone PHONE] [--yes]
python -m tg_agent.main dialogs pin-message CHAT_ID MESSAGE_ID [--phone PHONE] [--notify] [--yes]
python -m tg_agent.main dialogs unpin-message CHAT_ID [--message-id ID] [--phone PHONE] [--yes]
python -m tg_agent.main dialogs download-media CHAT_ID MESSAGE_ID [--phone PHONE] [--output-dir DIR]
python -m tg_agent.main dialogs participants CHAT_ID [--phone PHONE] [--limit N] [--search QUERY]
python -m tg_agent.main dialogs edit-admin CHAT_ID USER_ID [--phone PHONE] [--title TITLE] [--is-admin|--no-admin] [--yes]
python -m tg_agent.main dialogs edit-permissions CHAT_ID USER_ID [--phone PHONE] [--until-date ISO_DATE] [--send-messages BOOL] [--send-media BOOL] [--yes]
python -m tg_agent.main dialogs kick CHAT_ID USER_ID [--phone PHONE] [--yes]
python -m tg_agent.main dialogs broadcast-stats CHAT_ID [--phone PHONE]
python -m tg_agent.main dialogs archive CHAT_ID [--phone PHONE]
python -m tg_agent.main dialogs unarchive CHAT_ID [--phone PHONE]
python -m tg_agent.main dialogs mark-read CHAT_ID [--phone PHONE] [--max-id ID]
python -m tg_agent.main dialogs create-channel --title TITLE [--about TEXT] [--username USERNAME] [--phone PHONE]
```

`dialogs` is the documented CLI command for Telegram dialogs management.

## notification

```bash
python -m tg_agent.main notification setup
python -m tg_agent.main notification status
python -m tg_agent.main notification delete
python -m tg_agent.main notification test [--message TEXT]
python -m tg_agent.main notification dry-run
python -m tg_agent.main notification set-account --phone PHONE
```

## agent

```bash
python -m tg_agent.main agent threads
python -m tg_agent.main agent thread-create [--title TITLE]
python -m tg_agent.main agent thread-delete THREAD_ID
python -m tg_agent.main agent chat [-p TEXT] [--thread-id ID] [--model MODEL]
python -m tg_agent.main agent thread-rename THREAD_ID TITLE
python -m tg_agent.main agent messages THREAD_ID [--limit N]
python -m tg_agent.main agent context THREAD_ID --channel-id ID [--limit N] [--topic-id ID]
python -m tg_agent.main agent test-escaping
python -m tg_agent.main agent test-tools
```

## photo-loader

```bash
python -m tg_agent.main photo-loader dialogs --phone PHONE
python -m tg_agent.main photo-loader refresh --phone PHONE
python -m tg_agent.main photo-loader send --phone PHONE --target DIALOG_ID --files FILE [FILE ...] [--mode album|separate] [--caption TEXT]
python -m tg_agent.main photo-loader schedule-send --phone PHONE --target DIALOG_ID --files FILE [FILE ...] --at ISO_DATETIME [--mode album|separate] [--caption TEXT]
python -m tg_agent.main photo-loader batch-create --phone PHONE --target DIALOG_ID --manifest FILE [--caption TEXT]
python -m tg_agent.main photo-loader publish BATCH_ID
python -m tg_agent.main photo-loader batch-list
python -m tg_agent.main photo-loader items [--batch-id ID] [--limit N]
python -m tg_agent.main photo-loader batch-cancel ID
python -m tg_agent.main photo-loader auto-create --phone PHONE --target DIALOG_ID --folder PATH --interval MINUTES [--mode album|separate] [--caption TEXT]
python -m tg_agent.main photo-loader auto-list
python -m tg_agent.main photo-loader auto-update ID [--folder PATH] [--interval MINUTES] [--mode album|separate] [--caption TEXT] [--active|--paused]
python -m tg_agent.main photo-loader auto-toggle ID
python -m tg_agent.main photo-loader auto-delete ID
python -m tg_agent.main photo-loader run-due
```

## analytics

```bash
python -m tg_agent.main analytics top [--limit N] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m tg_agent.main analytics content-types [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m tg_agent.main analytics hourly [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m tg_agent.main analytics summary
python -m tg_agent.main analytics daily [--days N] [--pipeline-id ID]
python -m tg_agent.main analytics pipeline-stats [--pipeline-id ID]
python -m tg_agent.main analytics trending-topics [--days N] [--limit N]
python -m tg_agent.main analytics trending-channels [--days N] [--limit N]
python -m tg_agent.main analytics velocity [--days N]
python -m tg_agent.main analytics peak-hours
python -m tg_agent.main analytics calendar [--limit N] [--pipeline-id ID]
python -m tg_agent.main analytics trending-emojis [--days N] [--limit N]
```

## provider

```bash
python -m tg_agent.main provider list
python -m tg_agent.main provider add NAME --api-key KEY [--base-url URL]
python -m tg_agent.main provider delete NAME
python -m tg_agent.main provider probe NAME
python -m tg_agent.main provider refresh [NAME]
python -m tg_agent.main provider test-all
```

## export

```bash
python -m tg_agent.main export json [--channel-id ID] [--limit N] [--output FILE]
python -m tg_agent.main export csv [--channel-id ID] [--limit N] [--output FILE]
python -m tg_agent.main export rss [--channel-id ID] [--limit N] [--output FILE]
```

## translate

```bash
python -m tg_agent.main translate stats
python -m tg_agent.main translate detect [--batch-size N]
python -m tg_agent.main translate run [--target LANG] [--source-filter LANGS] [--limit N]
python -m tg_agent.main translate message MESSAGE_ID [--target LANG]
```

## settings

```bash
python -m tg_agent.main settings get [--key KEY]
python -m tg_agent.main settings set KEY VALUE
python -m tg_agent.main settings info
python -m tg_agent.main settings agent [--backend BACKEND] [--prompt-template TEMPLATE]
python -m tg_agent.main settings filter-criteria [--min-uniqueness N] [--min-sub-ratio N] [--max-cross-dupe N] [--min-cyrillic N]
python -m tg_agent.main settings reactions [--min-interval SECONDS]
python -m tg_agent.main settings semantic [--provider PROVIDER] [--model MODEL] [--api-key KEY]
```

## debug

```bash
python -m tg_agent.main debug logs [--limit N]
python -m tg_agent.main debug memory
python -m tg_agent.main debug timing
```

## test

```bash
python -m tg_agent.main test all
python -m tg_agent.main test read
python -m tg_agent.main test write
python -m tg_agent.main test telegram
python -m tg_agent.main test benchmark
```
