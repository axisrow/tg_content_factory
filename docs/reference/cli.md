# CLI Reference

```
python -m src.main [--config CONFIG] <command> [subcommand] [options]
```

## serve

```bash
python -m src.main serve [--web-pass PASS]
```

## stop / restart

```bash
python -m src.main stop
python -m src.main restart [--web-pass PASS]
```

## collect

```bash
python -m src.main collect [--channel-id ID]
python -m src.main collect sample CHANNEL_ID [--limit N]
```

## search

```bash
python -m src.main search "query" [--limit N] [--mode MODE]
```

Modes (`--mode`): `local` (default), `semantic`, `hybrid`, `telegram`, `my_chats`, `channel`.

## messages

```bash
python -m src.main messages read IDENTIFIER [--limit N] [--live] [--phone PHONE] [--query TEXT] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD] [--topic-id ID] [--offset-id ID] [--format text|json|csv]
```

## channel

```bash
python -m src.main channel list
python -m src.main channel add IDENTIFIER
python -m src.main channel delete IDENTIFIER
python -m src.main channel toggle IDENTIFIER
python -m src.main channel collect IDENTIFIER
python -m src.main channel stats [IDENTIFIER] [--all]
python -m src.main channel refresh-types
python -m src.main channel refresh-meta [IDENTIFIER] [--all]
python -m src.main channel import FILE_OR_TEXT
python -m src.main channel add-bulk --phone PHONE --dialog-ids ID1,ID2,...
python -m src.main channel tag list
python -m src.main channel tag add NAME
python -m src.main channel tag delete NAME
python -m src.main channel tag set PK TAG1,TAG2
python -m src.main channel tag get PK
```

## filter

```bash
python -m src.main filter analyze
python -m src.main filter apply
python -m src.main filter reset
python -m src.main filter precheck
python -m src.main filter toggle PK
python -m src.main filter purge [--pks PK1,PK2]
python -m src.main filter purge-messages --channel-id ID [--yes]
python -m src.main filter hard-delete [--pks PK1,PK2] [--yes]
```

## search-query

```bash
python -m src.main search-query list
python -m src.main search-query add "query" [--interval MINUTES] [--regex] [--fts] [--notify]
python -m src.main search-query edit ID [--query TEXT] [--interval MINUTES]
python -m src.main search-query delete ID
python -m src.main search-query toggle ID
python -m src.main search-query run ID
python -m src.main search-query stats ID [--days N]
```

## pipeline

```bash
python -m src.main pipeline list
python -m src.main pipeline show ID
python -m src.main pipeline add NAME --prompt-template TEXT --source CHANNEL_ID --target PHONE|DIALOG_ID [--source CHANNEL_ID ...] [--target PHONE|DIALOG_ID ...]
python -m src.main pipeline edit ID [--name TEXT] [--prompt-template TEXT]
python -m src.main pipeline delete ID
python -m src.main pipeline toggle ID
python -m src.main pipeline run ID [--preview | --publish]
python -m src.main pipeline generate ID [--model MODEL] [--preview]
python -m src.main pipeline runs ID [--limit N] [--status STATUS]
python -m src.main pipeline run-show RUN_ID
python -m src.main pipeline queue ID [--limit N]
python -m src.main pipeline publish RUN_ID
python -m src.main pipeline approve RUN_ID
python -m src.main pipeline reject RUN_ID
python -m src.main pipeline bulk-approve RUN_ID [RUN_ID ...]
python -m src.main pipeline bulk-reject RUN_ID [RUN_ID ...]
python -m src.main pipeline refinement-steps ID [--set JSON]
python -m src.main pipeline filter set ID [--message-kind KIND ...] [--service-action ACTION ...] [--media-type TYPE ...] [--sender-kind KIND ...] [--keyword TEXT ...] [--regex PATTERN] [--forwarded true|false] [--has-text true|false]
python -m src.main pipeline filter show ID
python -m src.main pipeline filter clear ID
```

## image

```bash
python -m src.main image generate "prompt" [--model provider:model_id]
python -m src.main image models --provider PROVIDER [--query TEXT]
python -m src.main image providers
```

## account

```bash
python -m src.main account list
python -m src.main account info [--phone PHONE]
python -m src.main account toggle ID
python -m src.main account delete ID
python -m src.main account add --phone PHONE [--api-id ID] [--api-hash HASH]
python -m src.main account flood-status
python -m src.main account flood-clear --phone PHONE
```

## scheduler

```bash
python -m src.main scheduler start
python -m src.main scheduler trigger
python -m src.main scheduler status
python -m src.main scheduler stop
python -m src.main scheduler job-toggle JOB_ID
python -m src.main scheduler set-interval JOB_ID MINUTES
python -m src.main scheduler task-cancel TASK_ID
python -m src.main scheduler clear-pending
```

## dialogs

```bash
python -m src.main dialogs list [--phone PHONE]
python -m src.main dialogs refresh [--phone PHONE]
python -m src.main dialogs leave DIALOG_ID [DIALOG_ID ...] [--phone PHONE] [--yes]
python -m src.main dialogs topics --channel-id ID [--phone PHONE]
python -m src.main dialogs cache-status
python -m src.main dialogs cache-clear [--phone PHONE]
python -m src.main dialogs send RECIPIENT TEXT [--phone PHONE] [--yes]
python -m src.main dialogs forward FROM_CHAT TO_CHAT MESSAGE_ID [MESSAGE_ID ...] [--phone PHONE] [--yes]
python -m src.main dialogs edit-message CHAT_ID MESSAGE_ID TEXT [--phone PHONE] [--yes]
python -m src.main dialogs delete-message CHAT_ID MESSAGE_ID [MESSAGE_ID ...] [--phone PHONE] [--yes]
python -m src.main dialogs pin-message CHAT_ID MESSAGE_ID [--phone PHONE] [--notify] [--yes]
python -m src.main dialogs unpin-message CHAT_ID [--message-id ID] [--phone PHONE] [--yes]
python -m src.main dialogs download-media CHAT_ID MESSAGE_ID [--phone PHONE] [--output-dir DIR]
python -m src.main dialogs participants CHAT_ID [--phone PHONE] [--limit N] [--search QUERY]
python -m src.main dialogs edit-admin CHAT_ID USER_ID [--phone PHONE] [--title TITLE] [--is-admin|--no-admin] [--yes]
python -m src.main dialogs edit-permissions CHAT_ID USER_ID [--phone PHONE] [--until-date ISO_DATE] [--send-messages BOOL] [--send-media BOOL] [--yes]
python -m src.main dialogs kick CHAT_ID USER_ID [--phone PHONE] [--yes]
python -m src.main dialogs broadcast-stats CHAT_ID [--phone PHONE]
python -m src.main dialogs archive CHAT_ID [--phone PHONE]
python -m src.main dialogs unarchive CHAT_ID [--phone PHONE]
python -m src.main dialogs mark-read CHAT_ID [--phone PHONE] [--max-id ID]
python -m src.main dialogs create-channel --title TITLE [--about TEXT] [--username USERNAME] [--phone PHONE]
```

Legacy alias `my-telegram` is still accepted for backward compatibility, but `dialogs` is the primary documented name.

## notification

```bash
python -m src.main notification setup
python -m src.main notification status
python -m src.main notification delete
python -m src.main notification test [--message TEXT]
python -m src.main notification dry-run
python -m src.main notification set-account --phone PHONE
```

## agent

```bash
python -m src.main agent threads
python -m src.main agent thread-create [--title TITLE]
python -m src.main agent thread-delete THREAD_ID
python -m src.main agent chat [-p TEXT] [--thread-id ID] [--model MODEL]
python -m src.main agent thread-rename THREAD_ID TITLE
python -m src.main agent messages THREAD_ID [--limit N]
python -m src.main agent context THREAD_ID --channel-id ID [--limit N] [--topic-id ID]
python -m src.main agent test-escaping
python -m src.main agent test-tools
```

## photo-loader

```bash
python -m src.main photo-loader dialogs --phone PHONE
python -m src.main photo-loader refresh --phone PHONE
python -m src.main photo-loader send --phone PHONE --target DIALOG_ID --files FILE [FILE ...] [--mode album|separate] [--caption TEXT]
python -m src.main photo-loader schedule-send --phone PHONE --target DIALOG_ID --files FILE [FILE ...] --at ISO_DATETIME [--mode album|separate] [--caption TEXT]
python -m src.main photo-loader batch-create --phone PHONE --target DIALOG_ID --manifest FILE [--caption TEXT]
python -m src.main photo-loader batch-list
python -m src.main photo-loader batch-cancel ID
python -m src.main photo-loader auto-create --phone PHONE --target DIALOG_ID --folder PATH --interval MINUTES [--mode album|separate] [--caption TEXT]
python -m src.main photo-loader auto-list
python -m src.main photo-loader auto-update ID [--folder PATH] [--interval MINUTES] [--mode album|separate] [--caption TEXT] [--active|--paused]
python -m src.main photo-loader auto-toggle ID
python -m src.main photo-loader auto-delete ID
python -m src.main photo-loader run-due
```

## analytics

```bash
python -m src.main analytics top [--limit N] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m src.main analytics content-types [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m src.main analytics hourly [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
python -m src.main analytics summary
python -m src.main analytics daily [--days N] [--pipeline-id ID]
python -m src.main analytics pipeline-stats [--pipeline-id ID]
python -m src.main analytics trending-topics [--days N] [--limit N]
python -m src.main analytics trending-channels [--days N] [--limit N]
python -m src.main analytics velocity [--days N]
python -m src.main analytics peak-hours
python -m src.main analytics calendar [--limit N] [--pipeline-id ID]
python -m src.main analytics trending-emojis [--days N] [--limit N]
```

## provider

```bash
python -m src.main provider list
python -m src.main provider add NAME --api-key KEY [--base-url URL]
python -m src.main provider delete NAME
python -m src.main provider probe NAME
python -m src.main provider refresh [NAME]
python -m src.main provider test-all
```

## export

```bash
python -m src.main export json [--channel-id ID] [--limit N] [--output FILE]
python -m src.main export csv [--channel-id ID] [--limit N] [--output FILE]
python -m src.main export rss [--channel-id ID] [--limit N] [--output FILE]
```

## translate

```bash
python -m src.main translate stats
python -m src.main translate detect [--batch-size N]
python -m src.main translate run [--target LANG] [--source-filter LANGS] [--limit N]
python -m src.main translate message MESSAGE_ID [--target LANG]
```

## settings

```bash
python -m src.main settings get [--key KEY]
python -m src.main settings set KEY VALUE
python -m src.main settings info
python -m src.main settings agent [--backend BACKEND] [--prompt-template TEMPLATE]
python -m src.main settings filter-criteria [--min-uniqueness N] [--min-sub-ratio N] [--max-cross-dupe N] [--min-cyrillic N]
python -m src.main settings semantic [--provider PROVIDER] [--model MODEL] [--api-key KEY]
```

## debug

```bash
python -m src.main debug logs [--limit N]
python -m src.main debug memory
python -m src.main debug timing
```

## test

```bash
python -m src.main test all
python -m src.main test read
python -m src.main test write
python -m src.main test telegram
python -m src.main test benchmark
```
