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
python -m src.main restart
```

## search

```bash
python -m src.main search "query" [--limit N] [--mode MODE]
```

Режимы (`--mode`): `fts` (default), `semantic`, `ai`.

## channel

```bash
python -m src.main channel list
python -m src.main channel add IDENTIFIER [--phone PHONE]
python -m src.main channel delete --channel-id ID
python -m src.main channel toggle --channel-id ID
python -m src.main channel collect --channel-id ID [--full]
python -m src.main channel stats --channel-id ID
python -m src.main channel refresh-types
python -m src.main channel refresh-meta [--channel-id ID]
python -m src.main channel import FILE_OR_TEXT
```

## collect

```bash
python -m src.main collect [--channel-id ID]
python -m src.main collect sample [--channel-id ID] [--limit N]
```

## filter

```bash
python -m src.main filter analyze
python -m src.main filter apply
python -m src.main filter reset
python -m src.main filter precheck
python -m src.main filter toggle --channel-id ID
python -m src.main filter purge [--channel-id ID]
python -m src.main filter hard-delete
```

## search-query

```bash
python -m src.main search-query list
python -m src.main search-query add "query" [--regex]
python -m src.main search-query edit ID "new query"
python -m src.main search-query delete ID
python -m src.main search-query toggle ID
python -m src.main search-query run ID
python -m src.main search-query stats ID
```

## pipeline

```bash
python -m src.main pipeline list
python -m src.main pipeline show ID
python -m src.main pipeline add
python -m src.main pipeline edit ID
python -m src.main pipeline delete ID
python -m src.main pipeline toggle ID
python -m src.main pipeline run ID
python -m src.main pipeline generate ID
python -m src.main pipeline runs ID
python -m src.main pipeline run-show RUN_ID
python -m src.main pipeline queue
python -m src.main pipeline publish RUN_ID
python -m src.main pipeline approve RUN_ID
python -m src.main pipeline reject RUN_ID
python -m src.main pipeline bulk-approve RUN_ID [RUN_ID ...]
python -m src.main pipeline bulk-reject RUN_ID [RUN_ID ...]
```

## image

```bash
python -m src.main image generate "prompt" [--model provider:model_id]
python -m src.main image models [--search QUERY]
python -m src.main image providers
```

## account

```bash
python -m src.main account list
python -m src.main account info [--phone PHONE]
python -m src.main account toggle --phone PHONE
python -m src.main account delete --phone PHONE
python -m src.main account flood-status
python -m src.main account flood-clear --phone PHONE
```

## scheduler

```bash
python -m src.main scheduler start
python -m src.main scheduler stop
python -m src.main scheduler trigger
python -m src.main scheduler status
python -m src.main scheduler job-toggle JOB_ID
python -m src.main scheduler set-interval JOB_ID MINUTES
python -m src.main scheduler task-cancel TASK_ID
python -m src.main scheduler clear-pending
```

## my-telegram

```bash
python -m src.main my-telegram list [--phone PHONE]
python -m src.main my-telegram refresh [--phone PHONE]
python -m src.main my-telegram leave DIALOG_ID [DIALOG_ID ...] [--phone PHONE] [--yes]
python -m src.main my-telegram topics --channel-id ID [--phone PHONE]
python -m src.main my-telegram cache-status
python -m src.main my-telegram cache-clear [--phone PHONE]
python -m src.main my-telegram send RECIPIENT TEXT [--phone PHONE] [--yes]
python -m src.main my-telegram edit-message CHAT_ID MESSAGE_ID TEXT [--phone PHONE] [--yes]
python -m src.main my-telegram delete-message CHAT_ID MESSAGE_ID [MESSAGE_ID ...] [--phone PHONE] [--yes]
python -m src.main my-telegram pin-message CHAT_ID MESSAGE_ID [--phone PHONE] [--notify] [--yes]
python -m src.main my-telegram unpin-message CHAT_ID [--message-id ID] [--phone PHONE] [--yes]
python -m src.main my-telegram download-media CHAT_ID MESSAGE_ID [--phone PHONE] [--output-dir DIR]
python -m src.main my-telegram participants CHAT_ID [--phone PHONE] [--limit N] [--search QUERY]
python -m src.main my-telegram edit-admin CHAT_ID USER_ID [--phone PHONE] [--title TITLE] [--yes]
python -m src.main my-telegram edit-permissions CHAT_ID USER_ID [--phone PHONE] [--until-date DATE] [--yes]
python -m src.main my-telegram kick CHAT_ID USER_ID [--phone PHONE] [--yes]
python -m src.main my-telegram broadcast-stats CHAT_ID [--phone PHONE]
python -m src.main my-telegram archive CHAT_ID [--phone PHONE]
python -m src.main my-telegram unarchive CHAT_ID [--phone PHONE]
python -m src.main my-telegram mark-read CHAT_ID [--phone PHONE] [--max-id ID]
python -m src.main my-telegram create-channel --title TITLE [--about TEXT] [--username USER] [--phone PHONE]
```

## notification

```bash
python -m src.main notification setup
python -m src.main notification status
python -m src.main notification delete
python -m src.main notification test [--message TEXT]
python -m src.main notification dry-run
```

## agent

```bash
python -m src.main agent threads
python -m src.main agent thread-create [--title TITLE]
python -m src.main agent thread-delete THREAD_ID
python -m src.main agent chat MESSAGE [--thread-id ID] [--model MODEL]
python -m src.main agent thread-rename THREAD_ID TITLE
python -m src.main agent messages THREAD_ID
python -m src.main agent context THREAD_ID
python -m src.main agent test-escaping
```

## photo-loader

```bash
python -m src.main photo-loader dialogs [--phone PHONE]
python -m src.main photo-loader refresh [--phone PHONE]
python -m src.main photo-loader send [--phone PHONE]
python -m src.main photo-loader schedule-send [--phone PHONE]
python -m src.main photo-loader batch-create MANIFEST
python -m src.main photo-loader batch-list
python -m src.main photo-loader batch-cancel ITEM_ID
python -m src.main photo-loader auto-create
python -m src.main photo-loader auto-list
python -m src.main photo-loader auto-update JOB_ID
python -m src.main photo-loader auto-toggle JOB_ID
python -m src.main photo-loader auto-delete JOB_ID
python -m src.main photo-loader run-due
```

## analytics

```bash
python -m src.main analytics top [--limit N]
python -m src.main analytics content-types
python -m src.main analytics hourly
python -m src.main analytics summary
python -m src.main analytics daily
python -m src.main analytics pipeline-stats
python -m src.main analytics trending-topics
python -m src.main analytics trending-channels
python -m src.main analytics velocity
python -m src.main analytics peak-hours
python -m src.main analytics calendar
python -m src.main analytics export
```

## settings

```bash
python -m src.main settings get [KEY]
python -m src.main settings set KEY VALUE
python -m src.main settings info
```

## test

```bash
python -m src.main test all
python -m src.main test read
python -m src.main test write
python -m src.main test telegram
python -m src.main test benchmark
```
