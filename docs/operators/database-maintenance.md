# Database maintenance

The SQLite database can retain pages freed by large deletes or index removal.
Check this from a running instance with `debug memory` (or `GET /debug/memory`):
the `database` object reports `page_count`, `freelist_count`, and
`freelist_percent` and `file_size_bytes`.

## Reclaiming free pages

`VACUUM` is blocking and needs free disk space approximately equal to the
database file. Schedule it during a maintenance window:

1. Stop `serve` and `worker` (and confirm no other process has the database open).
2. Check free disk space and make a filesystem backup of `data/tg_search.db`.
3. Record the current `page_count`, `freelist_count`, and `file_size_bytes`.
4. Run `sqlite3 data/tg_search.db 'PRAGMA quick_check;'` and proceed only if it
   reports `ok`.
5. Run `sqlite3 data/tg_search.db 'VACUUM;'`.
6. Verify `PRAGMA quick_check;` again and confirm `freelist_count` is near zero.
7. Start the application and check `debug memory` once more.

Do not run `VACUUM` against the production file while the application is
running. `auto_vacuum` remains unchanged by the application; changing it on an
existing database also requires a `VACUUM` and should be planned separately.
