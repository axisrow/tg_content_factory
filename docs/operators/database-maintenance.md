# Database maintenance

The SQLite database can retain pages freed by large deletes or index removal.
Check this with `tg-agent debug memory`: its `SQLite storage` block reports
`page_count`, `freelist_count` and `freelist_percent` (plus the DB file size),
and warns when the freelist reaches 25%.

## Reclaiming free pages

`VACUUM` is blocking and needs free disk space approximately equal to the
database file. Schedule it during a maintenance window:

1. Stop the worker daemon (`tg-agent stop`; check with `tg-agent daemon status`)
   and confirm no other process has the database open.
2. Check free disk space and make a filesystem backup of `data/tg_search.db`.
3. Record the current `page_count`, `freelist_count`, and DB file size.
4. Run `sqlite3 data/tg_search.db 'PRAGMA quick_check;'` and proceed only if it
   reports `ok`.
5. Run `sqlite3 data/tg_search.db 'VACUUM;'`.
6. Verify `PRAGMA quick_check;` again and confirm `freelist_count` is near zero.
7. Start the daemon again (`tg-agent restart`) and check `debug memory` once more.

Do not run `VACUUM` against the production file while the application is
running. `auto_vacuum` remains unchanged by the application; changing it on an
existing database also requires a `VACUUM` and should be planned separately.
