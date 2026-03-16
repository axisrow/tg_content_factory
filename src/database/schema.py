SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    phone TEXT UNIQUE NOT NULL,
    session_string TEXT NOT NULL,
    is_primary INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    flood_wait_until TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER UNIQUE NOT NULL,
    title TEXT,
    username TEXT,
    is_active INTEGER DEFAULT 1,
    last_collected_id INTEGER DEFAULT 0,
    added_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sender_id INTEGER,
    sender_name TEXT,
    text TEXT,
    media_type TEXT,
    topic_id INTEGER,
    reactions_json TEXT,
    views INTEGER,
    forwards INTEGER,
    reply_count INTEGER,
    date TEXT NOT NULL,
    collected_at TEXT DEFAULT (datetime('now')),
    UNIQUE(channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_tasks (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER,
    channel_title TEXT,
    channel_username TEXT,
    task_type TEXT NOT NULL DEFAULT 'channel_collect',
    status TEXT DEFAULT 'pending',
    messages_collected INTEGER DEFAULT 0,
    error TEXT,
    note TEXT,
    run_after TEXT,
    payload TEXT,
    parent_task_id INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS search_log (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL,
    query TEXT NOT NULL,
    results_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_search_log_phone_date
    ON search_log(phone, created_at);
CREATE TABLE IF NOT EXISTS channel_stats (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    subscriber_count INTEGER,
    avg_views REAL,
    avg_reactions REAL,
    avg_forwards REAL,
    collected_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_stats_channel_date
    ON channel_stats(channel_id, collected_at);
CREATE INDEX IF NOT EXISTS idx_channel_stats_lookup
    ON channel_stats(channel_id, collected_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_text ON messages(text);
CREATE INDEX IF NOT EXISTS idx_messages_channel_date ON messages(channel_id, date);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    text,
    content=messages,
    content_rowid=id,
    tokenize="unicode61"
);

CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;

CREATE TABLE IF NOT EXISTS search_queries (
    id               INTEGER PRIMARY KEY,
    name             TEXT NOT NULL,
    query            TEXT NOT NULL,
    is_regex         INTEGER DEFAULT 0,
    is_fts           INTEGER DEFAULT 0,
    is_active        INTEGER DEFAULT 1,
    notify_on_collect INTEGER DEFAULT 0,
    track_stats      INTEGER DEFAULT 1,
    interval_minutes INTEGER NOT NULL DEFAULT 60,
    exclude_patterns TEXT DEFAULT '',
    max_length       INTEGER DEFAULT NULL,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS search_query_stats (
    id          INTEGER PRIMARY KEY,
    query_id    INTEGER NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (query_id) REFERENCES search_queries(id)
);

CREATE INDEX IF NOT EXISTS idx_sqs_query_date
    ON search_query_stats(query_id, recorded_at);

CREATE TABLE IF NOT EXISTS forum_topics (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(channel_id, topic_id)
);

CREATE TABLE IF NOT EXISTS photo_batches (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL,
    target_dialog_id INTEGER NOT NULL,
    target_title TEXT,
    target_type TEXT,
    send_mode TEXT NOT NULL DEFAULT 'album',
    caption TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    last_run_at TEXT
);

CREATE TABLE IF NOT EXISTS photo_batch_items (
    id INTEGER PRIMARY KEY,
    batch_id INTEGER,
    phone TEXT NOT NULL,
    target_dialog_id INTEGER NOT NULL,
    target_title TEXT,
    target_type TEXT,
    file_paths TEXT NOT NULL,
    send_mode TEXT NOT NULL DEFAULT 'album',
    caption TEXT,
    schedule_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    telegram_message_ids TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT,
    FOREIGN KEY (batch_id) REFERENCES photo_batches(id)
);

CREATE INDEX IF NOT EXISTS idx_photo_batch_items_status_schedule
    ON photo_batch_items(status, schedule_at);
CREATE INDEX IF NOT EXISTS idx_photo_batch_items_batch_id
    ON photo_batch_items(batch_id);

CREATE TABLE IF NOT EXISTS photo_auto_upload_jobs (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL,
    target_dialog_id INTEGER NOT NULL,
    target_title TEXT,
    target_type TEXT,
    folder_path TEXT NOT NULL,
    send_mode TEXT NOT NULL DEFAULT 'album',
    caption TEXT,
    interval_minutes INTEGER NOT NULL DEFAULT 60,
    is_active INTEGER NOT NULL DEFAULT 1,
    error TEXT,
    last_run_at TEXT,
    last_seen_marker TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS photo_auto_upload_files (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    sent_at TEXT DEFAULT (datetime('now')),
    UNIQUE(job_id, file_path),
    FOREIGN KEY (job_id) REFERENCES photo_auto_upload_jobs(id)
);

CREATE TABLE IF NOT EXISTS dialog_cache (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL,
    dialog_id INTEGER NOT NULL,
    title TEXT,
    username TEXT,
    channel_type TEXT NOT NULL,
    deactivate INTEGER NOT NULL DEFAULT 0,
    is_own INTEGER NOT NULL DEFAULT 0,
    cached_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(phone, dialog_id)
);

CREATE INDEX IF NOT EXISTS idx_dialog_cache_phone
    ON dialog_cache(phone);

CREATE TABLE IF NOT EXISTS message_reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    emoji TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (channel_id, message_id)
        REFERENCES messages(channel_id, message_id) ON DELETE CASCADE,
    UNIQUE(channel_id, message_id, emoji)
);

CREATE INDEX IF NOT EXISTS idx_message_reactions_channel_msg
    ON message_reactions(channel_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_reactions_emoji
    ON message_reactions(emoji);
CREATE INDEX IF NOT EXISTS idx_messages_collected_at ON messages(collected_at);
"""
