SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    phone TEXT UNIQUE NOT NULL,
    session_string TEXT NOT NULL,
    is_primary INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    flood_wait_until TEXT,
    is_premium INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

-- At most one primary account (#733): partial unique index on is_primary = 1.
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_single_primary
    ON accounts(is_primary) WHERE is_primary = 1;

CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER UNIQUE NOT NULL,
    title TEXT,
    username TEXT,
    is_active INTEGER DEFAULT 1,
    last_collected_id INTEGER DEFAULT 0,
    added_at TEXT DEFAULT (datetime('now')),
    channel_type TEXT,
    is_filtered INTEGER DEFAULT 0,
    filter_flags TEXT DEFAULT '',
    about TEXT,
    linked_chat_id INTEGER,
    has_comments INTEGER DEFAULT 0,
    created_at TEXT,
    preferred_phone TEXT,
    needs_review INTEGER DEFAULT 0,
    review_reason TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sender_id INTEGER,
    sender_name TEXT,
    sender_first_name TEXT,
    sender_last_name TEXT,
    sender_username TEXT,
    text TEXT,
    message_kind TEXT,
    media_type TEXT,
    service_action_raw TEXT,
    service_action_semantic TEXT,
    service_action_payload_json TEXT,
    sender_kind TEXT,
    topic_id INTEGER,
    reactions_json TEXT,
    views INTEGER,
    forwards INTEGER,
    reply_count INTEGER,
    date TEXT NOT NULL,
    collected_at TEXT DEFAULT (datetime('now')),
    forward_from_channel_id INTEGER,
    detected_lang TEXT,
    translation_en TEXT,
    translation_custom TEXT,
    premium_search_query TEXT,
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
    completed_at TEXT,
    last_progress_at TEXT
);

CREATE TABLE IF NOT EXISTS telegram_commands (
    id INTEGER PRIMARY KEY,
    command_type TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    requested_by TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT,
    run_after TEXT,
    finished_at TEXT,
    error TEXT,
    result_payload TEXT
);

CREATE INDEX IF NOT EXISTS idx_telegram_commands_status_id
    ON telegram_commands(status, id);
CREATE INDEX IF NOT EXISTS idx_telegram_commands_status_run_after_id
    ON telegram_commands(status, run_after, id);

CREATE TABLE IF NOT EXISTS runtime_snapshots (
    snapshot_type TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    payload TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (snapshot_type, scope)
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
-- NOTE: there is intentionally no index on messages(text). A B-tree index on the
-- full message text cannot serve LIKE '%..%' (leading wildcard => full scan
-- regardless), and full-text search goes through messages_fts, not this column.
-- The old idx_messages_text cost ~21 GB on a 48 GB production DB and slowed every
-- insert while saving ~1.5% on the rarely-used LIKE fallback; it is dropped by a
-- migration (see _drop_obsolete_indexes in migrations.py). See issue #760.
CREATE INDEX IF NOT EXISTS idx_messages_channel_date ON messages(channel_id, date);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
CREATE INDEX IF NOT EXISTS idx_messages_premium_search_query
    ON messages(premium_search_query) WHERE premium_search_query IS NOT NULL;

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
    chat_filter      TEXT DEFAULT '',
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

CREATE TABLE IF NOT EXISTS notification_bots (
    id INTEGER PRIMARY KEY,
    tg_user_id INTEGER NOT NULL UNIQUE,
    tg_username TEXT,
    bot_id INTEGER,
    bot_username TEXT NOT NULL,
    bot_token TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Dedup ledger for sent notifications: decouples delivery from the collection
-- cursor so a transient send failure is retried (and never double-notified) on a
-- later pass instead of silently losing the matched lead (audit #838/1).
CREATE TABLE IF NOT EXISTS notified_messages (
    query_id    INTEGER NOT NULL,
    channel_id  INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    notified_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (query_id, channel_id, message_id)
);

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


CREATE TABLE IF NOT EXISTS content_pipelines (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    prompt_template TEXT NOT NULL,
    llm_model TEXT,
    image_model TEXT,
    publish_mode TEXT NOT NULL DEFAULT 'moderated',
    generation_backend TEXT NOT NULL DEFAULT 'chain',
    is_active INTEGER NOT NULL DEFAULT 1,
    last_generated_id INTEGER NOT NULL DEFAULT 0,
    generate_interval_minutes INTEGER NOT NULL DEFAULT 60,
    publish_times TEXT,
    refinement_steps TEXT,
    pipeline_json TEXT,
    account_phone TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_content_pipelines_active
    ON content_pipelines(is_active, id);

CREATE TABLE IF NOT EXISTS generation_runs (
    id INTEGER PRIMARY KEY,
    pipeline_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    prompt TEXT,
    generated_text TEXT,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT,
    image_url TEXT,
    moderation_status TEXT DEFAULT 'pending',
    published_at TEXT,
    quality_score REAL,
    quality_issues TEXT,
    variants TEXT,
    selected_variant INTEGER
);

CREATE TABLE IF NOT EXISTS pipeline_sources (
    id INTEGER PRIMARY KEY,
    pipeline_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(pipeline_id, channel_id),
    FOREIGN KEY (pipeline_id) REFERENCES content_pipelines(id) ON DELETE CASCADE,
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_sources_pipeline
    ON pipeline_sources(pipeline_id);

CREATE TABLE IF NOT EXISTS pipeline_targets (
    id INTEGER PRIMARY KEY,
    pipeline_id INTEGER NOT NULL,
    phone TEXT NOT NULL,
    target_dialog_id INTEGER NOT NULL,
    target_title TEXT,
    target_type TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(pipeline_id, phone, target_dialog_id),
    FOREIGN KEY (pipeline_id) REFERENCES content_pipelines(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_targets_pipeline
    ON pipeline_targets(pipeline_id);

CREATE TABLE IF NOT EXISTS message_reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    emoji TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    -- Denormalized copy of the parent message's `date`. Reaction analytics filter
    -- by recency; without a date here the period filter lives on `messages` and
    -- forces a full JOIN of all reactions against messages (trending-emojis took
    -- ~3m43s on a 6.8M-row table). With it the query filters reactions directly via
    -- idx_message_reactions_date_emoji and drops the JOIN. See issue #760.
    -- Safe to denormalize: messages.date is immutable (never UPDATEd). So far only
    -- get_trending_emojis uses it; other reaction queries (get_top_messages,
    -- engagement) still JOIN messages but measured fast, so were left unchanged —
    -- they can switch to mr.date if they ever become a bottleneck.
    date TEXT,
    FOREIGN KEY (channel_id, message_id)
        REFERENCES messages(channel_id, message_id) ON DELETE CASCADE,
    UNIQUE(channel_id, message_id, emoji)
);

CREATE INDEX IF NOT EXISTS idx_message_reactions_channel_msg
    ON message_reactions(channel_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_reactions_emoji
    ON message_reactions(emoji);
-- NOTE: idx_message_reactions_date_emoji is created by _backfill_message_reactions_date
-- in migrations.py, AFTER the one-off date backfill — not here. Building it up front
-- would force the 6.8M-row backfill UPDATE to maintain the index per row (write
-- amplification). On a fresh DB the migration also creates it (over an empty/just-filled
-- table, so it's cheap). See issue #760 / PR #945 review.
CREATE INDEX IF NOT EXISTS idx_messages_collected_at ON messages(collected_at);
-- Covering index for analytics GROUP BY media_type with the is_filtered
-- channel join: without it the count query full-scans the messages table (#826).
CREATE INDEX IF NOT EXISTS idx_messages_media_type ON messages(media_type, channel_id);

-- NOTE: the legacy denormalized `pipelines` table (schema v1, with JSON
-- `source_channel_ids`/`targets` columns) was superseded by the normalized
-- `content_pipelines` + `pipeline_sources` + `pipeline_targets` tables above and
-- is no longer created here. `run_migrations()` drops the empty leftover table
-- from older databases (see src/database/migrations.py).

CREATE TABLE IF NOT EXISTS message_embeddings (
    message_id INTEGER PRIMARY KEY,
    embedding  BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS message_embeddings_json (
    message_id INTEGER PRIMARY KEY,
    embedding  TEXT NOT NULL,
    dims       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS generated_images (
    id INTEGER PRIMARY KEY,
    prompt TEXT NOT NULL,
    model TEXT,
    image_url TEXT,
    local_path TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pipeline_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    template_json TEXT NOT NULL,
    is_builtin INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tags (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_tags (
    channel_pk INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (channel_pk, tag_id)
);

CREATE TABLE IF NOT EXISTS channel_rename_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  INTEGER NOT NULL,
    old_title   TEXT,
    new_title   TEXT,
    old_username TEXT,
    new_username TEXT,
    created_at  TEXT DEFAULT (datetime('now')),
    decided_at  TEXT,
    decision    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_rename_events_pending
    ON channel_rename_events(channel_id) WHERE decision IS NULL;

CREATE TABLE IF NOT EXISTS agent_threads (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    title      TEXT NOT NULL DEFAULT 'Новый тред',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id  INTEGER NOT NULL REFERENCES agent_threads(id) ON DELETE CASCADE,
    role       TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
    content    TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pipeline_action_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id INTEGER NOT NULL,
    node_id     TEXT NOT NULL,
    action      TEXT NOT NULL,
    channel_id  INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(pipeline_id, node_id, action, channel_id, message_id)
);
"""
