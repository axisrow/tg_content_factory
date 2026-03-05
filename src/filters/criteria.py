from __future__ import annotations

import aiosqlite


async def check_low_uniqueness(
    db: aiosqlite.Connection, channel_id: int, threshold: float = 30.0
) -> tuple[float | None, bool]:
    """Ratio of unique text prefixes to total messages.

    Returns (uniqueness_pct, is_flagged).
    """
    cur = await db.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT substr(text, 1, 100)) AS uniq
        FROM messages
        WHERE channel_id = ? AND text IS NOT NULL AND text != ''
        """,
        (channel_id,),
    )
    row = await cur.fetchone()
    total, uniq = row["total"], row["uniq"]
    if total == 0:
        return None, False
    pct = uniq / total * 100
    return round(pct, 1), pct < threshold


async def check_subscriber_ratio(
    db: aiosqlite.Connection, channel_id: int, threshold: float = 1.0
) -> tuple[float | None, bool]:
    """Subscriber count / message count ratio.

    Returns (ratio, is_flagged).
    """
    cur = await db.execute(
        "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id = ?",
        (channel_id,),
    )
    msg_count = (await cur.fetchone())["cnt"]
    if msg_count == 0:
        return None, False

    cur = await db.execute(
        """
        SELECT subscriber_count
        FROM channel_stats
        WHERE channel_id = ?
        ORDER BY collected_at DESC
        LIMIT 1
        """,
        (channel_id,),
    )
    row = await cur.fetchone()
    if not row or row["subscriber_count"] is None:
        return None, False

    ratio = row["subscriber_count"] / msg_count
    return round(ratio, 2), ratio < threshold


async def check_cross_channel_dupes(
    db: aiosqlite.Connection, channel_id: int, threshold: float = 50.0
) -> tuple[float | None, bool]:
    """Percentage of messages whose text prefix appears in other channels.

    Returns (cross_dupe_pct, is_flagged).
    """
    cur = await db.execute(
        """
        SELECT COUNT(*) AS total
        FROM messages
        WHERE channel_id = ? AND text IS NOT NULL AND length(text) > 10
        """,
        (channel_id,),
    )
    total = (await cur.fetchone())["total"]
    if total == 0:
        return None, False

    cur = await db.execute(
        """
        SELECT COUNT(*) AS duped
        FROM (
            SELECT DISTINCT substr(m1.text, 1, 100) AS prefix
            FROM messages m1
            WHERE m1.channel_id = ? AND m1.text IS NOT NULL AND length(m1.text) > 10
        ) t
        WHERE EXISTS (
            SELECT 1 FROM messages m2
            WHERE m2.channel_id != ?
              AND m2.text IS NOT NULL
              AND substr(m2.text, 1, 100) = t.prefix
        )
        """,
        (channel_id, channel_id),
    )
    duped = (await cur.fetchone())["duped"]

    cur = await db.execute(
        """
        SELECT COUNT(DISTINCT substr(text, 1, 100)) AS uniq_total
        FROM messages
        WHERE channel_id = ? AND text IS NOT NULL AND length(text) > 10
        """,
        (channel_id,),
    )
    uniq_total = (await cur.fetchone())["uniq_total"]
    if uniq_total == 0:
        return None, False

    pct = duped / uniq_total * 100
    return round(pct, 1), pct > threshold


async def check_non_cyrillic(
    db: aiosqlite.Connection, channel_id: int, threshold: float = 10.0
) -> tuple[float | None, bool]:
    """Percentage of messages containing at least one Cyrillic character.

    Returns (cyrillic_pct, is_flagged).
    """
    cur = await db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN text GLOB '*[а-яА-ЯёЁ]*' THEN 1 ELSE 0 END) AS cyr
        FROM messages
        WHERE channel_id = ? AND text IS NOT NULL AND text != ''
        """,
        (channel_id,),
    )
    row = await cur.fetchone()
    total, cyr = row["total"], row["cyr"] or 0
    if total == 0:
        return None, False
    pct = cyr / total * 100
    return round(pct, 1), pct < threshold


async def check_chat_noise(
    db: aiosqlite.Connection,
    channel_id: int,
    threshold: float = 70.0,
) -> tuple[float | None, bool]:
    """Percentage of short messages (<=10 chars) from groups.

    Returns (short_msg_pct, is_flagged).
    """
    # Only flag groups, not channels
    cur = await db.execute(
        "SELECT channel_type FROM channels WHERE channel_id = ?",
        (channel_id,),
    )
    ch_row = await cur.fetchone()
    if not ch_row or ch_row["channel_type"] != "group":
        return None, False

    cur = await db.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN length(COALESCE(text, '')) <= 10 THEN 1 ELSE 0 END) AS short
        FROM messages
        WHERE channel_id = ?
        """,
        (channel_id,),
    )
    row = await cur.fetchone()
    total, short = row["total"], row["short"] or 0
    if total == 0:
        return None, False
    pct = short / total * 100
    return round(pct, 1), pct > threshold
