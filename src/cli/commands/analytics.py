from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _config, db = await runtime.init_db(args.config)
        try:
            action = getattr(args, "analytics_action", None) or "top"
            date_from = getattr(args, "date_from", None)
            date_to = getattr(args, "date_to", None)

            if action == "top":
                limit = getattr(args, "limit", 20)
                limit = max(1, min(limit, 100))
                rows = await db.get_top_messages(limit=limit, date_from=date_from, date_to=date_to)
                if not rows:
                    print("No messages with reactions found.")
                    return
                print(f"{'#':<4} {'Reactions':<10} {'Date':<18} {'Channel':<30} {'Text'}")
                print("-" * 90)
                for i, row in enumerate(rows, 1):
                    channel = (
                        row.get("channel_title")
                        or row.get("channel_username")
                        or str(row.get("channel_id", ""))
                    )
                    text = (row.get("text") or "")[:60].replace("\n", " ")
                    date = str(row.get("date", ""))[:16]
                    print(f"{i:<4} {row['total_reactions']:<10} {date:<18} {channel:<30} {text}")

            elif action == "content-types":
                rows = await db.get_engagement_by_media_type(date_from=date_from, date_to=date_to)
                if not rows:
                    print("No data.")
                    return
                print(f"{'Content type':<20} {'Messages':<12} {'Avg reactions'}")
                print("-" * 50)
                for row in rows:
                    print(
                        f"{row['content_type']:<20} {row['message_count']:<12}"
                        f" {row['avg_reactions']:.1f}"
                    )

            elif action == "hourly":
                rows = await db.get_hourly_activity(date_from=date_from, date_to=date_to)
                if not rows:
                    print("No data.")
                    return
                print(f"{'Hour (UTC)':<14} {'Messages':<12} {'Avg reactions'}")
                print("-" * 40)
                for row in rows:
                    print(
                        f"{row['hour']:02d}:00        {row['message_count']:<12}"
                        f" {row['avg_reactions']:.1f}"
                    )

            elif action == "summary":
                from src.services.content_analytics_service import ContentAnalyticsService

                svc = ContentAnalyticsService(db)
                s = await svc.get_summary()
                print("Content generation summary:")
                print(f"  Total generations: {s.get('total_generations', 0)}")
                print(f"  Published:         {s.get('total_published', 0)}")
                print(f"  Pending:           {s.get('total_pending', 0)}")
                print(f"  Rejected:          {s.get('total_rejected', 0)}")
                print(f"  Pipelines:         {s.get('pipelines_count', 0)}")

            elif action == "pipeline-stats":
                from src.services.content_analytics_service import ContentAnalyticsService

                svc = ContentAnalyticsService(db)
                pipeline_id = getattr(args, "pipeline_id", None)
                stats = await svc.get_pipeline_stats(pipeline_id=pipeline_id)
                if not stats:
                    print("No pipeline stats found.")
                    return
                fmt = "{:<30} {:<8} {:<8} {:<8} {:<8} {:<8}"
                print(fmt.format("Pipeline", "Total", "Publ.", "Reject", "Pending", "Rate"))
                print("-" * 78)
                for s in stats:
                    print(fmt.format(
                        s.pipeline_name[:30],
                        str(s.total_generations),
                        str(s.total_published),
                        str(s.total_rejected),
                        str(s.pending_moderation),
                        f"{s.success_rate:.0%}",
                    ))

            elif action == "daily":
                from src.services.content_analytics_service import ContentAnalyticsService

                svc = ContentAnalyticsService(db)
                days = getattr(args, "days", 30)
                pipeline_id = getattr(args, "pipeline_id", None)
                rows = await svc.get_daily_stats(days=days, pipeline_id=pipeline_id)
                if not rows:
                    print("No data.")
                    return
                fmt = "{:<14} {:<12} {:<12}"
                print(fmt.format("Date", "Generated", "Published"))
                print("-" * 38)
                for row in rows:
                    if isinstance(row, dict):
                        date = row.get("date", "")
                        generated = row.get("generations", row.get("count", 0))
                        published = row.get("publications", row.get("published", 0))
                    else:
                        date = getattr(row, "date", "")
                        generated = getattr(row, "generations", 0)
                        published = getattr(row, "publications", 0)
                    print(fmt.format(str(date), str(generated), str(published)))

            elif action == "trending-topics":
                from src.services.trend_service import TrendService

                svc = TrendService(db)
                days = getattr(args, "days", 7)
                limit = getattr(args, "limit", 20)
                topics = await svc.get_trending_topics(days=days, limit=limit)
                if not topics:
                    print("No trending topics found.")
                    return
                fmt = "{:<4} {:<40} {:<10}"
                print(fmt.format("#", "Keyword", "Count"))
                print("-" * 54)
                for i, t in enumerate(topics, 1):
                    print(fmt.format(str(i), str(t.keyword)[:40], str(t.count)))

            elif action == "trending-channels":
                from src.services.trend_service import TrendService

                svc = TrendService(db)
                days = getattr(args, "days", 7)
                limit = getattr(args, "limit", 20)
                channels = await svc.get_trending_channels(days=days, limit=limit)
                if not channels:
                    print("No channel data found.")
                    return
                fmt = "{:<4} {:<40} {:<10}"
                print(fmt.format("#", "Channel", "Messages"))
                print("-" * 54)
                for i, ch in enumerate(channels, 1):
                    message_count = getattr(ch, "message_count", getattr(ch, "count", 0))
                    print(fmt.format(str(i), str(ch.title)[:40], str(message_count)))

            elif action == "velocity":
                from src.services.trend_service import TrendService

                svc = TrendService(db)
                days = getattr(args, "days", 30)
                velocity = await svc.get_message_velocity(days=days)
                if not velocity:
                    print("No velocity data found.")
                    return
                fmt = "{:<14} {:<10}"
                print(fmt.format("Date", "Messages"))
                print("-" * 24)
                for v in velocity:
                    print(fmt.format(str(v.date), str(v.count)))

            elif action == "peak-hours":
                from src.services.trend_service import TrendService

                svc = TrendService(db)
                hours = await svc.get_peak_hours()
                if not hours:
                    print("No peak hours data found.")
                    return
                print(f"{'Hour (UTC)':<14} {'Messages'}")
                print("-" * 30)
                for h in hours:
                    bar = "█" * max(1, h.count // 10)
                    print(f"{h.hour:02d}:00         {h.count} {bar}")

            elif action == "calendar":
                from src.services.content_calendar_service import ContentCalendarService

                svc = ContentCalendarService(db)
                limit = getattr(args, "limit", 20)
                pipeline_id = getattr(args, "pipeline_id", None)
                events = await svc.get_upcoming(limit=limit, pipeline_id=pipeline_id)
                if not events:
                    print("No upcoming publications.")
                    return
                fmt = "{:<8} {:<20} {:<12} {:<20} {}"
                print(fmt.format("Run ID", "Pipeline", "Status", "Scheduled", "Preview"))
                print("-" * 90)
                for e in events:
                    scheduled = str(e.scheduled_time or e.created_at)[:19]
                    print(fmt.format(
                        str(e.run_id),
                        str(e.pipeline_name)[:20],
                        str(e.moderation_status),
                        scheduled,
                        str(e.preview)[:40],
                    ))
            elif action == "trending-emojis":
                import re
                from collections import Counter

                days = getattr(args, "days", 7)
                limit = getattr(args, "limit", 20)
                from datetime import datetime, timedelta, timezone

                since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
                messages, _ = await db.search_messages(date_from=since, limit=10000)
                emoji_pattern = re.compile(
                    r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
                    r"\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF"
                    r"\U00002702-\U000027B0\U0001F900-\U0001F9FF"
                    r"\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF"
                    r"\U00002600-\U000026FF\U0000FE0F]"
                )
                counter: Counter[str] = Counter()
                for msg in messages:
                    text = msg.text or ""
                    for match in emoji_pattern.finditer(text):
                        counter[match.group()] += 1
                if not counter:
                    print("No emojis found in recent messages.")
                else:
                    print(f"Top {limit} emojis (last {days} days):\n")
                    for emoji, count in counter.most_common(limit):
                        print(f"  {emoji}  {count}")

            elif action == "channel":
                from src.services.channel_analytics_service import ChannelAnalyticsService

                channel_id = args.channel_id
                days = getattr(args, "days", 30)
                svc = ChannelAnalyticsService(db)
                ov = await svc.get_channel_overview(channel_id, days=days)
                if ov.title is None:
                    print(f"Channel {channel_id} not found.")
                    return
                print(f"Channel: {ov.title or ov.username or channel_id}")
                print(f"Username: {ov.username or '-'}")
                print(f"Subscribers: {ov.subscriber_count if ov.subscriber_count is not None else '-'}")
                if ov.subscriber_delta_week is not None:
                    print(f"  Delta week: {'+' if ov.subscriber_delta_week >= 0 else ''}{ov.subscriber_delta_week}")
                if ov.subscriber_delta_month is not None:
                    print(f"  Delta month: {'+' if ov.subscriber_delta_month >= 0 else ''}{ov.subscriber_delta_month}")
                print(f"ERR: {ov.err:.2f}%" if ov.err is not None else "ERR: -")
                print(f"ERR24: {ov.err24:.2f}%" if ov.err24 is not None else "ERR24: -")
                print(f"Posts total: {ov.total_posts}")
                print(f"  today / week / period({days}d): {ov.posts_today} / {ov.posts_week} / {ov.posts_month}")
                print(f"Avg views: {ov.avg_views if ov.avg_views is not None else '-'}")
                print(f"Avg forwards: {ov.avg_forwards if ov.avg_forwards is not None else '-'}")
                print(f"Avg reactions: {ov.avg_reactions if ov.avg_reactions is not None else '-'}")

                # Citation stats
                cit = await svc.get_citation_stats(channel_id)
                print("\nCitations (forwards):")
                print(f"  Total: {cit.total_forwards}  Posts: {cit.post_count}  Avg/post: {cit.avg_forwards}")

                # Cross-channel citations
                cross = await svc.get_cross_channel_citations(channel_id, days=days, limit=10)
                if cross:
                    print(f"\nCross-channel citations (last {days}d):")
                    fmt = "  {:<30} {:<10} {}"
                    print(fmt.format("Source", "Citations", "Last date"))
                    for row in cross:
                        name = row["source_title"] or row["source_username"] or str(row["source_channel_id"])
                        print(fmt.format(name[:30], str(row["citation_count"]), (row["latest_date"] or "")[:10]))
                else:
                    print(f"\nNo cross-channel citations in last {days}d.")

                # Heatmap (compact text representation)
                heatmap = await svc.get_heatmap(channel_id, days=days)
                if heatmap:
                    print(f"\nHeatmap (hour x weekday, last {days}d):")
                    # SQLite %w: 0=Sun; reorder to Mon-first
                    wd_order = [1, 2, 3, 4, 5, 6, 0]
                    wd_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                    heat_map = {}
                    max_cnt = 1
                    for r in heatmap:
                        heat_map[(r["weekday"], r["hour"])] = r["count"]
                        if r["count"] > max_cnt:
                            max_cnt = r["count"]
                    header = "       " + " ".join(f"{h:02d}" for h in range(24))
                    print(header)
                    for idx, wd in enumerate(wd_order):
                        cells = []
                        for h in range(24):
                            cnt = heat_map.get((wd, h), 0)
                            pct = cnt / max_cnt if max_cnt else 0
                            if cnt == 0:
                                cells.append("  .")
                            elif pct < 0.33:
                                cells.append("  -")
                            elif pct < 0.66:
                                cells.append("  +")
                            else:
                                cells.append("  *")
                        print(f"  {wd_labels[idx]:>3}  " + " ".join(cells))
                else:
                    print(f"\nNo heatmap data for last {days}d.")
        finally:
            await db.close()

    asyncio.run(_run())
