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
                    print(fmt.format(str(row["date"]), str(row.get("count", 0)), str(row.get("published", 0))))

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
                    print(fmt.format(str(i), str(ch.title)[:40], str(ch.count)))

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
        finally:
            await db.close()

    asyncio.run(_run())
