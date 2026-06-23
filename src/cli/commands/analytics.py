from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
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
                from src.services.trend_service import TrendService

                days = getattr(args, "days", 7)
                limit = getattr(args, "limit", 20)
                emojis = await TrendService(db).get_trending_emojis(days=days, limit=limit)
                if not emojis:
                    print("No emoji reactions found.")
                else:
                    print(f"Top {limit} emoji reactions (last {days} days):\n")
                    for item in emojis:
                        print(f"  {item.emoji}  {item.count}")

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

            elif action == "channel-rating":
                from src.services.channel_analysis_service import ChannelAnalysisService

                svc = ChannelAnalysisService(db)
                limit = max(1, min(getattr(args, "limit", 50), 1000))
                ratings = await svc.list_ratings(
                    useful=getattr(args, "useful", None),
                    genre=getattr(args, "genre", None),
                    limit=limit,
                )
                if not ratings:
                    print("No channel ratings found.")
                    return
                print(f"{'channel_id':<14} {'useful':<8} {'genre':<11} {'conf':<5} {'title'}")
                print("-" * 80)
                for r in ratings:
                    title = (r.title or r.username or str(r.channel_id))[:30]
                    print(f"{r.channel_id:<14} {r.useful:<8} {r.genre:<11} {r.confidence:<5.2f} {title}")

            elif action == "channel-rate":
                # Write path (#994): run the LLM judge for one channel and upsert
                # its verdict into channel_ratings. The read-only `channel-rating`
                # branch above only lists existing verdicts; this is the single
                # entry point that invokes ChannelAnalysisService.classify_channel.
                from src.services.channel_analysis_service import ChannelAnalysisService
                from src.services.provider_service import RuntimeProviderRegistry

                channel_id = args.channel_id
                provider_service = RuntimeProviderRegistry(db, config)
                await provider_service.load_db_providers()
                if not provider_service.has_providers():
                    print(
                        "LLM provider is not configured. Add one with `provider add`, in the web "
                        "/settings page, or set an API key env var (e.g. OPENAI_API_KEY)."
                    )
                    return

                # A mistyped --model must NOT silently fall back to the stub
                # provider and persist a meaningless verdict — fail loudly instead
                # (cycle-review #994, both reviewers flagged this as the top risk).
                try:
                    provider_callable = provider_service.resolve_provider_callable(args.model)
                except ValueError as exc:
                    print(str(exc))
                    return

                svc = ChannelAnalysisService(db)
                # Guard against an empty channel before spending a provider call:
                # no posts would otherwise persist a defaulted "useless/original"
                # verdict (n_total=0) that looks valid but carries zero signal.
                # Cap the sample upper bound so a huge --sample-size can't build a
                # prompt that trips token limits / OOM.
                sample_size = min(max(1, getattr(args, "sample_size", 40)), 200)
                posts = await svc.sample_posts(channel_id, sample_size)
                if not posts:
                    print(f"Channel {channel_id} has no text posts to judge; skipping.")
                    return

                try:
                    rating = await svc.classify_channel(
                        channel_id,
                        provider_callable=provider_callable,
                        sample_size=sample_size,
                    )
                except Exception as exc:  # provider/network/parse failure
                    print(f"Judge failed for channel {channel_id}: {exc}")
                    raise SystemExit(1) from exc

                name = rating.title or rating.username or str(rating.channel_id)
                print(f"Channel {rating.channel_id} ({name}):")
                print(f"  useful:     {rating.useful}")
                print(f"  genre:      {rating.genre}")
                print(f"  confidence: {rating.confidence:.2f}")
                print(f"  reason:     {rating.reason or '-'}")
                print(f"  posts seen: {rating.n_total}")
        finally:
            await db.close()

    asyncio.run(_run())
