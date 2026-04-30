from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    analytics_parser = subparsers.add_parser("analytics", help="Message analytics")
    analytics_sub = analytics_parser.add_subparsers(dest="analytics_action")

    analytics_top = analytics_sub.add_parser("top", help="Top messages by reactions")
    analytics_top.add_argument(
        "--limit", type=int, default=20, help="Number of results (default: 20)"
    )
    analytics_top.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_top.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_ct = analytics_sub.add_parser("content-types", help="Engagement by content type")
    analytics_ct.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_ct.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_hourly = analytics_sub.add_parser("hourly", help="Hourly activity patterns")
    analytics_hourly.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_hourly.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_sub.add_parser("summary", help="Content generation summary")

    analytics_daily = analytics_sub.add_parser("daily", help="Daily generation stats")
    analytics_daily.add_argument("--days", type=int, default=30, help="Number of days (default: 30)")
    analytics_daily.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_pipeline = analytics_sub.add_parser("pipeline-stats", help="Per-pipeline statistics")
    analytics_pipeline.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_trending = analytics_sub.add_parser("trending-topics", help="Trending topics/keywords")
    analytics_trending.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_trending.add_argument("--limit", type=int, default=20)

    analytics_channels = analytics_sub.add_parser("trending-channels", help="Top channels by activity")
    analytics_channels.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_channels.add_argument("--limit", type=int, default=20)

    analytics_velocity = analytics_sub.add_parser("velocity", help="Message volume per day")
    analytics_velocity.add_argument("--days", type=int, default=30, help="Number of days (default: 30)")

    analytics_sub.add_parser("peak-hours", help="Peak activity hours")

    analytics_calendar = analytics_sub.add_parser("calendar", help="Upcoming scheduled publications")
    analytics_calendar.add_argument("--limit", type=int, default=20)
    analytics_calendar.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_emojis = analytics_sub.add_parser("trending-emojis", help="Trending emojis in messages")
    analytics_emojis.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_emojis.add_argument("--limit", type=int, default=20)

    analytics_channel = analytics_sub.add_parser("channel", help="Per-channel statistics overview")
    analytics_channel.add_argument("channel_id", type=int, help="Telegram channel_id (negative int)")
    analytics_channel.add_argument("--days", type=int, default=30, help="Time window in days (default: 30)")
