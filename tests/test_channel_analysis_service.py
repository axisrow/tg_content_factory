"""Tests for channel rating foundation (#966): seed, repo, and service."""

from __future__ import annotations

from src.models import ChannelRating
from src.services.channel_analysis_service import ChannelAnalysisService

# --- pure verdict parsing / scoring (no DB) -------------------------------


def test_parse_verdict_valid_json():
    raw = '{"useful": "useful", "genre": "original", "confidence": 0.82, "reason": "хорошо"}'
    v = ChannelAnalysisService._parse_verdict(raw)
    assert v == {"useful": "useful", "genre": "original", "confidence": 0.82, "reason": "хорошо"}


def test_parse_verdict_text_fallback():
    # Not valid JSON, but contains the enum tokens.
    raw = "вердикт: useless канал, жанр infobiz, спам"
    v = ChannelAnalysisService._parse_verdict(raw)
    assert v["useful"] == "useless"
    assert v["genre"] == "infobiz"


def test_parse_verdict_defaults_and_clamps():
    v = ChannelAnalysisService._parse_verdict("garbage")
    assert v["useful"] == "useless"
    assert v["genre"] == "original"
    assert v["confidence"] == 0.0
    over = ChannelAnalysisService._parse_verdict('{"useful":"useful","genre":"ad","confidence":5}')
    assert over["confidence"] == 1.0


def test_emoji_trash_score():
    plain = ChannelAnalysisService._emoji_trash_score("Plain title", ["a" * 200])
    assert plain == 0.0
    spammy = ChannelAnalysisService._emoji_trash_score("🧠🔥🚀 Title", ["🔥" * 50 + "x" * 100])
    assert spammy > 0.0


# --- repo + seed over :memory: DB -----------------------------------------


async def test_csv_seed_loaded(db):
    count = await db.repos.channel_ratings.count()
    assert count == 447  # the committed seed CSV (#966)


async def test_repo_upsert_get_list(db):
    repo = db.repos.channel_ratings
    await repo.upsert(
        ChannelRating(channel_id=999001, useful="useful", genre="original", confidence=0.9)
    )
    got = await repo.get(999001)
    assert got is not None and got.useful == "useful" and got.confidence == 0.9

    # upsert again updates in place (no duplicate row).
    await repo.upsert(
        ChannelRating(channel_id=999001, useful="useless", genre="ad", confidence=0.5)
    )
    again = await repo.get(999001)
    assert again.useful == "useless" and again.genre == "ad"

    only_ads = await repo.list_ratings(genre="ad", limit=10)
    assert only_ads and all(r.genre == "ad" for r in only_ads)
    only_useless = await repo.list_ratings(useful="useless", genre="ad", limit=1000)
    assert any(r.channel_id == 999001 for r in only_useless)


async def test_classify_channel_with_fake_provider(db):
    async def fake_provider(*, prompt, max_tokens=256, temperature=0.0, **kw):
        assert "Посты канала" in prompt
        return '{"useful": "useful", "genre": "aggregator", "confidence": 0.7, "reason": "ok"}'

    svc = ChannelAnalysisService(db)
    rating = await svc.classify_channel(999002, provider_callable=fake_provider, sample_size=5)
    assert rating.useful == "useful"
    assert rating.genre == "aggregator"
    assert rating.confidence == 0.7

    stored = await svc.get_rating(999002)
    assert stored is not None and stored.genre == "aggregator"


async def test_upsert_preserves_flag_count_on_reclassify(db):
    repo = db.repos.channel_ratings
    await repo.upsert(ChannelRating(channel_id=900100, useful="useful", genre="original", flag_count=3))
    # Re-classification builds the row with flag_count=0 (default) — must NOT reset.
    await repo.upsert(ChannelRating(channel_id=900100, useful="useless", genre="ad", flag_count=0))
    got = await repo.get(900100)
    assert got.flag_count == 3
    assert got.genre == "ad"  # other fields still update


def test_genre_fallback_word_boundary():
    # "ad" must not match inside "broadcast"; no genre token → default original.
    v = ChannelAnalysisService._parse_verdict("broadcast news digest, no json here")
    assert v["genre"] == "original"
    # whole-word genre token is matched
    v2 = ChannelAnalysisService._parse_verdict("это явная реклама, жанр ad точно")
    assert v2["genre"] == "ad"
