import pytest

# All lazy telethon imports found inside function/method bodies across src/.
# If any import path becomes invalid, the test fails immediately with ImportError.
LAZY_IMPORTS = [
    # src/telegram/client_pool.py:419 — get_forum_topics()
    ("telethon.tl.functions.messages", "GetForumTopicsRequest"),
    # src/search/telegram_search.py:45 — _check_search_quota_with_client()
    ("telethon.tl.functions.channels", "CheckSearchPostsFloodRequest"),
    # src/search/telegram_search.py:123-125 — _search_posts_global()
    ("telethon.tl.functions.channels", "SearchPostsRequest"),
    ("telethon.tl.types", "InputPeerEmpty"),
    ("telethon.tl.types", "PeerChannel"),
    ("telethon.utils", "get_input_peer"),
    # src/search/transformers.py:11-25 — media_type_from_message()
    ("telethon.tl.types", "DocumentAttributeAnimated"),
    ("telethon.tl.types", "DocumentAttributeAudio"),
    ("telethon.tl.types", "DocumentAttributeSticker"),
    ("telethon.tl.types", "DocumentAttributeVideo"),
    ("telethon.tl.types", "MessageMediaContact"),
    ("telethon.tl.types", "MessageMediaDice"),
    ("telethon.tl.types", "MessageMediaDocument"),
    ("telethon.tl.types", "MessageMediaGame"),
    ("telethon.tl.types", "MessageMediaGeo"),
    ("telethon.tl.types", "MessageMediaGeoLive"),
    ("telethon.tl.types", "MessageMediaPhoto"),
    ("telethon.tl.types", "MessageMediaPoll"),
    ("telethon.tl.types", "MessageMediaWebPage"),
    # src/search/transformers.py:98 — resolve_sender()
    ("telethon.tl.types", "PeerUser"),
    # top-level imports that are critical (from CLAUDE.md patterns)
    ("telethon.tl.functions.channels", "GetFullChannelRequest"),
    ("telethon.tl.types", "ChannelForbidden"),
    ("telethon.tl.functions.auth", "ResendCodeRequest"),
    ("telethon.tl.types.auth", "SentCode"),
]


@pytest.mark.parametrize("module,name", LAZY_IMPORTS)
def test_telethon_import(module, name):
    """Smoke-test: ensure all lazy telethon imports resolve."""
    mod = __import__(module, fromlist=[name])
    assert hasattr(mod, name), f"{module}.{name} not found"
