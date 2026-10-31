"""Web-panel test helpers: build the FastAPI app over the real-pool harness (#1720 wave 3).

Moved out of the core ``tests/helpers.py`` so the core test helpers never
import the optional ``tg_webui`` package.
"""

from __future__ import annotations

import base64
from contextlib import asynccontextmanager

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tests.helpers import RealPoolHarness
from tg_agent.collection_queue import CollectionQueue
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account
from tg_agent.scheduler.service import SchedulerManager
from tg_agent.search.ai_search import AISearchEngine
from tg_agent.search.engine import SearchEngine
from tg_agent.telegram.collector import Collector
from tg_webui.app import create_app


async def build_web_app(
    config: AppConfig,
    harness: RealPoolHarness,
    *,
    db: Database | None = None,
    add_account: str | None = None,
    session_secret: str = "test_secret_key",
) -> tuple[FastAPI, Database]:
    app = create_app(config)
    if db is None:
        db = Database(
            config.database.path,
            session_encryption_secret=config.security.session_encryption_key,
        )
        await db.initialize()
    app.state.db = db
    app.state.auth = harness.auth
    app.state.pool = harness.pool
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = session_secret
    if add_account:
        await db.add_account(Account(phone=add_account, session_string="test_session"))
    return app, db


@asynccontextmanager
async def make_auth_client(app, *, password: str = "testpass", with_auth: bool = True):
    transport = ASGITransport(app=app)
    headers = {"Origin": "http://test"}
    if with_auth:
        auth_header = base64.b64encode(f":{password}".encode()).decode()
        headers["Authorization"] = f"Basic {auth_header}"
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers=headers,
    ) as c:
        yield c
