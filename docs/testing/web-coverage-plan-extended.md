# Расширенный план: покрытие тестами веб-интерфейса

## 1. Анализ текущего состояния

### 1.1 Существующие тесты routes (8 файлов)

| Файл | Статус |
|------|--------|
| `tests/routes/__init__.py` | пустой |
| `tests/routes/test_auth_routes.py` | есть |
| `tests/routes/test_debug_routes.py` | есть |
| `tests/routes/test_import_channels_routes.py` | есть |
| `tests/routes/test_dialogs_routes.py` | есть |
| `tests/routes/test_scheduler_routes.py` | **эталон** (474 строки, 44 теста) |
| `tests/routes/test_pipelines_routes.py` | **эталон** (136 строк, 6 тестов) |
| `tests/routes/test_moderation_routes.py` | **эталон** (154 строки, 3 теста) |

### 1.2 Непокрытые маршруты (10 файлов)

| Файл | Эндпоинтов | Сложность |
|------|-----------|-----------|
| `src/web/routes/analytics.py` | 4 | Низкая |
| `src/web/routes/search.py` | 2 | Низкая |
| `src/web/routes/search_queries.py` | 6 | Средняя |
| `src/web/routes/channels.py` | 6 | Средняя |
| `src/web/routes/filter.py` | 9 | Средняя |
| `src/web/routes/settings.py` | 19 | Высокая |
| `src/web/routes/agent.py` | 10 | Высокая |
| `src/web/routes/photo_loader.py` | 10 | Высокая |
| `src/web/routes/channel_collection.py` | 4 | Средняя |
| `src/web/routes/moderation.py` | +5 | Средняя |

---

## 2. Архитектура тестирования

### 2.1 Структура фикстуры `client`

**Минимальный набор `app.state.*`:**

```python
app.state.db = db                    # Database (обязательно)
app.state.pool = pool_mock           # ClientPool (MagicMock)
app.state.config = config            # AppConfig
app.state.auth = TelegramAuth(...)   # TelegramAuth
app.state.collector = Collector(...) # Collector
app.state.search_engine = SearchEngine(db)
app.state.ai_search = AISearchEngine(config.llm, db)
app.state.scheduler = SchedulerManager(config.scheduler)
app.state.session_secret = "test_secret"
app.state.shutting_down = False
```

**Для CollectionQueue (scheduler/channels/search_queries):**

```python
from src.collection_queue import CollectionQueue
app.state.collection_queue = CollectionQueue(collector, db)
# В teardown:
await app.state.collection_queue.shutdown()
```

**Для AgentManager (agent/settings):**

```python
from src.agent.manager import AgentManager
agent_manager_mock = MagicMock(spec=AgentManager)
agent_manager_mock.available = True
agent_manager_mock.get_runtime_status = AsyncMock(return_value=runtime_status_mock)
agent_manager_mock.cancel_stream = AsyncMock(return_value=False)
agent_manager_mock.estimate_prompt_tokens = AsyncMock(return_value=100)
agent_manager_mock.chat_stream = AsyncMock()  # возвращает async generator
app.state.agent_manager = agent_manager_mock
```

### 2.2 Паттерны мокирования

**Способ 1: monkeypatch для сервисов**

```python
def test_search_with_query(client, monkeypatch):
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=SearchResult(messages=[], total=0, query="x"))
    monkeypatch.setattr("src.web.routes.search.deps.search_service", lambda r: mock_svc)
    resp = await client.get("/search?q=x")
```

**Способ 2: patch через context manager**

```python
with patch("src.web.routes.scheduler.deps.scheduler_service") as mock_svc:
    mock_svc.return_value.start = AsyncMock()
    await client.post("/scheduler/start")
    mock_svc.return_value.start.assert_called_once()
```

**Способ 3: подмена класса**

```python
class FakePublishService:
    def __init__(self, db, pool): pass
    async def publish_run(self, run, pipeline):
        return [PublishResult(success=True, message_id=777)]

monkeypatch.setattr("src.web.routes.moderation.PublishService", FakePublishService)
```

### 2.3 Вспомогательные функции

```python
async def _add_channel(db, channel_id=100, title="Test") -> int:
    """Добавить канал, вернуть PK."""
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    channels = await db.get_channels_with_counts()
    return next(c.id for c in channels if c.channel_id == channel_id)

async def _add_account(db, phone="+1234567890"):
    """Добавить аккаунт."""
    await db.add_account(Account(phone=phone, session_string="test_session"))

async def _create_pipeline_and_run(db) -> tuple[int, int]:
    """Создать pipeline и run, вернуть (pipeline_id, run_id)."""
    pipeline_id = await db.repos.content_pipelines.add(
        ContentPipeline(name="Test", prompt_template="tpl", publish_mode=PipelinePublishMode.MODERATED),
        source_channel_ids=[100],
        targets=[PipelineTarget(pipeline_id=0, phone="+1234567890", dialog_id=200, title="T", dialog_type="channel")],
    )
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "template")
    await db.repos.generation_runs.save_result(run_id, "Post text")
    return pipeline_id, run_id
```

---

## 3. Детальный план по файлам

### 3.1 `tests/routes/conftest.py` (новый)

**Назначение:** Общие фикстуры для routes-тестов.

```python
# Фикстуры:
@pytest.fixture
async def base_app(tmp_path):
    """Полностью сконфигурированный app + db."""
    # config, app, db, pool_mock, CollectionQueue, SchedulerManager
    # db.add_account(Account(phone="+1234567890"))
    # db.add_channel(Channel(channel_id=100, title="Test"))
    # yield (app, db)
    # teardown: await cq.shutdown(); await db.close()

@pytest.fixture
async def route_client(base_app):
    """AsyncClient с Basic auth."""
    app, db = base_app
    # yield AsyncClient(...)

@pytest.fixture
def pool_mock():
    """Мок ClientPool с базовыми методами."""
    m = MagicMock()
    m.clients = {"+1234567890": MagicMock()}
    m.get_dialogs_for_phone = AsyncMock(return_value=[])
    m.resolve_channel = AsyncMock(return_value={"channel_id": -100, "title": "X", "channel_type": "channel"})
    m.get_forum_topics = AsyncMock(return_value=[])
    return m

@pytest.fixture
def agent_manager_mock():
    """Мок AgentManager."""
    m = MagicMock(spec=AgentManager)
    m.available = True

    runtime = MagicMock()
    runtime.claude_available = False
    runtime.deepagents_available = False
    runtime.dev_mode_enabled = False
    runtime.backend_override = None
    runtime.selected_backend = "deepagents"
    runtime.fallback_model = None
    runtime.fallback_provider = None
    runtime.using_override = False
    runtime.error = None

    m.get_runtime_status = AsyncMock(return_value=runtime)
    m.cancel_stream = AsyncMock(return_value=False)
    m.estimate_prompt_tokens = AsyncMock(return_value=100)

    async def _fake_stream(*a, **kw):
        yield 'data: {"delta": "hi"}\n\n'
        yield 'data: {"done": true, "full_text": "hi"}\n\n'
    m.chat_stream = _fake_stream

    return m
```

---

### 3.2 `tests/routes/test_analytics_routes.py` (новый, 10 тестов)

**Маршруты:**
- `GET /analytics` — analytics_page
- `GET /analytics/content` — content_analytics_page
- `GET /analytics/content/api/summary` — api_content_summary
- `GET /analytics/content/api/pipelines` — api_pipeline_stats

**Специфика:** Не требует моков — `ContentAnalyticsService` работает с реальной DB.

| Тест | Метод | Параметры | Проверки |
|------|-------|-----------|----------|
| `test_analytics_page_renders` | GET | `/analytics` | 200 |
| `test_analytics_page_with_dates` | GET | `?date_from=2024-01-01&date_to=2024-12-31` | 200 |
| `test_analytics_page_limit_param` | GET | `?limit=20` | 200 |
| `test_analytics_page_invalid_limit` | GET | `?limit=abc` | 200 (не 500) |
| `test_analytics_page_empty_db` | GET | `/analytics` | 200 |
| `test_content_analytics_page_renders` | GET | `/analytics/content` | 200 |
| `test_api_content_summary_returns_json` | GET | `/analytics/content/api/summary` | 200, `is dict` |
| `test_api_pipelines_returns_json` | GET | `/analytics/content/api/pipelines` | 200, `is list` |
| `test_api_pipelines_with_data` | GET | после создания pipeline | json содержит pipeline |
| `test_api_pipelines_filter_by_id` | GET | `?pipeline_id=1` | 200, фильтрация |

---

### 3.3 `tests/routes/test_search_routes.py` (новый, 11 тестов)

**Маршруты:**
- `GET /` — root_page (redirect)
- `GET /search` — search_page

**Специфика:**
- `agent_manager` может быть `None` или `MagicMock(available=True/False)`
- `deps.search_service` мокается для контроля результатов

| Тест | Подготовка | Проверки |
|------|-----------|----------|
| `test_root_redirects_to_agent_when_available` | `agent_manager.available = True` | 303 → `/agent` |
| `test_root_redirects_to_search_when_no_agent` | `agent_manager = None` | 303 → `/search` |
| `test_search_page_renders` | аккаунт в DB | 200 |
| `test_search_redirects_to_settings_no_accounts` | пустая DB | 303 → `/settings` |
| `test_search_with_query` | mock `svc.search` | 200, mock вызван |
| `test_search_invalid_channel_id` | `?channel_id=bad` | 200, "Некорректный ID" in text |
| `test_search_pagination` | `?page=2` | 200 |
| `test_search_fts_mode` | `?is_fts=true` | 200 |
| `test_search_hybrid_mode` | `?mode=hybrid` | 200 |
| `test_search_error_rendered` | `svc.search` raises Exception | 200, error in text |
| `test_search_date_filters` | `?date_from=...&date_to=...` | 200 |

**Код мока:**

```python
def test_search_with_query(client, monkeypatch):
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr("src.web.routes.search.deps.search_service", lambda r: mock_svc)
    resp = await client.get("/search?q=test")
    assert resp.status_code == 200
```

---

### 3.4 `tests/routes/test_search_queries_routes.py` (новый, 11 тестов)

**Маршруты:**
- `GET /search-queries/`
- `POST /search-queries/add`
- `POST /search-queries/{sq_id}/toggle`
- `POST /search-queries/{sq_id}/edit`
- `POST /search-queries/{sq_id}/delete`
- `POST /search-queries/{sq_id}/run`

**Специфика:**
- Требует `CollectionQueue` в teardown
- Требует `SchedulerManager` для `sync_search_query_jobs()`

| Тест | Проверки |
|------|----------|
| `test_page_renders_empty` | 200 |
| `test_page_lists_items` | после add, текст содержит query |
| `test_add_redirects` | 303, `msg=sq_added` |
| `test_add_with_all_fields` | `is_regex, is_fts, notify_on_collect, track_stats` |
| `test_toggle` | 303, `msg=sq_toggled` |
| `test_edit` | 303, `msg=sq_edited` |
| `test_delete` | 303, `msg=sq_deleted`, DB пустая |
| `test_run_query` | mock `svc.run_once` |
| `test_scheduler_synced_after_add` | `scheduler.is_running=True`, mock sync |
| `test_scheduler_synced_after_toggle` | аналогично |
| `test_delete_nonexistent_no_crash` | 303, не 500 |

---

### 3.5 `tests/routes/test_channels_routes.py` (новый, 16 тестов)

**Маршруты:**
- `GET /channels/`
- `POST /channels/add`
- `GET /channels/dialogs`
- `POST /channels/add-bulk`
- `POST /channels/{pk}/toggle`
- `POST /channels/{pk}/delete`

**Специфика:**
- `pool_mock.resolve_channel` — AsyncMock
- `db.repos.dialog_cache.replace_dialogs` для `/dialogs`

| Тест | Проверки |
|------|----------|
| `test_channels_page_renders` | 200 |
| `test_add_channel_success` | 303, `msg=channel_added` |
| `test_add_channel_no_client` | RuntimeError("no_client") → `error=no_client` |
| `test_resolve_channel_fail` | `add_by_identifier` returns False → `error=resolve` |
| `test_get_dialogs_json` | dialogs в кэше → 200, list |
| `test_add_bulk` | 303, `msg=channels_added` |
| `test_toggle_channel` | 303, `msg=channel_toggled` |
| `test_delete_channel` | 303, `msg=channel_deleted` |
| `test_delete_channel_in_pipeline` | FOREIGN KEY → `error=channel_in_pipeline` |
| `test_collect_all_redirect` | 303 → `/channels` |
| `test_collect_all_htmx` | `HX-Request: true` → 200, fragment |
| `test_collect_all_shutting_down` | `shutting_down=True` → `error=shutting_down` |
| `test_collect_all_shutting_down_htmx` | HTMX + shutting_down → 200 |
| `test_collect_channel` | 303 |
| `test_collect_channel_htmx` | 200, `collect-btn-{pk}` |
| `test_collect_stats` | 303 |

**Код фикстуры pool_mock:**

```python
async def _resolve_channel(self, identifier):
    return {"channel_id": -1001234567890, "title": "Test", "username": "test", "channel_type": "channel"}

pool_mock = MagicMock()
pool_mock.clients = {"+1234567890": MagicMock()}
pool_mock.resolve_channel = _resolve_channel
pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
```

---

### 3.6 `tests/routes/test_filter_routes.py` (новый, 19 тестов)

**Маршруты:**
- `GET /channels/filter/manage`
- `POST /filter/purge-selected`
- `POST /filter/purge-all`
- `POST /filter/hard-delete-selected`
- `POST /filter/analyze`
- `POST /filter/apply`
- `POST /filter/precheck`
- `POST /filter/reset`
- `POST /{channel_id}/purge-messages`
- `POST /{pk}/filter-toggle`

**Специфика:**
- `dev_mode` через `db.set_setting("agent_dev_mode_enabled", "1")`
- FilterDeletionService мокается через monkeypatch

| Тест | Проверки |
|------|----------|
| `test_manage_renders_empty` | 200 |
| `test_manage_shows_filtered` | filtered канал виден |
| `test_purge_selected_no_pks` | `error=no_filtered_channels` |
| `test_purge_selected_success` | `msg=purged_selected` |
| `test_purge_selected_removes_messages` | сообщения удалены из DB |
| `test_purge_all_no_filtered` | `error=no_filtered_channels` |
| `test_purge_all_success` | `msg=purged_all_filtered` |
| `test_hard_delete_blocked_without_dev_mode` | `error=dev_mode_required_for_hard_delete` |
| `test_hard_delete_no_pks` | dev_mode + нет pks → error |
| `test_hard_delete_success` | dev_mode + pks → `msg=deleted_filtered` |
| `test_analyze_redirects` | 303 → `/filter/manage` |
| `test_apply_missing_snapshot` | `error=filter_snapshot_required` |
| `test_apply_with_snapshot` | `msg=filter_applied` |
| `test_precheck_redirects` | `msg=precheck_done` |
| `test_reset_redirects` | `msg=filter_reset` |
| `test_purge_messages_not_filtered` | `error=not_filtered` |
| `test_purge_messages_success` | `msg=purged` |
| `test_filter_toggle_not_found` | 303, не 500 |
| `test_filter_toggle_success` | `msg=filter_toggled` |

**Вспомогательные функции:**

```python
async def _add_filtered_channel(db, channel_id=200, title="Filtered") -> int:
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    pk = ...  # получить из DB
    await db.set_channel_filtered(pk, True)
    return pk

async def _enable_dev_mode(db):
    await db.set_setting("agent_dev_mode_enabled", "1")
```

---

### 3.7 `tests/routes/test_settings_routes.py` (новый, 11 тестов)

**Маршруты:** 19 эндпоинтов, но тестируем ключевые.

**Специфика:**
- Тяжёлые зависимости: `AgentProviderService.load_provider_configs`, `load_model_cache`
- Мокаем через monkeypatch

| Тест | Проверки |
|------|----------|
| `test_settings_page_renders` | 200 |
| `test_settings_shows_accounts` | phone in text |
| `test_settings_no_accounts` | 200, нет краша |
| `test_settings_msg_param` | `?msg=credentials_saved` → 200 |
| `test_save_scheduler` | `msg=scheduler_saved` |
| `test_save_scheduler_invalid` | `error=invalid_value` |
| `test_save_credentials_valid` | `msg=credentials_saved` |
| `test_save_credentials_invalid_id` | `error=invalid_api_id` |
| `test_toggle_account` | 303 |
| `test_delete_account` | 303 |
| `test_save_filters` | 303 |

**Код мока:**

```python
monkeypatch.setattr(
    "src.web.routes.settings.AgentProviderService.load_provider_configs",
    AsyncMock(return_value=[]),
)
monkeypatch.setattr(
    "src.web.routes.settings.AgentProviderService.load_model_cache",
    AsyncMock(return_value={}),
)
```

---

### 3.8 `tests/routes/test_agent_routes.py` (новый, 20 тестов)

**Маршруты:**
- `GET /agent`
- `POST /agent/threads`
- `DELETE /agent/threads/{thread_id}`
- `POST /agent/threads/{thread_id}/rename`
- `GET /agent/channels-json`
- `GET /agent/forum-topics`
- `POST /agent/threads/{thread_id}/context`
- `POST /agent/threads/{thread_id}/stop`
- `POST /agent/threads/{thread_id}/chat`

**Специфика:**
- Специальная фикстура с `agent_manager_mock`
- SSE streaming: проверяем `content-type: text/event-stream`

| Тест | Проверки |
|------|----------|
| `test_agent_page_autocreates_thread` | 303 → `?thread_id=1` |
| `test_agent_page_redirects_to_first_thread` | 303 → `thread_id=` |
| `test_agent_page_renders_with_thread` | 200 |
| `test_agent_page_invalid_thread_redirects` | 303 → существующий |
| `test_create_thread` | 303, location содержит `thread_id=` |
| `test_delete_thread` | 200, `{"ok": true}` |
| `test_rename_thread_success` | 200, ok |
| `test_rename_thread_empty_title` | 400 |
| `test_get_channels_json` | 200, list |
| `test_get_forum_topics_empty` | 200, `[]` |
| `test_get_forum_topics_returns_data` | данные в json |
| `test_inject_context_no_channel_id` | 400 |
| `test_inject_context_thread_not_found` | 404 |
| `test_inject_context_success` | 200, "content" in json |
| `test_stop_chat` | 200, `{"ok": true}` |
| `test_chat_no_agent_manager` | 503 |
| `test_chat_empty_message` | 400 |
| `test_chat_thread_not_found` | 404 |
| `test_chat_no_backend` | `selected_backend=None` → 503 |
| `test_chat_streaming` | 200, `text/event-stream` |

**Код фикстуры agent_manager:**

```python
@pytest.fixture
def agent_manager_mock():
    m = MagicMock(spec=AgentManager)
    m.available = True

    runtime = MagicMock()
    runtime.selected_backend = "deepagents"
    runtime.using_override = False
    runtime.error = None
    m.get_runtime_status = AsyncMock(return_value=runtime)
    m.cancel_stream = AsyncMock(return_value=False)
    m.estimate_prompt_tokens = AsyncMock(return_value=100)

    async def _fake_stream(*a, **kw):
        yield 'data: {"delta": "hi"}\n\n'
        yield 'data: {"done": true, "full_text": "hi"}\n\n'
    m.chat_stream = _fake_stream

    return m
```

---

### 3.9 `tests/routes/test_photo_loader_routes.py` (новый, 12 тестов)

**Маршруты:**
- `GET /dialogs/photos`
- `POST /photos/refresh`
- `POST /photos/send`
- `POST /photos/schedule`
- `POST /photos/batch`
- `POST /photos/auto`
- `POST /photos/run-due`
- `POST /photos/items/{item_id}/cancel`
- `POST /photos/auto/{job_id}/toggle`
- `POST /photos/auto/{job_id}/delete`

**Специфика:**
- `pool_mock.get_dialogs_for_phone` возвращает диалоги
- `_persist_uploads` мокается для file upload тестов

| Тест | Проверки |
|------|----------|
| `test_page_renders_no_phone` | 200 |
| `test_page_renders_with_phone` | `?phone=...` → 200 |
| `test_page_shows_no_jobs` | 200, нет краша |
| `test_refresh_redirects` | 303 |
| `test_send_missing_target` | `error=photo_target_required` |
| `test_send_invalid_target_id` | `error=photo_target_invalid` |
| `test_send_no_files` | mock `_persist_uploads` → `[]`, `error=photo_no_files` |
| `test_schedule_missing_target` | error |
| `test_run_due_redirects` | 303 |
| `test_cancel_item_not_found` | 303, не 500 |
| `test_toggle_auto_not_found` | 303, не 500 |
| `test_delete_auto_not_found` | 303, не 500 |

**Код мока:**

```python
monkeypatch.setattr(
    "src.web.routes.photo_loader._persist_uploads",
    AsyncMock(return_value=[]),
)
```

---

### 3.10 Дополнения в `test_moderation_routes.py` (+9 тестов)

**Новые тесты:**

| Тест | Проверки |
|------|----------|
| `test_view_run_renders` | `GET /moderation/{run_id}/view` → 200 |
| `test_view_run_not_found` | 303, `error=run_not_found` |
| `test_approve_run` | 303, `msg=run_approved` |
| `test_approve_run_not_found` | 303, error |
| `test_reject_run` | 303, `msg=run_rejected`, статус в DB |
| `test_reject_run_not_found` | 303, error |
| `test_bulk_approve` | 2 runs → оба approved |
| `test_bulk_reject` | 2 runs → оба rejected |
| `test_bulk_approve_empty` | без run_ids → 303, не 500 |

---

### 3.11 Дополнения в `test_pipelines_routes.py` (+5 тестов)

**Новые тесты:**

| Тест | Проверки |
|------|----------|
| `test_run_pipeline_not_found` | 303, `error=pipeline_invalid` |
| `test_run_pipeline_enqueues` | 303, `msg=pipeline_run_enqueued`, mock вызван |
| `test_run_pipeline_failure` | enqueue raises → `error=pipeline_run_failed` |
| `test_generate_page_renders` | 200 |
| `test_generate_page_not_found` | 303, error |

**Фикстура:**

```python
from src.services.task_enqueuer import TaskEnqueuer
enqueuer_mock = MagicMock(spec=TaskEnqueuer)
enqueuer_mock.enqueue_pipeline_run = AsyncMock()
app.state.task_enqueuer = enqueuer_mock
```

---

## 4. Технические ловушки

### 4.1 CollectionQueue.shutdown()

**Проблема:** Без вызова `shutdown()` воркер-поток aiosqlite держит pytest.

**Решение:** Всегда в teardown:

```python
await app.state.collection_queue.shutdown()
await db.close()
```

### 4.2 SSE Streaming

**Проблема:** httpx читает StreamingResponse целиком.

**Решение:** Достаточно проверить `status_code` и `content-type`:

```python
assert resp.status_code == 200
assert "text/event-stream" in resp.headers["content-type"]
```

### 4.3 Settings page — тяжёлые зависимости

**Проблема:** `AgentProviderService` делает HTTP-запросы.

**Решение:** Мокать через monkeypatch:

```python
monkeypatch.setattr("src.web.routes.settings.AgentProviderService.load_provider_configs", AsyncMock(return_value=[]))
monkeypatch.setattr("src.web.routes.settings.AgentProviderService.load_model_cache", AsyncMock(return_value={}))
```

### 4.4 MagicMock().available truthy по умолчанию

**Проблема:** `MagicMock().available` возвращает `MagicMock`, который truthy.

**Решение:** Явно задавать:

```python
agent_manager_mock.available = True   # или
agent_manager_mock.available = False
```

### 4.5 pool_mock.resolve_channel

**Проблема:** `MagicMock()` не coroutine.

**Решение:**

```python
pool_mock.resolve_channel = AsyncMock(return_value={...})
# или
async def _resolve(self, identifier):
    return {...}
pool_mock.resolve_channel = _resolve
```

---

## 5. Порядок реализации

### Фаза 1: Инфраструктура (1 файл)

1. `tests/routes/conftest.py` — общие фикстуры

### Фаза 2: Низкая сложность (2 файла)

2. `tests/routes/test_analytics_routes.py` — 10 тестов
3. `tests/routes/test_search_routes.py` — 11 тестов

### Фаза 3: Средняя сложность (5 файлов)

4. `tests/routes/test_search_queries_routes.py` — 11 тестов
5. `tests/routes/test_channels_routes.py` — 16 тестов
6. `tests/routes/test_filter_routes.py` — 19 тестов
7. `tests/routes/test_settings_routes.py` — 11 тестов
8. Дополнения `test_moderation_routes.py` — +9 тестов
9. Дополнения `test_pipelines_routes.py` — +5 тестов

### Фаза 4: Высокая сложность (2 файла)

10. `tests/routes/test_agent_routes.py` — 20 тестов
11. `tests/routes/test_photo_loader_routes.py` — 12 тестов

---

## 6. Верификация

```bash
# Новые тесты (параллельно)
pytest tests/routes/ -v -n auto

# Полный прогон
pytest tests/ -v -m "not aiosqlite_serial" -n auto

# Серийные тесты отдельно
pytest tests/ -v -m aiosqlite_serial
```

**Ожидаемый результат:** ≥80% покрытия веб-маршрутов, ~124 новых теста.

---

## 7. Карта зависимостей сервисов

```
Route → deps.xxx() → Service → Bundle → Database

deps.get_db(request)              → Database
deps.get_pool(request)            → ClientPool
deps.get_scheduler(request)       → SchedulerManager
deps.get_agent_manager(request)   → AgentManager | None
deps.channel_service(request)     → ChannelService → ChannelBundle + ClientPool + CollectionQueue
deps.account_service(request)     → AccountService → AccountBundle + ClientPool
deps.collection_service(request)  → CollectionService → ChannelBundle + Collector + CollectionQueue
deps.search_service(request)      → SearchService → SearchEngine + AISearchEngine
deps.search_query_service(request)→ SearchQueryService → SearchQueryBundle
deps.filter_deletion_service(request) → FilterDeletionService → Database + ChannelService
deps.pipeline_service(request)    → PipelineService → PipelineBundle
```

---

## 8. Шаблон тест-файла

```python
"""Tests for XXX routes."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app


@pytest.fixture
async def client(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    pool_mock = MagicMock()
    pool_mock.clients = {}
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c

    await db.close()


# === Тесты ===

async def test_page_renders(client):
    resp = await client.get("/xxx/")
    assert resp.status_code == 200
```
