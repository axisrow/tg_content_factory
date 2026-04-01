# План: покрытие тестами веб-интерфейса

## Контекст

Текущее покрытие веб-маршрутов ~17-20% (~25 из 120+ endpoints). Хорошо покрыты: auth, import_channels, scheduler, dialogs, debug. Полностью не покрыты: search, search_queries, filter, agent, photo_loader, analytics. Частично: channels, pipelines, moderation, settings.

Цель — добиться ~80%+ покрытия, написав ~123 новых теста в 9 новых файлах + дополнения к 2 существующим.

---

## Паттерн тестирования

Все тесты используют `httpx.AsyncClient` с `ASGITransport`:

```python
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
    app.state.pool = MagicMock()
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False
    # ...специфичные state атрибуты...

    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c
    await db.close()
```

**Ключевые правила:**
- Моки через `app.state.*`, не через `dependency_overrides`
- Точечный `patch("src.web.routes.X.deps.Y")` для конкретных сервисов
- POST — форм-данные через `data={...}`, JSON через `json={...}`
- Redirect: `follow_redirects=False` + проверка `location` header
- `CollectionQueue` в teardown: всегда `await cq.shutdown()`
- `asyncio_mode="auto"` в pyproject.toml → маркеры `@pytest.mark.asyncio` не нужны

---

## Порядок реализации

| # | Файл | Новых тестов | Сложность |
|---|------|-------------|-----------|
| 1 | `tests/routes/conftest.py` (создать) | фикстуры | Низкая |
| 2 | `tests/routes/test_analytics_routes.py` | 10 | Низкая |
| 3 | `tests/routes/test_search_routes.py` | 11 | Низкая |
| 4 | `tests/routes/test_search_queries_routes.py` | 11 | Средняя |
| 5 | `tests/routes/test_channels_routes.py` | 16 | Средняя |
| 6 | `tests/routes/test_filter_routes.py` | 19 | Средняя |
| 7 | `tests/routes/test_settings_routes.py` | 11 | Средняя |
| 8 | `tests/routes/test_agent_routes.py` | 20 | Высокая |
| 9 | `tests/routes/test_photo_loader_routes.py` | 12 | Высокая |
| 10 | Дополнить `test_moderation_routes.py` | +9 | Средняя |
| 11 | Дополнить `test_pipelines_routes.py` | +5 | Средняя |

**Итого: ~124 новых теста**

---

## Детальная спецификация

### 1. `tests/routes/conftest.py` (новый)

Общие фикстуры для нескольких файлов:

```python
@pytest.fixture
async def base_app(tmp_path):
    """Полностью сконфигурированный app + db с одним аккаунтом и одним каналом."""
    # config + app + db + pool_mock + CollectionQueue + SchedulerManager
    # db.add_account(Account(phone="+1234567890"))
    # db.add_channel(Channel(channel_id=100, title="Test"))
    # app.state.agent_manager = None
    # yields (app, db)
    # teardown: await cq.shutdown(); await db.close()

@pytest.fixture
async def route_client(base_app):
    """AsyncClient с Basic auth и follow_redirects=True."""
    # yields client
```

Файлы со специфичными потребностями (agent, photo_loader) объявляют свой `client` fixture.

---

### 2. `tests/routes/test_analytics_routes.py`

Использует `route_client`. `ContentAnalyticsService` работает с реальной DB — моки не нужны.

| Тест | Проверяет |
|------|-----------|
| `test_analytics_page_renders` | `GET /analytics` → 200 |
| `test_analytics_page_with_dates` | `GET /analytics?date_from=2024-01-01&date_to=2024-12-31` → 200 |
| `test_analytics_page_limit_param` | `GET /analytics?limit=20` → 200 |
| `test_analytics_page_invalid_limit` | `GET /analytics?limit=abc` → 200, нет 500 |
| `test_analytics_page_empty_db` | без данных → 200 |
| `test_content_analytics_page_renders` | `GET /analytics/content` → 200 |
| `test_api_content_summary_returns_json` | `GET /analytics/content/api/summary` → 200, `resp.json()` is dict |
| `test_api_pipelines_returns_json` | `GET /analytics/content/api/pipelines` → 200, `resp.json()` is list |
| `test_api_pipelines_with_data` | создать pipeline в DB → json содержит pipeline |
| `test_api_pipelines_filter_by_id` | `GET /api/pipelines?pipeline_id=1` → 200 |

---

### 3. `tests/routes/test_search_routes.py`

Локальный `client` fixture. Мок `search_service` через `monkeypatch.setattr("src.web.routes.search.deps.search_service", ...)`.

| Тест | Подготовка | Проверяет |
|------|-----------|-----------|
| `test_root_redirects_to_agent_when_available` | `agent_manager_mock.available = True` | `GET /` → `/agent` |
| `test_root_redirects_to_search_when_no_agent` | `agent_manager = None` | `GET /` → `/search` |
| `test_search_page_renders` | аккаунт в DB | 200 |
| `test_search_redirects_to_settings_no_accounts` | пустая DB | 303 → `/settings` |
| `test_search_with_query` | мок `svc.search = AsyncMock(return_value=...)` | 200, mock вызван |
| `test_search_invalid_channel_id` | `GET /search?q=x&channel_id=bad` | 200, "Некорректный ID" in text |
| `test_search_pagination` | `GET /search?q=x&page=2` | 200 |
| `test_search_fts_mode` | `GET /search?q=x&is_fts=true` | 200 |
| `test_search_hybrid_mode` | `GET /search?q=x&mode=hybrid` | 200 |
| `test_search_error_rendered` | `svc.search` raises Exception | 200, сообщение об ошибке in text |
| `test_search_date_filters` | `GET /search?q=x&date_from=2024-01-01&date_to=2024-12-31` | 200 |

---

### 4. `tests/routes/test_search_queries_routes.py`

Локальный `client` с `CollectionQueue` (для teardown) и `SchedulerManager`.

Вспомогательная функция:
```python
async def _add_sq(client, query="test query", interval=60) -> int:
    resp = await client.post("/search-queries/add",
        data={"query": query, "interval_minutes": str(interval)},
        follow_redirects=False)
    # извлечь id из DB или location
```

| Тест | Проверяет |
|------|-----------|
| `test_page_renders_empty` | `GET /search-queries/` → 200 |
| `test_page_lists_items` | после `_add_sq` → "test query" in text |
| `test_add_redirects` | 303, `msg=sq_added` in location |
| `test_add_with_all_fields` | `is_regex=true, is_fts=true, notify_on_collect=true, track_stats=true` → 303 |
| `test_toggle` | `POST /{sq_id}/toggle` → 303, `msg=sq_toggled` |
| `test_edit` | `POST /{sq_id}/edit` новые данные → 303, `msg=sq_edited` |
| `test_delete` | `POST /{sq_id}/delete` → 303, `msg=sq_deleted`; DB пустая |
| `test_run_query` | мок `svc.run_once = AsyncMock` → 303, `msg=sq_run`, mock вызван |
| `test_scheduler_synced_after_add` | `scheduler.is_running=True`, мок `sync_search_query_jobs` → after POST assert_awaited |
| `test_scheduler_synced_after_toggle` | аналогично |
| `test_delete_nonexistent_no_crash` | `POST /999/delete` → 303, не 500 |

---

### 5. `tests/routes/test_channels_routes.py`

Локальный `client`. `pool_mock.resolve_channel = AsyncMock(return_value={channel_id:..., title:..., ...})`.

Вспомогательная функция:
```python
async def _add_channel(db, channel_id=100, title="Test") -> int:
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    channels = await db.get_channels_with_counts()
    return next(c.id for c in channels if c.channel_id == channel_id)
```

| Тест | Проверяет |
|------|-----------|
| `test_channels_page_renders` | `GET /channels/` → 200, "Test" in text |
| `test_add_channel_success` | `POST /channels/add` → 303, `msg=channel_added` |
| `test_add_channel_no_client` | `add_by_identifier` raises "no_client" → 303, `error=no_client` |
| `test_resolve_channel_fail` | `add_by_identifier` returns `False` → 303, `error=resolve` |
| `test_get_dialogs_json` | `db.repos.dialog_cache` заполнен → `GET /dialogs` → 200, list |
| `test_add_bulk` | диалоги в кэше → `POST /add-bulk` → 303, `msg=channels_added` |
| `test_toggle_channel` | канал в DB → `POST /{pk}/toggle` → 303, `msg=channel_toggled` |
| `test_delete_channel` | `POST /{pk}/delete` → 303, `msg=channel_deleted` |
| `test_delete_channel_in_pipeline` | delete raises FOREIGN KEY → 303, `error=channel_in_pipeline` |
| `test_collect_all_redirect` | `POST /collect-all` → 303 → `/channels` |
| `test_collect_all_htmx` | header `HX-Request: true` → 200, fragment |
| `test_collect_all_shutting_down` | `shutting_down=True` → 303, `error=shutting_down` |
| `test_collect_all_shutting_down_htmx` | HTMX + shutting_down → 200, текст об остановке |
| `test_collect_channel` | `POST /{pk}/collect` → 303 |
| `test_collect_channel_htmx` | HTMX → 200, `f"collect-btn-{pk}"` in text |
| `test_collect_stats` | `POST /{pk}/stats` → 303 |

---

### 6. `tests/routes/test_filter_routes.py`

Локальный `client`. Вспомогательные функции:

```python
async def _add_filtered_channel(db, channel_id=200, title="Filtered") -> int:
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    pk = ...  # получить из DB
    await db.set_channel_filtered(pk, True)
    return pk

async def _enable_dev_mode(db):
    await db.repos.settings.set("agent_dev_mode_enabled", "1")
```

| Тест | Проверяет |
|------|-----------|
| `test_manage_renders_empty` | `GET /channels/filter/manage` → 200 |
| `test_manage_shows_filtered` | filtered канал → "Filtered" in text |
| `test_purge_selected_no_pks` | POST без pks → 303, `error=no_filtered_channels` |
| `test_purge_selected_success` | с `pks=[pk]` → 303, `msg=purged_selected` |
| `test_purge_selected_removes_messages` | после purge — сообщения удалены из DB |
| `test_purge_all_no_filtered` | пустая filtered list → 303, `error=no_filtered_channels` |
| `test_purge_all_success` | 2 filtered → 303, `msg=purged_all_filtered` |
| `test_hard_delete_blocked_without_dev_mode` | 303, `error=dev_mode_required_for_hard_delete` |
| `test_hard_delete_no_pks` | dev_mode + нет pks → 303, `error=no_filtered_channels` |
| `test_hard_delete_success` | dev_mode + pks → 303, `msg=deleted_filtered` |
| `test_analyze_redirects` | `POST /analyze` → 303 → `/filter/manage` |
| `test_apply_missing_snapshot` | без `snapshot=1` → 303, `error=filter_snapshot_required` |
| `test_apply_with_snapshot` | `snapshot=1, selected=[...]` → 303, `msg=filter_applied` |
| `test_precheck_redirects` | `POST /precheck` → 303, `msg=precheck_done` |
| `test_reset_redirects` | `POST /reset` → 303, `msg=filter_reset` |
| `test_purge_messages_not_filtered` | обычный канал → 303, `error=not_filtered` |
| `test_purge_messages_success` | filtered канал → 303, `msg=purged` |
| `test_filter_toggle_not_found` | `POST /9999/filter-toggle` → 303, not 500 |
| `test_filter_toggle_success` | канал в DB → 303, `msg=filter_toggled` |

---

### 7. `tests/routes/test_settings_routes.py`

Локальный `client`. Мокаем тяжёлые сервисы через `monkeypatch`:
```python
monkeypatch.setattr("src.web.routes.settings.AgentProviderService.load_provider_configs", AsyncMock(return_value=[]))
monkeypatch.setattr("src.web.routes.settings.AgentProviderService.load_model_cache", AsyncMock(return_value={}))
```

| Тест | Проверяет |
|------|-----------|
| `test_settings_page_renders` | `GET /settings` → 200 |
| `test_settings_shows_accounts` | аккаунт в DB → "+1234567890" in text |
| `test_settings_no_accounts` | пустая DB → 200, нет краша |
| `test_settings_msg_param` | `GET /settings?msg=credentials_saved` → 200 |
| `test_save_scheduler` | `POST /settings/save-scheduler` → 303, `msg=scheduler_saved` |
| `test_save_scheduler_invalid` | `collect_interval_minutes=abc` → 303, `error=invalid_value` |
| `test_save_credentials_valid` | `api_id=12345, api_hash=abc` → 303, `msg=credentials_saved` |
| `test_save_credentials_invalid_id` | `api_id=notanumber` → 303, `error=invalid_api_id` |
| `test_toggle_account` | аккаунт в DB → `POST /settings/{phone}/toggle` → 303 |
| `test_delete_account` | → `POST /settings/{phone}/delete` → 303 |
| `test_save_filters` | `POST /settings/save-filters` с параметрами → 303 |

---

### 8. `tests/routes/test_agent_routes.py`

Специальный `client` fixture с `agent_manager_mock`:

```python
agent_manager_mock = MagicMock(spec=AgentManager)
agent_manager_mock.available = True
agent_manager_mock.get_runtime_status = AsyncMock(return_value=runtime_status_mock)
agent_manager_mock.cancel_stream = AsyncMock(return_value=False)
agent_manager_mock.estimate_prompt_tokens = AsyncMock(return_value=100)
app.state.agent_manager = agent_manager_mock

# Для streaming:
async def _fake_stream(*a, **kw):
    yield 'data: {"delta": "hi"}\n\n'
    yield 'data: {"done": true, "full_text": "hi"}\n\n'
agent_manager_mock.chat_stream = _fake_stream
```

| Тест | Проверяет |
|------|-----------|
| `test_agent_page_autocreates_thread` | нет тредов → 303 → `/agent?thread_id=1` |
| `test_agent_page_redirects_to_first_thread` | тред в DB → 303 → `thread_id=1` |
| `test_agent_page_renders_with_thread` | `GET /agent?thread_id=1` → 200 |
| `test_agent_page_invalid_thread_redirects` | `?thread_id=999` → 303 → существующий thread |
| `test_create_thread` | `POST /agent/threads` → 303, location содержит `thread_id=` |
| `test_delete_thread` | `DELETE /agent/threads/1` → 200, `{"ok": True}` |
| `test_rename_thread_success` | `POST /threads/1/rename` json `{"title":"New"}` → 200, ok |
| `test_rename_thread_empty_title` | `{"title":""}` → 400 |
| `test_get_channels_json` | каналы в DB → `GET /agent/channels-json` → 200, list |
| `test_get_forum_topics_empty` | `pool_mock.get_forum_topics = AsyncMock([])` → 200, `[]` |
| `test_get_forum_topics_returns_data` | `AsyncMock([{"id":1,"title":"T"}])` → данные в json |
| `test_inject_context_no_channel_id` | `POST /threads/1/context` json `{}` → 400 |
| `test_inject_context_thread_not_found` | `POST /threads/999/context` → 404 |
| `test_inject_context_success` | тред + канал → 200, "content" in json |
| `test_stop_chat` | `POST /threads/1/stop` → 200, `{"ok": True, "cancelled": False}` |
| `test_chat_no_agent_manager` | `agent_manager = None` → 503 |
| `test_chat_empty_message` | `json={"message":""}` → 400 |
| `test_chat_thread_not_found` | `POST /threads/9999/chat` → 404 |
| `test_chat_no_backend` | `runtime_status.selected_backend = None` → 503 |
| `test_chat_streaming` | `POST /threads/1/chat` → 200, `text/event-stream` |

---

### 9. `tests/routes/test_photo_loader_routes.py`

Специальный `client` fixture. `pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[...])`.
Для upload-тестов: `monkeypatch.setattr("src.web.routes.photo_loader._persist_uploads", AsyncMock(return_value=[]))`.

| Тест | Проверяет |
|------|-----------|
| `test_page_renders_no_phone` | `GET /dialogs/photos` → 200 |
| `test_page_renders_with_phone` | `GET /dialogs/photos?phone=%2B1234567890` → 200 |
| `test_page_shows_no_jobs` | нет jobs в DB → 200, нет краша |
| `test_refresh_redirects` | `POST /photos/refresh` → 303 |
| `test_send_missing_target` | без `target_dialog_id` → 303, `error=photo_target_required` |
| `test_send_invalid_target_id` | `target_dialog_id=abc` → 303, `error=photo_target_invalid` |
| `test_send_no_files` | мок `_persist_uploads` → `[]`, верный target → 303, `error=photo_no_files` |
| `test_schedule_missing_target` | `POST /schedule` без target → 303, error |
| `test_run_due_redirects` | `POST /run-due` data `{"phone":"+1234567890"}` → 303 |
| `test_cancel_item_not_found` | `POST /items/999/cancel` → 303, нет 500 |
| `test_toggle_auto_not_found` | `POST /auto/999/toggle` → 303, нет 500 |
| `test_delete_auto_not_found` | `POST /auto/999/delete` → 303, нет 500 |

---

### 10. Дополнения в `test_moderation_routes.py`

Вспомогательная функция:
```python
async def _create_pipeline_and_run(db) -> tuple[int, int]:
    pipeline_id = await db.repos.pipelines.create(...)
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "template")
    await db.repos.generation_runs.save_result(run_id, "Post text")
    return pipeline_id, run_id
```

| Тест | Проверяет |
|------|-----------|
| `test_view_run_renders` | `GET /moderation/{run_id}/view` → 200 |
| `test_view_run_not_found` | `GET /moderation/9999/view` → 303, `error=run_not_found` |
| `test_approve_run` | `POST /{run_id}/approve` → 303, `msg=run_approved` |
| `test_approve_run_not_found` | `POST /9999/approve` → 303, `error=run_not_found` |
| `test_reject_run` | `POST /{run_id}/reject` → 303, `msg=run_rejected`; статус в DB = rejected |
| `test_reject_run_not_found` | `POST /9999/reject` → 303, `error=run_not_found` |
| `test_bulk_approve` | 2 runs → `POST /bulk-approve` `data={"run_ids":[1,2]}` → 303, оба approved |
| `test_bulk_reject` | 2 runs → `POST /bulk-reject` → 303, оба rejected |
| `test_bulk_approve_empty` | без run_ids → 303, нет 500 |

---

### 11. Дополнения в `test_pipelines_routes.py`

```python
# В fixture добавить:
from src.services.task_enqueuer import TaskEnqueuer
enqueuer_mock = MagicMock(spec=TaskEnqueuer)
enqueuer_mock.enqueue_pipeline_run = AsyncMock()
app.state.task_enqueuer = enqueuer_mock
```

| Тест | Проверяет |
|------|-----------|
| `test_run_pipeline_not_found` | `POST /pipelines/999/run` → 303, `error=pipeline_invalid` |
| `test_run_pipeline_enqueues` | pipeline в DB → 303, `msg=pipeline_run_enqueued`; mock вызван |
| `test_run_pipeline_failure` | `enqueue_pipeline_run` raises Exception → 303, `error=pipeline_run_failed` |
| `test_generate_page_renders` | pipeline в DB → `GET /pipelines/1/generate` → 200 |
| `test_generate_page_not_found` | `GET /pipelines/999/generate` → 303, `error=pipeline_invalid` |

---

## Технические ловушки

1. **`CollectionQueue.shutdown()` в teardown** — всегда вызывать, иначе воркер-поток держит pytest
2. **SSE streaming** — httpx читает StreamingResponse целиком, достаточно проверить `status_code` и `content-type: text/event-stream`
3. **`settings_page` тяжёлые зависимости** — патчить `AgentProviderService.load_provider_configs` и `load_model_cache` через monkeypatch
4. **`agent_manager_mock.available`** — MagicMock().available truthy по умолчанию, задавать явно `= True` или `= False`
5. **`pool_mock.resolve_channel`** — использовать `AsyncMock(return_value={...})`, не bound method

---

## Критичные файлы для реализации

- `tests/routes/test_scheduler_routes.py` — эталон паттерна fixture с CollectionQueue и teardown
- `tests/routes/test_pipelines_routes.py` — эталон fixture и структуры POST-тестов
- `tests/routes/test_moderation_routes.py` — паттерн вспомогательных DB-функций и monkeypatch
- `src/web/deps.py` — точки монтирования для правильных путей `patch(...)`
- `src/web/routes/agent.py` — логика agent routes и нужные поля `AgentManager`
- `src/web/container.py` — список всех `app.state.*` атрибутов

---

## Верификация

После реализации запустить:

```bash
# Новые тесты (параллельно)
pytest tests/routes/ -v -n auto

# Полный прогон (существующие + новые)
pytest tests/ -v -m "not aiosqlite_serial" -n auto

# Серийные тесты отдельно
pytest tests/ -v -m aiosqlite_serial
```

Ожидаемое покрытие после реализации: **≥80% веб-маршрутов**.
