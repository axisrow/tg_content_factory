# Parity-матрица: домен × {FastAPI, Web, CLI, agent, TUI}

> **Статус:** аудит-отчёт (issue [#1048](https://github.com/axisrow/tg_content_factory/issues/1048), родитель [#1023](https://github.com/axisrow/tg_content_factory/issues/1023), группа 3 — parity).
> **Тип:** read-only аудит. **Код-правок нет** — только инвентарь + ранжированный список пробелов. Конкретные фиксы декомпозируются в отдельные sub-issue ПОСЛЕ по результатам этого отчёта.
> **Метод:** верификация каждой ячейки по реальному коду (grep + исполнение `permissions.py`), а не копирование карты [#1022](feature-map.md) вслепую.
> **Эталон:** [`docs/feature-map.md`](feature-map.md) — карта сервисов (#1022), метод и формат таблиц поверхностей.

## Что это и зачем

У проекта **5 РАЗНЫХ поверхностей** к одной логике. Этот отчёт — матрица «26 сервисов × 5 поверхностей» + список пробелов parity, где функция есть в одной поверхности, но неоправданно забыта в другой. Это вход для эпика автодоки (#1022): пробелы по FastAPI REST = то, что не попадёт в авто-генерируемые `/docs`.

## Пять поверхностей (из #1023)

| # | Поверхность | Для кого | Где живёт | Узкая? |
|---|-------------|----------|-----------|:------:|
| 1 | **FastAPI REST** (`/api/*`, JSON) | для **программ** (interop) | `src/web/routes/tasks.py` (`/api/tasks`, #829) + машинные JSON parity-зеркала внутри HTML-роутеров | ✅ узкая |
| 2 | **Web** (HTML/HTMX) | для **человека** в браузере | `src/web/routes/` — Jinja2/HTMX/SSE/формы | — |
| 3 | **CLI** | команды терминала | `src/cli/commands/` + `src/cli/parser_domains/` | — |
| 4 | **Agent-tools** | тулы AI-агента (MCP) | `src/agent/tools/` | — |
| 5 | **TUI** (Textual) | интерактивный терминальный UI | `src/cli/commands/agent_tui.py` + `.tcss` | ✅ узкая |

**Ключевое (NB из #1023):** `FastAPI ≠ Web` — разные потребители (программы vs человек). **TUI и FastAPI — узкие**: для них parity ≠ «всё везде», а «нет неоправданных пробелов». Пробел в узкой поверхности — это **«by design»**, не баг, если он осознан.

## Авторитетные счётчики (верифицированы исполнением кода)

Числа ниже получены прямым исполнением `src/agent/tools/permissions.py` и подсчётом регистраторов — НЕ скопированы из карты. Сверены с инвариантом `tests/test_agent_tools_permissions.py` (который проверяет `len(tools) == len(TOOL_CATEGORIES)` и `len(modules) == len(MODULE_GROUPS)`, а не хардкод-числа).

| Поверхность | Счётчик | Источник истины |
|-------------|---------|-----------------|
| **FastAPI REST** | 1 полноценный interop-контур (`/api/tasks`) + JSON parity-зеркала в ~14 роутерах | `src/web/routes/tasks.py` + `JSONResponse`-эндпоинты |
| **Web** | 26 смонтированных роутеров, ~18 UI-страниц меню | `src/web/assembly.py` |
| **CLI** | 25 регистраторов доменов (`server_control` даёт `stop`+`restart` → 26 top-level команд), ~209 leaf-команд | `src/cli/parser_domains/__init__.py` |
| **Agent-tools** | **173 tools**, **20 групп**, **75 read / 82 write / 16 delete** | `permissions.py` (`TOOL_CATEGORIES`, `MODULE_GROUPS`) |
| **TUI** | 1 app (`AgentTuiApp`) — покрывает ровно 1 сервис («AI-агент») | `src/cli/commands/agent_tui.py` |

> **20 групп agent-tools** (verbatim): Поиск (8), Каналы (18), Сбор (4), Пайплайны (20), Модерация (6), Поисковые запросы (8), Аккаунты (8), Фильтры (8), Аналитика (15), Планировщик (8), Уведомления (5), Фото (14), Диалоги (11), Сообщения (14), Управление чатом (8), Изображения (4), Экспорт (1), Настройки (6), Треды агента (5), Веб-поиск (2). **Нет групп** для Jobs, Providers-management, Calendar (одиночный `get_calendar` в Аналитике), Translation (одиночный `translate_message` в Сообщениях), Limits, A/B.

---

## Легенда матрицы

| Метка | Смысл |
|-------|-------|
| ✅ | поверхность есть |
| — | поверхности нет |
| ⚠️ | **пробел parity** — функция ЕСТЬ в др. поверхностях, тут неоправданно забыта (нарушает инвариант) |
| 🟦 | **by design** — поверхности нет осознанно (узкая поверхность: TUI=только чат, FastAPI=только interop/parity-зеркало) |
| 🟥 | **нет выделенной поверхности нигде** — сервис управляется только через config / не подключён; это НЕ parity-пробел в смысле инварианта (нечему быть «забытым в одной поверхности»), а вопрос «нужна ли поверхность вообще» |
| N/A | поверхность неприменима к этому сервису |

> **⚠️ vs 🟥 — важное различие.** `⚠️` = функция доступна в ≥1 поверхности, но забыта в другой (асимметрия → нарушение инварианта `CLAUDE.md:160`). `🟥` = функции нет ни в одной из 5 поверхностей (только config-only или вообще не подключена) — это не «забыли поверхность», а отдельный вопрос продуктового решения. Поэтому **🟥-ячейки НЕ входят в счёт настоящих parity-пробелов** (их 3, все ⚠️); 🟥 разбираются в Тир 3.

---

## Матрица: 26 сервисов × 5 поверхностей

| # | Сервис | FastAPI REST | Web | CLI | agent | TUI |
|---|--------|:---:|:---:|:---:|:---:|:---:|
| 1 | 📥 Collection | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 2 | 🔎 Search | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 3 | 🏷 Channels | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 4 | 🧹 Filter | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 5 | ⭐ Channel Rating | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 6 | 🤖 Content Generation | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 7 | 🖼 Image Generation | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 8 | 🚰 Pipelines | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 9 | ✅ Moderation | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 10 | 📤 Publishing | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 11 | 📸 Photo Loader | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 12 | 💬 Dialogs | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 13 | 👤 Accounts | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 14 | 🔔 Notifications | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 15 | 🔁 Search Queries | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 16 | 📊 Analytics (+тренды) | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 17 | 📅 Calendar | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 18 | ⏰ Scheduler | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 19 | 🗂 **Jobs** | ✅ | ✅ | **⚠️** | **⚠️** | 🟦 |
| 20 | 🌍 Translation | 🟦 | ✅ | ✅ | ✅ | 🟦 |
| 21 | 📦 Export | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 22 | 🔌 **Providers** | ✅ | ✅ | ✅ | **⚠️** | 🟦 |
| 23 | 🧠 Agent / chat | ✅ | ✅ | ✅ | ✅ | ✅ |
| 24 | ⚙️ Settings & Debug | ✅ | ✅ | ✅ | ✅ | 🟦 |
| 25 | 🛡 **Prod Limits & Quality** | — | **🟥** | **🟥** | **🟥** | 🟦 |
| 26 | 🧪 **A/B Testing** | — | — | — | — | N/A |

> **TUI-колонка:** ровно один сервис (#23 Agent) = ✅. Все остальные = 🟦 by design — `AgentTuiApp` (`src/cli/commands/agent_tui.py`) единственный Textual-app в проекте (подтверждено grep по `from textual`/`ComposeResult`/`.tcss`) и покрывает только AI-чат (треды + потоковый чат + диалог разрешений; модель жёстко `None`, выбора провайдера/настроек нет). Сервис #25 (Prod Limits) = 🟥 в Web/CLI/agent: нет выделенной поверхности нигде (config-only, off-by-default), FastAPI = «—» — это не parity-пробел, а вопрос «нужна ли поверхность» (Тир 3, D2). Сервис #26 = N/A: A/B не подключён ни к одной поверхности вообще.

---

## Верификация ячеек (ссылки на реальный код)

Каждая нетривиальная ячейка подтверждена файлом:символом. Полная детализация по сервисам — в карте [#1022](feature-map.md); ниже только то, что верифицировалось/уточнялось в этом аудите.

### FastAPI REST: где «—» (нет JSON parity-зеркала вообще)
Сервисы **только-HTML** (web есть, JSON-эндпоинта нет): **Collection** (`channel_collection.py` — HTMX-фрагменты), **Channels** (`channels.py` — теги тоже HTML), **Filter** (`filter.py`), **Moderation** (`moderation.py`), **Scheduler** (`scheduler.py`), **Translation** (`settings.py` — только HTML-формы). Это помечено 🟦 — FastAPI узкий, отсутствие JSON-зеркала для чисто-человеческих операций оправдано.

### Уточнения к карте #1022 (выявлено при верификации)
- **Export (#21) web** — это НЕ HTML-страница: `POST /channels/{channel_id}/export` → `JSONResponse` (`src/web/routes/export.py:24-34`, #834). Web UI = кнопка на `/channels`, дёргающая этот эндпоинт. Поэтому Export в FastAPI REST = ✅ (`/rss.xml`, `/atom.xml`, `POST .../export`), web = ✅ (кнопка).
- **Providers refresh/probe/test-all** — это JSON-эндпоинты внутри settings-роутера (`src/web/routes/settings.py:179-209`, `settings_json_response`), не отдельный REST-контур. FastAPI REST для Providers = ✅ через них.

### Jobs (#19) — детально
- Web: `GET /jobs` + `GET /jobs/fragments/list` (`src/web/routes/jobs.py:46-88`), бэкенд `JobsReadModel` (`src/services/jobs_read_model.py`).
- FastAPI REST: `GET /jobs/api/list` (JSON, filterable по source/status — `jobs.py:57-66`).
- CLI: **нет домена `jobs`** — подтверждено: в `src/cli/parser_domains/__init__.py` 25 регистраторов, `jobs` отсутствует. Косвенно видно через `scheduler status`, `dialogs queue status`, `pipeline runs`.
- agent: **нет прямого tool** — только косвенно `get_telegram_queue_status` (группа «Сообщения») и `get_pipeline_queue` (группа «Пайплайны»). Нет агрегированного jobs-tool.

### Providers (#22) — детально
- CLI: `provider list/add/delete/probe/refresh/test-all` (`src/cli/commands/provider.py`).
- Web: `POST /settings/agent-providers/{add,save,delete,refresh,probe,test-all}` + image-providers (`settings.py`).
- agent: **нет прямого tool управления** — только косвенно `list_image_providers` (READ, группа «Изображения», `src/agent/tools/images.py`). Нет `add_provider`/`probe_provider`/`test_all_providers`/`refresh_provider` для agent-провайдеров.

### Production Limits & Quality (#25) — детально
- `QualityScoringService` — **подключён** (импортируется в `src/web/pipelines/handlers.py:576`, `src/cli/commands/pipeline.py`, `src/services/task_handlers/base.py`), пишет `quality_score`.
- `ProductionLimitsService` — **подключён, но off-by-default**: `from_config()` возвращает `None` если `config.production_limits.enabled` ложно (`src/services/production_limits_service.py:387-391`, #814). Используется в `image_generation_service.py`, `task_handlers/base.py`.
- `ErrorRecoveryService` — **сейчас не подключён**: импортов вне своего файла = 0 (только `tests/test_error_recovery_service.py`). По решению владельца (vulture-аудит #1044) — **подключить**, не удалять.
- **Нет выделенной поверхности** ни в CLI, ни в Web, ни в agent — управление только через `config.yaml` / косвенно `settings`.

### A/B Testing (#26) — детально
- `ABTestingService` (`src/services/ab_testing_service.py`) — **сейчас изолирован**: импортов вне своего файла = 0 (только `tests/test_ab_testing_service.py`, `tests/test_cross_domain_regression_paths.py`). Не подключён ни к одной из 5 поверхностей. По решению владельца (vulture-аудит #1044) — **подключить**, не удалять; parity по поверхностям появится после подключения.

### Инвариант parity
Декларирован в `CLAUDE.md:160`: *«CLI/Web parity: every web operation must have a CLI equivalent and vice versa»*. Нарушается в ячейках, помеченных ⚠️ ниже.

---

## Ранжированный список пробелов

Классификация: **🔴 настоящий пробел** (надо закрыть) · **🟡 спорный** (зависит от решения владельца) · **⚪ by design / debt** (оставить или отдельный трек).

### 🔴 Тир 1 — настоящие пробелы (нарушение инварианта parity)

| # | Пробел | Где есть | Где нет | Рекомендация |
|---|--------|----------|---------|--------------|
| P1 | **Jobs → CLI** | Web + FastAPI REST + бэкенд `JobsReadModel` | CLI (нет домена `jobs`) | **Закрыть.** Read-only `jobs list [--source S] [--status S]` — тонкая обёртка над `JobsReadModel`, зеркалит `/jobs/api/list`. Нарушает `CLAUDE.md:160`. |
| P2 | **Jobs → agent-tool** | Web + FastAPI REST | agent (только косвенно `get_telegram_queue_status`/`get_pipeline_queue`) | **Закрыть.** `get_jobs` (READ) поверх `JobsReadModel` — единая точка для агента вместо двух частичных. |
| P3 | **Providers (agent-mgmt) → agent-tool** | CLI (`provider *`) + Web (`/settings/agent-providers/*`) | agent (только `list_image_providers` READ) | **Закрыть частично.** Минимум READ `list_providers` (паритет к `provider list`). Write-tools (add/delete/probe/test-all) — опционально, под `require_confirmation` (provider-spend). |

### 🟡 Тир 2 — спорные (решает владелец)

| # | Пробел | Где есть | Где нет | Рекомендация |
|---|--------|----------|---------|--------------|
| P4 | **Server Control (stop/restart) → Web** | CLI (`stop`, `restart` — `server_control.py`) | Web, agent | **Оставить by design (склоняюсь).** Управление жизненным циклом процесса из браузера/агента опасно (саморубка веб-сервера). CLI/ops-плоскость — правильное место. Если нужен graceful-restart из UI — отдельный осознанный sub-issue. |
| P5 | **Test (`test all/read/write/telegram/benchmark`) → Web/agent** | CLI (`test.py`) | Web, agent | **Оставить by design.** Диагностические/dev-команды; их место в CLI. Частично есть в web как точечные провайдер/notification-тесты. |
| P6 | **Messages Read (лента диалога по identifier + `--live`) → agent** | CLI (`messages read`) + FastAPI REST (`GET /messages/{identifier}`, JSON-зеркало — `search.py:13`, `response_class=JSONResponse`) | agent (есть `search_in_channel`, но не read-by-identifier с `--live`) | **Спорно.** `search_messages`/`search_in_channel` покрывают поиск, но не «прочитать ленту канала как есть». Кандидат на `read_channel_messages` если агенту нужен этот режим. |

### ⚪ Тир 3 — by design / технический долг (не parity-фикс)

| # | Пункт | Статус | Рекомендация |
|---|-------|--------|--------------|
| D1 | **MCP-Server (`mcp-server`) → Web/agent** | CLI-only | **By design.** Спец-команда запуска stdio-MCP для внешних агентов (Codex/ADK). Поверхность процесса, не продуктовая фича. Оставить. |
| D2 | **Prod Limits & Quality (#25) — нет поверхности нигде** (🟥) | config-only, off-by-default | **Спорно/debt — НЕ parity-пробел** (нет асимметрии: функции нет ни в одной из 5 поверхностей, нечему быть «забытым»). Если лимиты реально нужны в проде — они по умолчанию не работают (риск перерасхода). Кандидат на `settings limits get/set` + web-тумблер. Решает владелец: нужны ли лимиты как продуктовая фича. |
| D3 | **ErrorRecoveryService — не подключён** | импортов 0 | **Подключить** (решение владельца, #1044 — НЕ удалять). Не parity-вопрос до подключения; после — войдёт в матрицу. Входит в эпик «подключить неиспользуемое». |
| D4 | **ABTestingService (#26) — изолирован** | импортов 0 (кроме тестов) | **Подключить** (решение владельца, #1044 — НЕ удалять). После подключения — parity по 4 поверхностям (FastAPI узкий). Входит в эпик «подключить неиспользуемое». |

---

## Предложенные follow-up sub-issue

По результатам аудита (родитель #1023, группа parity):

1. **`feat: jobs CLI + agent-tool parity`** (закрывает P1+P2) — read-only `jobs list` CLI-команда + `get_jobs` agent-tool поверх `JobsReadModel`; зеркалят `/jobs/api/list`. Размер S. Инвариант `CLAUDE.md:160`.
2. **`feat: providers list agent-tool`** (закрывает P3, минимум) — `list_providers` (READ) для паритета к `provider list` / web. Опционально write-tools под confirmation. Размер S.
3. **`feat: подключить ErrorRecoveryService`** (D3) — решение владельца уже принято (#1044: подключить, не удалять). Часть эпика «подключить неиспользуемое». Не parity до подключения.
4. **`feat: подключить ABTestingService`** (D4) — решение владельца уже принято (#1044: подключить, не удалять). После подключения → parity по 4 поверхностям (FastAPI узкий). Часть того же эпика.
5. **`decision: Production Limits — продуктовая фича или config-only`** (D2) — решение владельца: нужен ли `settings limits` + web-тумблер, или off-by-default config достаточно.

> P4 (server-control→web), P5 (test→web), P6 (messages-read→agent), D1 (mcp-server) — **рекомендация: оставить by design**, sub-issue не заводить без явного запроса владельца.

---

## Сводка

- **26 сервисов × 5 поверхностей** — матрица построена, ячейки верифицированы реальным кодом.
- **Настоящие пробелы parity: 3** (Jobs→CLI, Jobs→agent, Providers→agent) — нарушают инвариант `CLAUDE.md:160`, рекомендованы к закрытию (sub-issue 1–2).
- **Спорные: 3** (server-control, test, messages-read) — склоняюсь к «by design».
- **Не подключённые сервисы (не parity): ErrorRecovery + A/B** — владелец решил их **подключить** (#1044, эпик «подключить неиспользуемое»), не удалять; parity появится после подключения. Plus 1 config-only off-by-default (Prod Limits).
- **TUI и FastAPI** ведут себя как узкие поверхности корректно: TUI = только чат (1/26), FastAPI = interop + JSON-зеркала, без неоправданных пробелов.
- **Известные из #1022 пробелы верифицированы и расширены**: к Jobs/Providers/Limits добавлены server-control, test, messages-read, mcp-server; уточнён статус Export-web (JSON, не HTML) и Providers-web (JSON в settings-роутере).
