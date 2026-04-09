# Pipeline Redesign Spec

**Issue:** #403 — [GLOBAL BUG] Третий редизайн пайплайнов
**Date:** 2026-04-09
**Status:** Draft

## Problem

Интерфейс пайплайнов неконсистентен: смешаны три системы иконок (emoji, Unicode, Bootstrap Icons), кнопки и статусы выглядят по-разному на разных страницах, форма создания — громоздкий collapsible, мобильная версия неудобна. Юзер не может зайти в пайплайны и создать новый из-за «сырого» UI.

Параллельно нужно покрыть три категории автоматизации через единый интерфейс пайплайнов: простые действия, LLM-действия, агентная автоматизация.

## Design Decisions

1. **Все типы пайплайнов равноправны** — нет иерархии уровней, есть только порядок реализации
2. **Шаблоны — основной способ создания** — юзер выбирает преднастроенный шаблон (auto-react, forward, cleanup и т.д.)
3. **Bootstrap Icons — единая система иконок** — убрать emoji и Unicode-символы
4. **Legacy-пайплайны** продолжают работать, новые создаются через DAG
5. **Context-aware формы** — показывать только поля, релевантные выбранному типу пайплайна

## Implementation Priorities

### Priority 1: UI Redesign

**Goal:** Унифицировать визуальную систему пайплайнов, чтобы юзер мог интуитивно создавать и управлять пайплайнами.

**Changes:**

1. **Icon unification** (`src/web/templates/_macros.html`)
   - Replace all emoji icons with Bootstrap Icons equivalents
   - Map: `edit` → `bi-pencil`, `delete` → `bi-trash`, `toggle_on` → `bi-pause-circle`, `toggle_off` → `bi-play-circle`, `run` → `bi-play-fill`, `test` → `bi-lightning`, etc.

2. **Pipeline list redesign** (`src/web/templates/pipelines.html`)
   - Replace desktop table + mobile cards with unified card grid
   - Each card: type icon (Bootstrap Icon), name, status badge, source/target summary, quick actions
   - Consistent status indicators: `bi-circle-fill` (green) for active, `bi-circle` (gray) for inactive

3. **Wizard improvements** (`src/web/templates/pipelines/create.html`)
   - Clearer step indicators with Bootstrap Icons
   - Better mobile responsive layout
   - Template cards with type icons instead of emoji

4. **Edit page** (`src/web/templates/pipelines/edit.html`)
   - Context-aware fields: show only relevant configuration for pipeline type
   - Consistent button styling with Bootstrap Icons

5. **Remove Unicode escapes**
   - Replace `&#x1F3AF;`, `&#x25BA;`, etc. with `<i class="bi-*">` throughout pipeline templates

**Files affected:**
- `src/web/templates/_macros.html` — icon macro overhaul
- `src/web/templates/pipelines.html` — list page
- `src/web/templates/pipelines/create.html` — wizard
- `src/web/templates/pipelines/edit.html` — editor
- `src/web/templates/pipelines/generate.html` — generation page
- `src/web/templates/pipelines/templates.html` — template selection

### Priority 2: Simple Actions (no LLM)

**Goal:** Реализовать «простые действия» — автоматизация без LLM.

**Pipeline types:**

| Type | Flow | Config |
|------|------|--------|
| Forward | SOURCE → FORWARD | target channel |
| React | SOURCE → [DELAY] → REACT | emoji list, delay seconds |
| Delete | SOURCE → FILTER → DELETE_MESSAGE | filter: service_message, anonymous_sender |
| Cleanup | SOURCE → FILTER → DELETE_MESSAGE | preconfigured for join/leave |

**Implementation:**
- All use existing DAG node types (SOURCE, FORWARD, REACT, FILTER, DELETE_MESSAGE, DELAY)
- Pre-configured templates with sensible defaults
- Template picker in wizard shows these as "quick start" options
- No LLM provider required — hide LLM config fields

**Files affected:**
- `src/web/templates/pipelines/create.html` — template cards for simple actions
- `src/web/templates/pipelines/templates.html` — template definitions
- `src/services/pipeline_service.py` — template registration
- Backend node handlers already exist

### Priority 3: LLM Actions

**Goal:** Пайплайны с LLM — рерайтинг, суммаризация, перевод, генерация контента.

**Pipeline types:**
- Rewrite: SOURCE → LLM_GENERATE (with rewrite prompt) → PUBLISH
- Summarize: SOURCE → RETRIEVE_CONTEXT → LLM_GENERATE (summarize prompt) → PUBLISH
- Translate: SOURCE → LLM_GENERATE (translate prompt + target language) → PUBLISH
- Content generation: SOURCE → RETRIEVE_CONTEXT → LLM_GENERATE → [IMAGE_GENERATE] → PUBLISH

**Implementation:**
- Templates with pre-configured prompts
- LLM model selector in template config
- Requires at least one LLM provider configured
- Show warning if no provider available

**Files affected:**
- Templates and service layer extensions
- Provider status checks in wizard

### Priority 4: Agent Automation

**Goal:** Интеграция с Claude Agent SDK для агентной автоматизации.

**Implementation:**
- New AGENT_LOOP node type
- Integration with existing `AgentProviderService`
- Agent loop: receives message context, has access to tools, returns action
- Configuration: agent backend, system prompt, tool permissions

**Files affected:**
- `src/services/pipeline_executor.py` — new node handler
- `src/agent/` — agent loop integration
- `src/database/schema.py` — agent node config fields

## Testing Strategy

- Each priority has its own test suite
- Priority 1: visual regression via template snapshots
- Priority 2-4: integration tests with DAG executor
- No changes to existing legacy pipeline tests

## Migration & Compatibility

- Legacy pipelines continue to work unchanged
- New pipelines default to DAG mode
- UI shows clear distinction between legacy and DAG pipelines
- No database schema changes for Priority 1-2
