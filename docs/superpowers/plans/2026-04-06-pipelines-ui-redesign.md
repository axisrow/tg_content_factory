# Pipelines UI/UX Full Redesign

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete UI/UX overhaul of the pipelines section — unified dialog pickers with search, emoji action buttons, mobile cards, collapsible add form, always-available edit for all pipeline types.

**Architecture:** Replace raw `<select multiple>` with searchable multi-select pickers (reusing `photo_loader.html` pattern). Extract `pipeline_actions` macro into `_macros.html` with emoji buttons matching channels pattern. Add desktop/mobile dual-render. Move inline edit to a dedicated page `/pipelines/<id>/edit`.

**Tech Stack:** Jinja2, Bootstrap 5.3.3, vanilla JS, HTMX 2.0.4, existing `TGConfirm`, `data-busy`, `.emoji-btn` CSS

**Parent issue:** #343

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/web/static/js/searchable-picker.js` | **Create** | Reusable multi-select searchable picker component (vanilla JS) |
| `src/web/templates/_macros.html` | **Modify** | Add `pipeline_actions` macro |
| `src/web/templates/pipelines.html` | **Rewrite** | New list layout: collapsible add form, emoji buttons, mobile cards, searchable pickers |
| `src/web/templates/pipelines/edit.html` | **Create** | Dedicated edit page with searchable pickers |
| `src/web/routes/pipelines.py` | **Modify** | Add `GET /{id}/edit` route, update `templates_page` context |
| `src/web/templates/pipelines/templates.html` | **Modify** | Conditional modal fields (LLM vs non-LLM) |
| `src/web/static/style.css` | **Modify** | Add `.searchable-picker` styles |
| `tests/test_web.py` | **Modify** | Add tests for new edit route |

---

## Task 1: Searchable Multi-Select Picker Component

**Files:**
- Create: `src/web/static/js/searchable-picker.js`
- Modify: `src/web/static/style.css`

This component replaces all raw `<select multiple>` elements. It renders a search input + type filter buttons + scrollable checkbox list. Selected items appear as badges above the list. Communicates via hidden `<input>` elements for form submission.

### API Contract

HTML structure (server-rendered):
```html
<div class="searchable-picker" data-picker-name="source_channel_ids" data-picker-multi="true">
    <div class="picker-selected"></div>
    <input type="search" class="form-control form-control-sm" placeholder="Поиск..." data-picker-search>
    <div class="btn-group btn-group-sm mt-1 mb-2" data-picker-filters>
        <button type="button" class="btn btn-outline-secondary active" data-filter="all">Все</button>
        <!-- optional type filters -->
    </div>
    <div class="list-group picker-list" style="max-height:15rem;overflow-y:auto">
        <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
               data-picker-option
               data-value="123"
               data-group="channel"
               data-search-text="channel title username">
            <input type="checkbox" class="form-check-input mt-0">
            <span>Channel Title</span>
            <small class="text-muted ms-auto">channel</small>
        </label>
    </div>
    <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
</div>
```

JS auto-initializes all `[data-picker-name]` on DOMContentLoaded. Creates hidden `<input name="..." value="...">` per selected item inside the picker div for form submission.

- [ ] **Step 1: Write the picker JS**

Create `src/web/static/js/searchable-picker.js`:

```javascript
/**
 * Searchable multi-select picker.
 *
 * Usage: add [data-picker-name="field_name"] to a container div.
 * Options are <label data-picker-option data-value="..." data-group="..." data-search-text="...">
 * with an <input type="checkbox"> inside.
 */
(function () {
    "use strict";

    function initPicker(container) {
        var name = container.dataset.pickerName;
        var isMulti = container.dataset.pickerMulti !== "false";
        var searchInput = container.querySelector("[data-picker-search]");
        var options = Array.from(container.querySelectorAll("[data-picker-option]"));
        var filterButtons = Array.from(container.querySelectorAll("[data-filter]"));
        var emptyEl = container.querySelector("[data-picker-empty]");
        var selectedContainer = container.querySelector(".picker-selected");
        var activeFilter = "all";

        function getCheckbox(opt) {
            return opt.querySelector("input[type=checkbox]");
        }

        function syncHiddenInputs() {
            // Remove old hidden inputs
            container.querySelectorAll("input[type=hidden][data-picker-hidden]").forEach(function (el) {
                el.remove();
            });
            // Create new ones for selected values
            options.forEach(function (opt) {
                if (getCheckbox(opt).checked) {
                    var inp = document.createElement("input");
                    inp.type = "hidden";
                    inp.name = name;
                    inp.value = opt.dataset.value;
                    inp.setAttribute("data-picker-hidden", "");
                    container.appendChild(inp);
                }
            });
            renderBadges();
        }

        function renderBadges() {
            if (!selectedContainer) return;
            selectedContainer.innerHTML = "";
            options.forEach(function (opt) {
                if (!getCheckbox(opt).checked) return;
                var badge = document.createElement("span");
                badge.className = "badge bg-primary me-1 mb-1";
                badge.style.cursor = "pointer";
                badge.title = "Убрать";
                badge.textContent = opt.dataset.searchText.split(" ")[0] || opt.dataset.value;
                badge.addEventListener("click", function () {
                    getCheckbox(opt).checked = false;
                    syncHiddenInputs();
                });
                selectedContainer.appendChild(badge);
            });
        }

        function applyFilters() {
            var query = (searchInput ? searchInput.value : "").trim().toLowerCase();
            var visibleCount = 0;
            options.forEach(function (opt) {
                var matchesType = activeFilter === "all" || opt.dataset.group === activeFilter;
                var matchesText = !query || opt.dataset.searchText.indexOf(query) !== -1;
                var visible = matchesType && matchesText;
                opt.classList.toggle("d-none", !visible);
                if (visible) visibleCount += 1;
            });
            if (emptyEl) emptyEl.classList.toggle("d-none", visibleCount !== 0);
        }

        options.forEach(function (opt) {
            getCheckbox(opt).addEventListener("change", function () {
                if (!isMulti) {
                    // Single-select: uncheck others
                    options.forEach(function (other) {
                        if (other !== opt) getCheckbox(other).checked = false;
                    });
                }
                syncHiddenInputs();
            });
        });

        filterButtons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                activeFilter = btn.dataset.filter;
                filterButtons.forEach(function (b) {
                    b.classList.toggle("active", b === btn);
                });
                applyFilters();
            });
        });

        if (searchInput) searchInput.addEventListener("input", applyFilters);

        // Initial sync for pre-checked items
        syncHiddenInputs();
        applyFilters();
    }

    document.addEventListener("DOMContentLoaded", function () {
        document.querySelectorAll("[data-picker-name]").forEach(initPicker);
    });
})();
```

- [ ] **Step 2: Add picker CSS to `style.css`**

Append to `src/web/static/style.css`:

```css
/* Searchable multi-select picker */
.searchable-picker .picker-list {
    max-height: 15rem;
    overflow-y: auto;
}
.searchable-picker .picker-selected {
    min-height: 1.5rem;
    margin-bottom: 0.25rem;
}
.searchable-picker .picker-selected .badge {
    font-weight: 500;
    font-size: 0.8rem;
}
.searchable-picker .list-group-item {
    padding: 0.35rem 0.5rem;
    font-size: 0.875rem;
}
```

- [ ] **Step 3: Add `<script>` tag in `base.html`**

In `src/web/templates/base.html`, after the existing `_busy_button.js` script tag, add:

```html
<script src="/static/js/searchable-picker.js"></script>
```

- [ ] **Step 4: Verify picker renders standalone**

Create a minimal test HTML page manually in browser dev tools, confirm search filters and checkbox selection work. No automated test needed — this is pure client-side JS.

- [ ] **Step 5: Commit**

```bash
git add src/web/static/js/searchable-picker.js src/web/static/style.css src/web/templates/base.html
git commit -m "feat(web): add reusable searchable multi-select picker component"
```

---

## Task 2: `pipeline_actions` Macro

**Files:**
- Modify: `src/web/templates/_macros.html` (append after `channel_actions` macro, line 54)

Macro renders emoji action buttons for a pipeline row, following the exact pattern of `channel_actions`.

- [ ] **Step 1: Write the macro**

Append to `src/web/templates/_macros.html` after line 54:

```jinja2


{% macro pipeline_actions(pipeline, needs_llm=true, llm_configured=true, prefix='') %}
{% set llm_blocked = needs_llm and not llm_configured %}
{# Edit (always available) #}
<a href="/pipelines/{{ pipeline.id }}/edit" class="btn btn-outline-secondary btn-sm emoji-btn" title="Редактировать">&#9998;&#65039;</a>
{# Generate (only for LLM pipelines with provider) #}
{% if needs_llm and not llm_blocked %}
<a href="/pipelines/{{ pipeline.id }}/generate" class="btn btn-outline-primary btn-sm emoji-btn" title="Генерация">&#9889;</a>
{% elif needs_llm and llm_blocked %}
<span class="btn btn-outline-primary btn-sm emoji-btn disabled" aria-disabled="true" title="Настройте LLM провайдер">&#9889;</span>
{% endif %}
{# Run now #}
<form method="post" action="/pipelines/{{ pipeline.id }}/run" class="d-inline" data-busy>
    <button type="submit" class="btn btn-outline-success btn-sm emoji-btn" title="Запустить" {% if llm_blocked %}disabled{% endif %}>&#9654;&#65039;</button>
</form>
{# Toggle active #}
<form method="post" action="/pipelines/{{ pipeline.id }}/toggle" class="d-inline" data-busy>
    <button type="submit" class="btn btn-outline-secondary btn-sm emoji-btn" title="{{ 'Отключить' if pipeline.is_active else 'Включить' }}">
        {{ "\u23cf\ufe0f" if pipeline.is_active else "\u25b6\ufe0f" }}
    </button>
</form>
{# Export JSON #}
<a href="/pipelines/{{ pipeline.id }}/export" class="btn btn-outline-dark btn-sm emoji-btn" title="Экспорт JSON">&#128196;</a>
{# AI edit (only for DAG pipelines with LLM) #}
{% if pipeline.pipeline_json and llm_configured %}
<button type="button" class="btn btn-outline-info btn-sm emoji-btn" onclick="toggleAiEdit({{ pipeline.id }})" title="AI редактирование">&#129302;</button>
{% endif %}
{# Delete #}
<form method="post" action="/pipelines/{{ pipeline.id }}/delete" class="d-inline" data-busy data-confirm="Удалить pipeline «{{ pipeline.name }}»?">
    <button type="submit" class="btn btn-outline-danger btn-sm emoji-btn" title="Удалить">&#128465;&#65039;</button>
</form>
{% endmacro %}
```

Emoji mapping:
| Action | Emoji | Entity |
|--------|-------|--------|
| Edit | ✏️ | `&#9998;&#65039;` |
| Generate | ⚡ | `&#9889;` |
| Run | ▶️ | `&#9654;&#65039;` |
| Toggle on/off | ⏏️/▶️ | same as channels |
| Export JSON | 📄 | `&#128196;` |
| AI edit | 🤖 | `&#129302;` |
| Delete | 🗑️ | `&#128465;&#65039;` |

- [ ] **Step 2: Run lint**

```bash
ruff check src/web/templates/_macros.html 2>/dev/null; echo "Template — no ruff needed"
```

- [ ] **Step 3: Commit**

```bash
git add src/web/templates/_macros.html
git commit -m "feat(web): add pipeline_actions emoji macro"
```

---

## Task 3: Dedicated Edit Page

**Files:**
- Create: `src/web/templates/pipelines/edit.html`
- Modify: `src/web/routes/pipelines.py` — add `GET /{id}/edit` route (~line 254)

The edit page replaces inline edit rows. Every pipeline type (legacy chain AND DAG) gets an edit page. Source/target selectors use the new searchable picker.

- [ ] **Step 1: Write the edit page template**

Create `src/web/templates/pipelines/edit.html`:

```html
{% extends "base.html" %}
{% block title %}Редактировать — {{ pipeline.name }}{% endblock %}
{% block content %}
<div class="d-flex justify-content-between align-items-center mb-3">
    <h1 class="mb-0">Редактировать: {{ pipeline.name }}</h1>
    <a class="btn btn-secondary btn-sm" href="/pipelines">&#8592; Назад</a>
</div>

<form method="post" action="/pipelines/{{ pipeline.id }}/edit" data-busy>
    <div class="row g-3 mb-3">
        <div class="col-12 col-lg-6">
            <label class="form-label">Название</label>
            <input class="form-control" type="text" name="name" value="{{ pipeline.name }}" required>
        </div>
        <div class="col-6 col-md-3 col-lg-2">
            <label class="form-label">Backend</label>
            <select class="form-select" name="generation_backend">
                {% for backend in generation_backends %}
                <option value="{{ backend.value }}" {{ "selected" if backend == pipeline.generation_backend }}>{{ backend.value }}</option>
                {% endfor %}
            </select>
        </div>
        <div class="col-6 col-md-3 col-lg-2">
            <label class="form-label">Mode</label>
            <select class="form-select" name="publish_mode">
                {% for mode in publish_modes %}
                <option value="{{ mode.value }}" {{ "selected" if mode == pipeline.publish_mode }}>{{ mode.value }}</option>
                {% endfor %}
            </select>
        </div>
        <div class="col-6 col-md-3 col-lg-2">
            <label class="form-label">Интервал (мин)</label>
            <input class="form-control" type="number" name="generate_interval_minutes" min="1" value="{{ pipeline.generate_interval_minutes }}">
        </div>
    </div>

    <div class="row g-3 mb-3">
        <div class="col-12 col-md-6">
            <label class="form-label">LLM model</label>
            <input class="form-control" type="text" name="llm_model" value="{{ pipeline.llm_model or '' }}">
        </div>
        <div class="col-12 col-md-6">
            <label class="form-label">Image model</label>
            <input class="form-control" type="text" name="image_model" value="{{ pipeline.image_model or '' }}" list="image-model-list">
            <datalist id="image-model-list">
                <option value="together:black-forest-labs/FLUX.1-schnell">
                <option value="together:black-forest-labs/FLUX.1-dev">
                <option value="openai:dall-e-3">
                <option value="huggingface:stabilityai/stable-diffusion-xl-base-1.0">
                <option value="replicate:black-forest-labs/flux-schnell">
            </datalist>
        </div>
    </div>

    <div class="mb-3">
        <label class="form-label">Prompt template</label>
        <textarea class="form-control" name="prompt_template" rows="5" required>{{ pipeline.prompt_template }}</textarea>
        <div class="form-text">
            Доступные переменные:
            {% for variable in prompt_variables %}
            <code>{{ "{" }}{{ variable }}{{ "}" }}</code>{% if not loop.last %}, {% endif %}
            {% endfor %}
        </div>
    </div>

    {# Source channels — searchable picker #}
    <div class="row g-3 mb-3">
        <div class="col-12 col-lg-6">
            <label class="form-label">Каналы-источники</label>
            <div class="searchable-picker" data-picker-name="source_channel_ids" data-picker-multi="true">
                <div class="picker-selected"></div>
                <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск канала..." data-picker-search>
                <div class="list-group picker-list">
                    {% for channel in channels %}
                    <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                           data-picker-option
                           data-value="{{ channel.channel_id }}"
                           data-group="channel"
                           data-search-text="{{ ((channel.title or '') ~ ' ' ~ (channel.username or '') ~ ' ' ~ channel.channel_id)|lower }}">
                        <input type="checkbox" class="form-check-input mt-0"
                               {{ "checked" if channel.channel_id in source_ids }}>
                        <span>{{ channel.title or channel.channel_id }}{% if channel.username %} <small class="text-muted">@{{ channel.username }}</small>{% endif %}</span>
                    </label>
                    {% endfor %}
                </div>
                <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
            </div>
        </div>

        {# Target dialogs — searchable picker #}
        <div class="col-12 col-lg-6">
            <div class="d-flex justify-content-between align-items-center">
                <label class="form-label mb-0">Целевые диалоги</label>
                {% if accounts %}
                <a class="small" href="/pipelines/{{ pipeline.id }}/edit?refresh=1">Обновить кэш</a>
                {% endif %}
            </div>
            <div class="searchable-picker mt-1" data-picker-name="target_refs" data-picker-multi="true">
                <div class="picker-selected"></div>
                <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск диалога..." data-picker-search>
                <div class="btn-group btn-group-sm mb-1" data-picker-filters>
                    <button type="button" class="btn btn-outline-secondary active" data-filter="all">Все</button>
                    <button type="button" class="btn btn-outline-secondary" data-filter="channel">Каналы</button>
                    <button type="button" class="btn btn-outline-secondary" data-filter="group">Группы</button>
                    <button type="button" class="btn btn-outline-secondary" data-filter="dm">Личные</button>
                </div>
                <div class="list-group picker-list">
                    {% for account in accounts %}
                    {% set dialogs = cached_dialogs.get(account.phone, []) %}
                    {% for dialog in dialogs %}
                    {% set ref = account.phone ~ "|" ~ dialog.channel_id %}
                    {% set dtype = dialog.channel_type %}
                    {% set dgroup = "dm" if dtype == "dm" else ("channel" if dtype in ("channel", "monoforum") else "group") %}
                    <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                           data-picker-option
                           data-value="{{ ref }}"
                           data-group="{{ dgroup }}"
                           data-search-text="{{ ((dialog.title or '') ~ ' ' ~ dtype ~ ' ' ~ account.phone)|lower }}">
                        <input type="checkbox" class="form-check-input mt-0"
                               {{ "checked" if ref in target_refs }}>
                        <span>{{ dialog.title or dialog.channel_id }}</span>
                        <small class="text-muted ms-auto">{{ dtype }}</small>
                    </label>
                    {% endfor %}
                    {% endfor %}
                </div>
                <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
            </div>
        </div>
    </div>

    <div class="form-check mb-3">
        <input class="form-check-input" type="checkbox" name="is_active" value="true" id="edit-active" {{ "checked" if pipeline.is_active }}>
        <label class="form-check-label" for="edit-active">Pipeline активен</label>
    </div>

    <button type="submit" class="btn btn-primary me-2">Сохранить</button>
    <a href="/pipelines" class="btn btn-secondary">Отмена</a>
</form>
{% endblock %}
```

- [ ] **Step 2: Add the GET edit route**

In `src/web/routes/pipelines.py`, add before the `generate_page` route (before `@router.get("/{pipeline_id}/generate")`):

```python
@router.get("/{pipeline_id}/edit", response_class=HTMLResponse)
async def edit_page(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    channels = await deps.get_channel_bundle(request).list_channels(include_filtered=True)
    accounts = await deps.get_account_bundle(request).list_accounts()
    selected_phone = request.query_params.get("phone") or (accounts[0].phone if accounts else "")
    if selected_phone and request.query_params.get("refresh") == "1":
        try:
            await deps.channel_service(request).get_my_dialogs(selected_phone, refresh=True)
        except Exception:
            logger.warning("Failed to refresh dialog cache for %s", selected_phone, exc_info=True)
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    # Existing source/target IDs for pre-selection
    sources = await db.repos.pipeline_sources.list_by_pipeline(pipeline_id)
    targets = await db.repos.pipeline_targets.list_by_pipeline(pipeline_id)
    source_ids = [s.channel_id for s in sources]
    target_refs = [f"{t.phone}|{t.dialog_id}" for t in targets]
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/edit.html",
        {
            "pipeline": pipeline,
            "channels": channels,
            "accounts": accounts,
            "cached_dialogs": cached_dialogs,
            "source_ids": source_ids,
            "target_refs": target_refs,
            "prompt_variables": sorted(ALLOWED_TEMPLATE_VARIABLES),
            "generation_backends": list(PipelineGenerationBackend),
            "publish_modes": list(PipelinePublishMode),
        },
    )
```

- [ ] **Step 3: Write test for edit page route**

Add to `tests/test_web.py`:

```python
@pytest.mark.asyncio
async def test_pipeline_edit_page_loads(client_with_pipeline):
    """GET /pipelines/<id>/edit returns 200 with edit form."""
    resp = await client_with_pipeline.get("/pipelines/1/edit")
    assert resp.status_code == 200
    assert "Редактировать" in resp.text
```

Note: `client_with_pipeline` fixture may need to be created or adapted from existing pipeline test fixtures. Check existing pipeline test patterns in `tests/test_web.py` for the correct fixture name and setup.

- [ ] **Step 4: Run test**

```bash
pytest tests/test_web.py -v -k "test_pipeline_edit_page" -n auto
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/web/templates/pipelines/edit.html src/web/routes/pipelines.py tests/test_web.py
git commit -m "feat(web): add dedicated pipeline edit page with searchable pickers"
```

---

## Task 4: Rewrite Pipeline List — Emoji Buttons + Mobile Cards

**Files:**
- Rewrite: `src/web/templates/pipelines.html` (lines 155–325 — the list table and inline edit rows)

Replace text buttons with `pipeline_actions` macro. Remove inline edit rows (replaced by Task 3 edit page). Add mobile card layout.

- [ ] **Step 1: Replace the pipeline list table**

In `src/web/templates/pipelines.html`, replace the entire list section (from `{% if items %}` to the closing `</table></div>`) with:

```html
{% if items %}
{# Desktop table #}
<div class="table-responsive d-none d-md-block">
    <table class="table table-striped align-middle">
        <thead>
            <tr>
                <th>Название</th>
                <th>Источники</th>
                <th>Цели</th>
                <th>Интервал</th>
                <th></th>
            </tr>
        </thead>
        <tbody>
            {% for item in items %}
            {% set pipeline = item.pipeline %}
            {% set pipeline_needs_llm_val = needs_llm_map.get(pipeline.id, true) %}
            <tr id="pipeline-row-{{ pipeline.id }}">
                <td>
                    <div class="fw-semibold">{{ pipeline.name }}</div>
                    <small class="text-muted">
                        {{ pipeline.generation_backend.value }} / {{ pipeline.publish_mode.value }}
                        {% if not pipeline.is_active %} &middot; <span class="text-warning">inactive</span>{% endif %}
                    </small>
                </td>
                <td>
                    {% for source_title in item.source_titles[:3] %}
                    <div class="small">{{ source_title }}</div>
                    {% endfor %}
                    {% if item.source_titles|length > 3 %}
                    <div class="small text-muted">+{{ item.source_titles|length - 3 }}</div>
                    {% endif %}
                </td>
                <td>
                    {% for target in item.targets[:3] %}
                    <div class="small">{{ target.title or target.dialog_id }}</div>
                    {% endfor %}
                    {% if item.targets|length > 3 %}
                    <div class="small text-muted">+{{ item.targets|length - 3 }}</div>
                    {% endif %}
                </td>
                <td>
                    {{ pipeline.generate_interval_minutes }} мин
                    {% set nr = next_runs.get(pipeline.id) %}
                    {% if nr %}
                    <div class="small text-muted">{{ nr|local_dt }}</div>
                    {% endif %}
                </td>
                <td class="text-nowrap">
                    {{ pipeline_actions(pipeline, needs_llm=pipeline_needs_llm_val, llm_configured=llm_configured) }}
                </td>
            </tr>
            {# AI edit row (preserved for DAG pipelines) #}
            {% if pipeline.pipeline_json %}
            <tr id="pipeline-ai-edit-{{ pipeline.id }}" class="d-none">
                <td colspan="5">
                    <div class="p-3 border rounded bg-light">
                        <label class="form-label fw-semibold">AI-редактирование pipeline</label>
                        <pre class="border rounded p-2 bg-white small" style="max-height:300px;overflow:auto" id="pipeline-json-preview-{{ pipeline.id }}">Загрузка...</pre>
                        <div class="input-group mb-2">
                            <input class="form-control" type="text" id="ai-instruction-{{ pipeline.id }}" placeholder="Инструкция для AI">
                            <button class="btn btn-primary" type="button" onclick="sendAiEdit({{ pipeline.id }})">Применить</button>
                        </div>
                        <div id="ai-edit-result-{{ pipeline.id }}" class="text-muted small"></div>
                    </div>
                </td>
            </tr>
            {% endif %}
            {% endfor %}
        </tbody>
    </table>
</div>

{# Mobile cards #}
<div class="d-md-none mobile-cards">
    {% for item in items %}
    {% set pipeline = item.pipeline %}
    {% set pipeline_needs_llm_val = needs_llm_map.get(pipeline.id, true) %}
    <div class="card mb-2">
        <div class="card-body">
            <div class="d-flex justify-content-between align-items-start">
                <div class="min-w-0 flex-fill">
                    <div class="d-flex align-items-center gap-1 mb-1">
                        {% if pipeline.is_active %}<span class="badge-active">&#9679;</span>
                        {% else %}<span class="badge-inactive">&#9679;</span>{% endif %}
                        <strong class="text-truncate">{{ pipeline.name }}</strong>
                    </div>
                    <small class="text-muted">
                        {{ pipeline.generation_backend.value }} / {{ pipeline.publish_mode.value }}
                        &middot; {{ pipeline.generate_interval_minutes }} мин
                    </small>
                    {% if item.source_titles %}
                    <div class="small mt-1">
                        Источники: {{ item.source_titles[:2]|join(", ") }}{% if item.source_titles|length > 2 %} +{{ item.source_titles|length - 2 }}{% endif %}
                    </div>
                    {% endif %}
                    {% if item.targets %}
                    <div class="small">
                        Цели: {{ item.targets[:2]|map(attribute="title")|join(", ") }}{% if item.targets|length > 2 %} +{{ item.targets|length - 2 }}{% endif %}
                    </div>
                    {% endif %}
                </div>
            </div>
            <div class="card-actions">
                {{ pipeline_actions(pipeline, needs_llm=pipeline_needs_llm_val, llm_configured=llm_configured, prefix="m-") }}
            </div>
        </div>
    </div>
    {% endfor %}
</div>
{% else %}
<p>Нет созданных пайплайнов. <a href="/pipelines/templates">Создайте из шаблона</a> или заполните форму выше.</p>
{% endif %}
```

Key changes:
- Removed columns: ID (internal detail), Backend, Mode (moved to subtitle under name)
- Sources/Targets show max 3 items with "+N" overflow
- Removed inline edit rows (legacy chain and DAG) — replaced by `/pipelines/{id}/edit`
- `pipeline_actions` macro replaces all text buttons
- AI edit row preserved as inline toggle
- Added complete mobile card layout
- Table `colspan` changed from 8 to 5

- [ ] **Step 2: Remove `togglePipelineEdit` JS function**

Remove the `togglePipelineEdit` function from the inline `<script>` block at the bottom of `pipelines.html` (it's no longer needed — edit is a separate page now). Keep `toggleAiEdit`, `loadPipelineJson`, and `sendAiEdit`.

- [ ] **Step 3: Verify page renders**

```bash
python -m pytest tests/test_web.py -v -k "test_pipelines_page" -n auto
```

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/pipelines.html
git commit -m "feat(web): pipeline list with emoji buttons and mobile cards"
```

---

## Task 5: Collapsible Add Form with Searchable Pickers

**Files:**
- Modify: `src/web/templates/pipelines.html` (lines 57–153 — the add form)

Replace raw `<select multiple>` with searchable pickers. Make the form collapsible.

- [ ] **Step 1: Rewrite the add form section**

Replace lines 57–153 in `src/web/templates/pipelines.html`:

```html
<div class="card mb-4">
    <div class="card-header fw-semibold d-flex justify-content-between align-items-center"
         data-bs-toggle="collapse" data-bs-target="#add-pipeline-form" style="cursor:pointer">
        Новый pipeline
        <i class="bi bi-chevron-down"></i>
    </div>
    <div class="collapse" id="add-pipeline-form">
        <div class="card-body">
            <form method="post" action="/pipelines/add{% if selected_phone %}?phone={{ selected_phone|urlencode }}{% endif %}" data-busy>
                <div class="row g-3 mb-3">
                    <div class="col-12 col-lg-6">
                        <label class="form-label">Название</label>
                        <input class="form-control" type="text" name="name" required placeholder="Daily digest">
                    </div>
                    <div class="col-6 col-md-4 col-lg-3">
                        <label class="form-label">Backend</label>
                        <select class="form-select" name="generation_backend">
                            {% for backend in generation_backends %}
                            <option value="{{ backend.value }}">{{ backend.value }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="col-6 col-md-4 col-lg-3">
                        <label class="form-label">Publish mode</label>
                        <select class="form-select" name="publish_mode">
                            {% for mode in publish_modes %}
                            <option value="{{ mode.value }}">{{ mode.value }}</option>
                            {% endfor %}
                        </select>
                    </div>
                </div>
                <div class="row g-3 mb-3">
                    <div class="col-6 col-md-3">
                        <label class="form-label">Интервал (мин)</label>
                        <input class="form-control" type="number" name="generate_interval_minutes" min="1" value="60">
                    </div>
                    <div class="col-6 col-md-4">
                        <label class="form-label">LLM model</label>
                        <input class="form-control" type="text" name="llm_model" placeholder="claude-sonnet-4.5">
                    </div>
                    <div class="col-12 col-md-5">
                        <label class="form-label">Image model</label>
                        <input class="form-control" type="text" name="image_model" placeholder="optional" list="image-model-list">
                        <datalist id="image-model-list">
                            <option value="together:black-forest-labs/FLUX.1-schnell">
                            <option value="together:black-forest-labs/FLUX.1-dev">
                            <option value="openai:dall-e-3">
                            <option value="huggingface:stabilityai/stable-diffusion-xl-base-1.0">
                            <option value="replicate:black-forest-labs/flux-schnell">
                        </datalist>
                    </div>
                </div>

                <div class="mb-3">
                    <label class="form-label">Prompt template</label>
                    <textarea class="form-control" name="prompt_template" rows="5" required placeholder="Собери пост на основе {source_messages}"></textarea>
                    <div class="form-text">
                        Доступные переменные:
                        {% for variable in prompt_variables %}
                        <code>{{ "{" }}{{ variable }}{{ "}" }}</code>{% if not loop.last %}, {% endif %}
                        {% endfor %}
                    </div>
                </div>

                {# Source channels — searchable picker #}
                <div class="row g-3 mb-3">
                    <div class="col-12 col-lg-6">
                        <label class="form-label">Каналы-источники</label>
                        <div class="searchable-picker" data-picker-name="source_channel_ids" data-picker-multi="true">
                            <div class="picker-selected"></div>
                            <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск канала..." data-picker-search>
                            <div class="list-group picker-list">
                                {% for channel in channels %}
                                <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                                       data-picker-option
                                       data-value="{{ channel.channel_id }}"
                                       data-group="channel"
                                       data-search-text="{{ ((channel.title or '') ~ ' ' ~ (channel.username or '') ~ ' ' ~ channel.channel_id)|lower }}">
                                    <input type="checkbox" class="form-check-input mt-0">
                                    <span>{{ channel.title or channel.channel_id }}{% if channel.username %} <small class="text-muted">@{{ channel.username }}</small>{% endif %}</span>
                                </label>
                                {% endfor %}
                            </div>
                            <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
                        </div>
                    </div>

                    {# Target dialogs — searchable picker #}
                    <div class="col-12 col-lg-6">
                        <div class="d-flex justify-content-between align-items-center">
                            <label class="form-label mb-0">Целевые диалоги</label>
                            {% if selected_phone %}
                            <a class="small" href="/pipelines?phone={{ selected_phone|urlencode }}&refresh=1">Обновить кэш</a>
                            {% endif %}
                        </div>
                        <div class="searchable-picker mt-1" data-picker-name="target_refs" data-picker-multi="true">
                            <div class="picker-selected"></div>
                            <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск диалога..." data-picker-search>
                            <div class="btn-group btn-group-sm mb-1" data-picker-filters>
                                <button type="button" class="btn btn-outline-secondary active" data-filter="all">Все</button>
                                <button type="button" class="btn btn-outline-secondary" data-filter="channel">Каналы</button>
                                <button type="button" class="btn btn-outline-secondary" data-filter="group">Группы</button>
                                <button type="button" class="btn btn-outline-secondary" data-filter="dm">Личные</button>
                            </div>
                            <div class="list-group picker-list">
                                {% for account in accounts %}
                                {% set dialogs = cached_dialogs.get(account.phone, []) %}
                                {% for dialog in dialogs %}
                                {% set ref = account.phone ~ "|" ~ dialog.channel_id %}
                                {% set dtype = dialog.channel_type %}
                                {% set dgroup = "dm" if dtype == "dm" else ("channel" if dtype in ("channel", "monoforum") else "group") %}
                                <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                                       data-picker-option
                                       data-value="{{ ref }}"
                                       data-group="{{ dgroup }}"
                                       data-search-text="{{ ((dialog.title or '') ~ ' ' ~ dtype ~ ' ' ~ account.phone)|lower }}">
                                    <input type="checkbox" class="form-check-input mt-0">
                                    <span>{{ dialog.title or dialog.channel_id }}</span>
                                    <small class="text-muted ms-auto">{{ dtype }}</small>
                                </label>
                                {% endfor %}
                                {% endfor %}
                            </div>
                            <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
                        </div>
                    </div>
                </div>

                <div class="form-check mb-3">
                    <input class="form-check-input" type="checkbox" name="is_active" value="true" id="pipeline-active" checked>
                    <label class="form-check-label" for="pipeline-active">Pipeline активен</label>
                </div>
                <button type="submit" class="btn btn-primary">Создать pipeline</button>
            </form>
        </div>
    </div>
</div>
```

Key changes:
- Card header has `data-bs-toggle="collapse"` + chevron icon — form starts collapsed
- Source channels: `<select multiple>` replaced with searchable picker
- Target dialogs: `<select multiple>` replaced with searchable picker with type filter buttons
- All dialog types unified — no `optgroup` separation, just search + filter buttons

- [ ] **Step 2: Run tests**

```bash
pytest tests/test_web.py -v -k "pipeline" -n auto
```

- [ ] **Step 3: Commit**

```bash
git add src/web/templates/pipelines.html
git commit -m "feat(web): collapsible add form with searchable source/target pickers"
```

---

## Task 6: Templates Page — Conditional Modal Fields

**Files:**
- Modify: `src/web/templates/pipelines/templates.html` (lines 44–75 — modal)
- Modify: `src/web/routes/pipelines.py` — `templates_page()` (line 431)

For non-LLM templates, remove LLM model field and show source/target/account fields instead.

- [ ] **Step 1: Update `templates_page` route to pass channels/accounts**

In `src/web/routes/pipelines.py`, update the `templates_page` handler:

```python
@router.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request):
    svc: PipelineService = deps.pipeline_service(request)
    templates = await svc.list_templates()
    channels = await deps.get_channel_bundle(request).list_channels(include_filtered=True)
    accounts = await deps.get_account_bundle(request).list_accounts()
    selected_phone = request.query_params.get("phone") or (accounts[0].phone if accounts else "")
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    llm_configured = deps.get_llm_provider_service(request).has_providers()
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/templates.html",
        {
            "templates": templates,
            "channels": channels,
            "accounts": accounts,
            "cached_dialogs": cached_dialogs,
            "llm_configured": llm_configured,
        },
    )
```

- [ ] **Step 2: Update the modal in `templates.html`**

Replace the modal body (lines 53–66 inside the form) with conditional rendering:

```html
<div class="modal-body">
    <div class="mb-3">
        <label class="form-label">Название</label>
        <input class="form-control" type="text" name="name" value="{{ tpl.name }}" required>
    </div>

    {% set uses_llm = tpl.template_json.nodes | selectattr("type.value", "in", ["llm_generate", "llm_refine"]) | list %}
    {% set has_forward = tpl.template_json.nodes | selectattr("type.value", "equalto", "forward") | list %}
    {% set has_publish = tpl.template_json.nodes | selectattr("type.value", "equalto", "publish") | list %}

    {% if uses_llm %}
    <div class="mb-3">
        <label class="form-label">LLM модель</label>
        <input class="form-control" type="text" name="llm_model" placeholder="claude-sonnet-4.5">
        {% if not llm_configured %}
        <div class="form-text text-warning">LLM-провайдер не настроен. <a href="/settings">Настройки</a></div>
        {% endif %}
    </div>
    {% endif %}

    <div class="mb-3">
        <label class="form-label">Интервал (мин)</label>
        <input class="form-control" type="number" name="generate_interval_minutes" min="1" value="60">
    </div>

    {# Source channels — always relevant #}
    <div class="mb-3">
        <label class="form-label">Каналы-источники</label>
        <div class="searchable-picker" data-picker-name="source_channel_ids" data-picker-multi="true">
            <div class="picker-selected"></div>
            <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск канала..." data-picker-search>
            <div class="list-group picker-list">
                {% for channel in channels %}
                <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                       data-picker-option
                       data-value="{{ channel.channel_id }}"
                       data-group="channel"
                       data-search-text="{{ ((channel.title or '') ~ ' ' ~ (channel.username or '') ~ ' ' ~ channel.channel_id)|lower }}">
                    <input type="checkbox" class="form-check-input mt-0">
                    <span>{{ channel.title or channel.channel_id }}{% if channel.username %} <small class="text-muted">@{{ channel.username }}</small>{% endif %}</span>
                </label>
                {% endfor %}
            </div>
            <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
        </div>
    </div>

    {% if has_forward or has_publish %}
    {# Target dialogs — only for forward/publish templates #}
    <div class="mb-3">
        <label class="form-label">Целевые диалоги</label>
        <div class="searchable-picker" data-picker-name="target_refs" data-picker-multi="true">
            <div class="picker-selected"></div>
            <input type="search" class="form-control form-control-sm mb-1" placeholder="Поиск диалога..." data-picker-search>
            <div class="btn-group btn-group-sm mb-1" data-picker-filters>
                <button type="button" class="btn btn-outline-secondary active" data-filter="all">Все</button>
                <button type="button" class="btn btn-outline-secondary" data-filter="channel">Каналы</button>
                <button type="button" class="btn btn-outline-secondary" data-filter="group">Группы</button>
                <button type="button" class="btn btn-outline-secondary" data-filter="dm">Личные</button>
            </div>
            <div class="list-group picker-list">
                {% for account in accounts %}
                {% set dialogs = cached_dialogs.get(account.phone, []) %}
                {% for dialog in dialogs %}
                {% set ref = account.phone ~ "|" ~ dialog.channel_id %}
                {% set dtype = dialog.channel_type %}
                {% set dgroup = "dm" if dtype == "dm" else ("channel" if dtype in ("channel", "monoforum") else "group") %}
                <label class="list-group-item list-group-item-action d-flex align-items-center gap-2"
                       data-picker-option
                       data-value="{{ ref }}"
                       data-group="{{ dgroup }}"
                       data-search-text="{{ ((dialog.title or '') ~ ' ' ~ dtype ~ ' ' ~ account.phone)|lower }}">
                    <input type="checkbox" class="form-check-input mt-0">
                    <span>{{ dialog.title or dialog.channel_id }}</span>
                    <small class="text-muted ms-auto">{{ dtype }}</small>
                </label>
                {% endfor %}
                {% endfor %}
            </div>
            <small class="text-muted d-none" data-picker-empty>Ничего не найдено</small>
        </div>
    </div>
    {% endif %}
</div>
```

Note: The modal size should be increased to `modal-lg` (line 45 of templates.html — change `<div class="modal-dialog">` to `<div class="modal-dialog modal-lg">`) to accommodate the picker lists.

- [ ] **Step 3: Run tests**

```bash
pytest tests/test_web.py -v -k "template" -n auto
```

- [ ] **Step 4: Commit**

```bash
git add src/web/templates/pipelines/templates.html src/web/routes/pipelines.py
git commit -m "feat(web): conditional template modal with searchable pickers"
```

---

## Task 7: Redirect `from-template` to Edit Instead of Generate

**Files:**
- Modify: `src/web/routes/pipelines.py` — `create_from_template()` (line 490)

Currently redirects to `/pipelines/{id}/generate` which doesn't make sense for non-LLM pipelines.

- [ ] **Step 1: Change redirect target**

In `src/web/routes/pipelines.py`, change line 490:

```python
# Before:
return RedirectResponse(url=f"/pipelines/{pipeline_id}/generate", status_code=303)

# After:
return RedirectResponse(url=f"/pipelines/{pipeline_id}/edit", status_code=303)
```

- [ ] **Step 2: Run tests**

```bash
pytest tests/test_web.py -v -k "template" -n auto
```

- [ ] **Step 3: Commit**

```bash
git add src/web/routes/pipelines.py
git commit -m "fix(web): redirect from-template to edit page instead of generate"
```

---

## Task 8: Localize Table Headers and Clean Up

**Files:**
- Modify: `src/web/templates/pipelines.html` — page header, localization

- [ ] **Step 1: Localize page header and remaining English text**

In `src/web/templates/pipelines.html`:

- Page title `<h1>`: keep as "Пайплайны" (already has Russian title)
- Header buttons: localize "Импорт JSON" → keep (already Russian), "Шаблоны" → keep
- Table headers: already changed in Task 4 to "Название", "Источники", "Цели", "Интервал"
- Form labels: localize "Backend" → "Бэкенд", "Publish mode" → "Режим публикации", "Prompt template" → "Шаблон промпта"
- Add form submit: "Создать pipeline" → "Создать пайплайн"
- Edit page: check all labels match the add form

- [ ] **Step 2: Commit**

```bash
git add src/web/templates/pipelines.html src/web/templates/pipelines/edit.html
git commit -m "fix(web): localize pipeline form labels to Russian"
```

---

## Task 9: Full Verification

- [ ] **Step 1: Lint**

```bash
ruff check src/web/routes/pipelines.py
```

- [ ] **Step 2: Run all pipeline tests**

```bash
pytest tests/test_web.py -v -k "pipeline" -n auto
```

- [ ] **Step 3: Run full test suite**

```bash
pytest tests/ -v -m "not aiosqlite_serial" -n auto
```

- [ ] **Step 4: Manual browser verification**

Open in browser:
1. `GET /pipelines` — list loads, emoji buttons visible, mobile cards work on narrow screen, add form collapsed
2. Click ✏️ on any pipeline → `/pipelines/{id}/edit` — edit page with searchable pickers
3. Search for a channel in source picker — type-ahead filters
4. Search for a dialog in target picker — type filter buttons work
5. `GET /pipelines/templates` — click "Использовать" on a non-LLM template → no LLM model field, source picker shown
6. Click "Использовать" on an LLM template → LLM model field shown

- [ ] **Step 5: Commit any fixes**

```bash
git add -A
git commit -m "fix(web): address pipeline redesign review feedback"
```
