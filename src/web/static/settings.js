/* Settings page — agent provider management */
const providerProbeTokens = {};
let bulkTestPollTimer = null;
const htmlEscapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
};

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => htmlEscapeMap[char]);
}

function compatibilityBadgeClass(status) {
    if (status === 'supported') return 'success';
    if (status === 'unsupported') return 'danger';
    return 'warning';
}

function renderCompatibilityHtml(payload) {
    if (!payload || !payload.status) {
        return '<div class="text-muted">Совместимость модели с deepagents ещё не проверялась.</div>';
    }
    const lines = [
        'compatibility: <span class="badge text-bg-' + compatibilityBadgeClass(payload.status) + '">' + payload.status + '</span>'
    ];
    if (payload.tested_at) {
        lines.push('| ' + escapeHtml(payload.tested_at));
    }
    let html = '<div>' + lines.join(' ') + '</div>';
    if (payload.reason) {
        html += '<div class="text-muted">' + escapeHtml(payload.reason) + '</div>';
    }
    return html;
}

function renderModelMetaHtml(source, fetchedAt) {
    const badgeClass = source === 'live' ? 'success' : 'secondary';
    let html = 'source: <span class="badge text-bg-' + badgeClass + '">' + escapeHtml(source || 'static cache') + '</span>';
    if (fetchedAt) {
        html += ' | ' + escapeHtml(fetchedAt);
    }
    return html;
}

function applyRefreshedProviderState(provider, payload) {
    const select = document.getElementById('provider_model__' + provider);
    const compatBox = document.getElementById('provider_compat__' + provider);
    const metaBox = document.getElementById('provider_model_meta__' + provider);
    const errorBox = document.getElementById('provider_model_error__' + provider);
    const currentModel = select ? select.value : '';
    const compatibility = payload.compatibility || {};
    const models = Array.isArray(payload.models) ? payload.models.slice() : [];

    if (currentModel && !models.includes(currentModel)) {
        models.unshift(currentModel);
    }

    if (select) {
        select.innerHTML = '';
        models.forEach((model) => {
            const compatPayload = compatibility[model] || {};
            const option = document.createElement('option');
            option.value = model;
            option.dataset.compatStatus = compatPayload.status || '';
            option.textContent = model + (compatPayload.status ? ' [' + compatPayload.status + ']' : '');
            select.appendChild(option);
        });
        if (currentModel && models.includes(currentModel)) {
            select.value = currentModel;
        }
    }

    if (metaBox) {
        metaBox.innerHTML = renderModelMetaHtml(payload.source, payload.fetched_at);
    }
    if (errorBox) {
        if (payload.error) {
            errorBox.textContent = payload.error;
            errorBox.classList.remove('d-none');
        } else {
            errorBox.textContent = '';
            errorBox.classList.add('d-none');
        }
    }
    if (compatBox && select) {
        compatBox.innerHTML = renderCompatibilityHtml(compatibility[select.value] || {});
    }
}

function updateModelOptionLabel(provider, model, status) {
    const select = document.getElementById('provider_model__' + provider);
    if (!select) return;
    const option = Array.from(select.options).find((item) => item.value === model);
    if (!option) return;
    option.dataset.compatStatus = status || '';
    option.textContent = model + (status ? ' [' + status + ']' : '');
}

function setAsyncButtonBusy(button, busy, busyLabel) {
    if (!button) return;
    if (!button.dataset.defaultLabel) {
        button.dataset.defaultLabel = button.innerHTML;
    }
    if (busy) {
        button.disabled = true;
        button.innerHTML =
            '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>' +
            (busyLabel || 'Выполняется...');
        return;
    }
    button.disabled = false;
    button.innerHTML = button.dataset.defaultLabel;
}

function setAgentProviderActionsStatus(level, html) {
    const box = document.getElementById('agent-provider-actions-status');
    if (!box) return;
    if (!html) {
        box.className = 'small mb-3';
        box.innerHTML = '';
        return;
    }
    box.className = 'alert alert-' + level + ' py-2 mb-3';
    box.innerHTML = html;
}

function renderBulkTestProgress(status) {
    const summary = status.summary || {};
    const events = Array.isArray(status.recent_events) ? status.recent_events : [];
    const lines = [];
    if (status.running) {
        lines.push(
            '<div><strong>Идёт тестирование.</strong> ' +
            'Проверено ' + (status.completed_probes || 0) + ' из ' + (status.total_probes || 0) + '.</div>'
        );
        if (status.current_provider || status.current_model) {
            lines.push(
                '<div class="small">Сейчас: <code>' +
                escapeHtml(status.current_provider || '—') + '</code> / <code>' +
                escapeHtml(status.current_model || '—') + '</code></div>'
            );
        }
    } else if (status.finished_at) {
        lines.push('<div><strong>Тестирование завершено.</strong></div>');
    }
    lines.push(
        '<div class="small">supported=' + (summary.supported || 0) +
        ', unsupported=' + (summary.unsupported || 0) +
        ', unknown=' + (summary.unknown || 0) + '</div>'
    );
    if (status.catalog_path) {
        lines.push('<div class="small">Каталог: <code>' + escapeHtml(status.catalog_path) + '</code></div>');
    }
    if (status.error) {
        lines.push('<div class="small text-danger">' + escapeHtml(status.error) + '</div>');
    }
    if (events.length) {
        lines.push('<div class="small mt-2">' + events.slice(-4).map((item) => escapeHtml(item)).join('<br>') + '</div>');
    }
    return lines.join('');
}

function applyBulkTestResults(status) {
    Object.entries(status.providers || {}).forEach(([provider, providerPayload]) => {
        const select = document.getElementById('provider_model__' + provider);
        const currentModel = select ? select.value : '';
        (providerPayload.models || []).forEach((item) => {
            updateModelOptionLabel(provider, item.model, item.status);
            if (currentModel && currentModel === item.model) {
                const compatBox = document.getElementById('provider_compat__' + provider);
                if (compatBox) {
                    compatBox.innerHTML = renderCompatibilityHtml(item);
                }
            }
        });
    });
}

async function pollBulkTestStatus() {
    try {
        const resp = await fetch('/settings/agent-providers/test-all/status');
        const payload = await resp.json().catch(() => ({}));
        if (!resp.ok || !payload.ok) {
            setAgentProviderActionsStatus('danger', escapeHtml(payload.error || 'Не удалось получить статус тестирования'));
            clearBulkTestPolling();
            setAsyncButtonBusy(document.getElementById('bulk-test-agent-providers-btn'), false);
            return;
        }
        applyBulkTestResults(payload);
        setAgentProviderActionsStatus(payload.error ? 'danger' : (payload.running ? 'secondary' : 'success'), renderBulkTestProgress(payload));
        if (!payload.running) {
            clearBulkTestPolling();
            setAsyncButtonBusy(document.getElementById('bulk-test-agent-providers-btn'), false);
        }
    } catch (err) {
        setAgentProviderActionsStatus('danger', 'Ошибка получения статуса массового тестирования: ' + escapeHtml(err.message));
        clearBulkTestPolling();
        setAsyncButtonBusy(document.getElementById('bulk-test-agent-providers-btn'), false);
    }
}

function clearBulkTestPolling() {
    if (bulkTestPollTimer) {
        clearInterval(bulkTestPollTimer);
        bulkTestPollTimer = null;
    }
}

function startBulkTestPolling() {
    clearBulkTestPolling();
    pollBulkTestStatus();
    bulkTestPollTimer = setInterval(pollBulkTestStatus, 1500);
}

function appendProviderFields(data, root) {
    if (!root) {
        return data;
    }
    root.querySelectorAll('input, select, textarea').forEach((field) => {
        if (!field.name || field.disabled) {
            return;
        }
        if ((field.type === 'checkbox' || field.type === 'radio') && !field.checked) {
            return;
        }
        data.append(field.name, field.value);
    });
    return data;
}

function buildProviderFormData(provider) {
    const card = document.querySelector('[data-provider-card="' + provider + '"]');
    return appendProviderFields(new FormData(), card);
}

function buildAllProviderFormData() {
    const data = new FormData();
    document.querySelectorAll('[data-provider-card]').forEach((card) => {
        appendProviderFields(data, card);
    });
    return data;
}

async function probeProviderModel(provider) {
    const compatBox = document.getElementById('provider_compat__' + provider);
    if (!compatBox) {
        return;
    }
    const token = Date.now() + Math.random();
    providerProbeTokens[provider] = token;
    compatBox.innerHTML = '<div class="text-muted">Проверяю совместимость модели\u2026</div>';

    const resp = await fetch('/settings/agent-providers/' + encodeURIComponent(provider) + '/probe', {
        method: 'POST',
        body: buildProviderFormData(provider),
    });
    const payload = await resp.json().catch(() => ({}));
    if (providerProbeTokens[provider] !== token) {
        return;
    }
    if (!resp.ok || !payload.ok) {
        compatBox.innerHTML = '<div class="text-danger">' + escapeHtml(payload.error || 'Не удалось проверить совместимость модели') + '</div>';
        return;
    }
    compatBox.innerHTML = renderCompatibilityHtml(payload);
    updateModelOptionLabel(provider, payload.model, payload.status);
}

async function refreshAgentProvider(provider) {
    const button = document.querySelector('[data-provider-refresh-btn="' + provider + '"]');
    setAsyncButtonBusy(button, true, 'Обновляю...');
    try {
        const resp = await fetch('/settings/agent-providers/' + encodeURIComponent(provider) + '/refresh', {
            method: 'POST',
            body: buildProviderFormData(provider),
        });
        const payload = await resp.json().catch(() => ({}));
        if (!resp.ok || payload.ok === false) {
            setAgentProviderActionsStatus('danger', escapeHtml(payload.error || ('Не удалось обновить список моделей для ' + provider)));
            return;
        }
        applyRefreshedProviderState(provider, payload);
        setAgentProviderActionsStatus('success', 'Список моделей для <code>' + escapeHtml(provider) + '</code> обновлён без сброса введённых полей.');
    } catch (err) {
        setAgentProviderActionsStatus('danger', 'Ошибка обновления моделей: ' + escapeHtml(err.message));
    } finally {
        setAsyncButtonBusy(button, false);
    }
}

async function refreshAllAgentProviders() {
    const button = document.getElementById('refresh-all-agent-providers-btn');
    setAsyncButtonBusy(button, true, 'Обновляю...');
    setAgentProviderActionsStatus('secondary', 'Обновляю списки моделей у всех провайдеров...');
    try {
        const resp = await fetch('/settings/agent-providers/refresh-all', {
            method: 'POST',
            body: buildAllProviderFormData(),
        });
        const payload = await resp.json().catch(() => ({}));
        if (!resp.ok || payload.ok === false) {
            setAgentProviderActionsStatus('danger', escapeHtml(payload.error || 'Не удалось обновить списки моделей'));
            return;
        }
        Object.entries(payload.providers || {}).forEach(([provider, providerPayload]) => {
            applyRefreshedProviderState(provider, providerPayload);
        });
        setAgentProviderActionsStatus('success', 'Списки моделей обновлены без сброса введённых полей.');
    } catch (err) {
        setAgentProviderActionsStatus('danger', 'Ошибка обновления списков моделей: ' + escapeHtml(err.message));
    } finally {
        setAsyncButtonBusy(button, false);
    }
}

async function bulkTestAgentProviders() {
    const button = document.getElementById('bulk-test-agent-providers-btn');
    setAsyncButtonBusy(button, true, 'Тестирую...');
    setAgentProviderActionsStatus(
        'secondary',
        'Запущен массовый compat-probe по всем моделям. Это может занять заметное время, особенно для cloud API.'
    );
    try {
        const resp = await fetch('/settings/agent-providers/test-all', {
            method: 'POST',
            body: buildAllProviderFormData(),
        });
        const payload = await resp.json().catch(() => ({}));
        if (!resp.ok || !payload.ok) {
            setAgentProviderActionsStatus('danger', escapeHtml(payload.error || 'Не удалось протестировать модели'));
            setAsyncButtonBusy(button, false);
            return;
        }
        startBulkTestPolling();
    } catch (err) {
        setAgentProviderActionsStatus('danger', 'Ошибка массового тестирования: ' + escapeHtml(err.message));
        setAsyncButtonBusy(button, false);
    }
}

/* Settings tab persistence & flash-to-tab mapping */
const FLASH_TAB_MAP = {
    account_connected: 'accounts',
    account_toggled: 'accounts',
    account_deleted: 'accounts',
    credentials_saved: 'credentials',
    scheduler_saved: 'scheduler',
    agent_saved: 'devmode',
    filters_saved: 'filters',
    notification_account_saved: 'notifications',
    notification_bot_created: 'notifications',
    notification_bot_deleted: 'notifications',
    notification_test_sent: 'notifications',
    /* error codes */
    no_accounts: 'accounts',
    invalid_api_id: 'credentials',
    invalid_value: 'scheduler',
    agent_dev_mode_required: 'devmode',
    agent_prompt_template_invalid: 'devmode',
    agent_provider_secret_required: 'devmode',
    agent_provider_invalid: 'devmode',
    notification_account_invalid: 'notifications',
    notification_account_unavailable: 'notifications',
    notification_bot_missing: 'notifications',
    notification_action_failed: 'notifications',
    notification_test_failed: 'notifications',
};

function activateSettingsTab(tabId) {
    const btn = document.getElementById('tab-' + tabId);
    if (btn) {
        var tab = new bootstrap.Tab(btn);
        tab.show();
    }
}

function initSettingsTabs() {
    /* Determine which tab to show.
       base.html IIFE saves flash codes to window globals before clearing the URL,
       so we read from globals (reliable) with URL params as fallback. */
    var msg = window.__flashMsg || new URLSearchParams(window.location.search).get('msg');
    var err = window.__flashError || new URLSearchParams(window.location.search).get('error');
    var hash = window.location.hash.replace('#', '');

    /* Priority: flash message tab > URL hash > default (accounts) */
    var targetTab = '';
    if (msg && FLASH_TAB_MAP[msg]) {
        targetTab = FLASH_TAB_MAP[msg];
    } else if (err && FLASH_TAB_MAP[err]) {
        targetTab = FLASH_TAB_MAP[err];
    } else if (hash) {
        targetTab = hash;
    }

    if (targetTab) {
        activateSettingsTab(targetTab);
    }

    /* Persist tab in hash on switch */
    document.querySelectorAll('#settings-tabs button[data-bs-toggle="pill"]').forEach(function(btn) {
        btn.addEventListener('shown.bs.tab', function() {
            var id = btn.getAttribute('data-bs-target').replace('#pane-', '');
            history.replaceState(null, '', '#' + id);
        });
    });
}

document.addEventListener('DOMContentLoaded', function() {
    /* Agent provider model change handlers */
    document.querySelectorAll('select[id^="provider_model__"]').forEach(function(select) {
        var provider = select.id.replace('provider_model__', '');
        select.addEventListener('change', function() {
            probeProviderModel(provider);
        });
    });
    if (document.getElementById('agent-provider-actions-status')) {
        pollBulkTestStatus();
    }

    /* Tab navigation */
    initSettingsTabs();
});
