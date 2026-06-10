(function() {
    var msgs = window.__flashMessages || {};
    var errors = window.__flashErrors || {};
    var params = new URLSearchParams(window.location.search);
    var code = params.get("msg");
    var errCode = params.get("error");
    var warningText = params.get("warning");
    if (code) window.__flashMsg = code;
    if (errCode) window.__flashError = errCode;
    var container = document.getElementById("flash-container");
    if (container) {
        if (code && msgs[code]) {
            container.innerHTML = '<div class="alert alert-success" role="alert">' + msgs[code] + '</div>';
        }
        if (errCode) {
            var errDiv = document.createElement("div");
            errDiv.className = "alert alert-danger";
            errDiv.setAttribute("role", "alert");
            var errText = errors[errCode] || errCode;
            // Partial hard-delete carries irreversible-failure breakdown in
            // the query string — surface it next to the error message so the
            // admin sees exactly what survived (Codex round 10).
            if (errCode === "hard_delete_partial") {
                var purged = params.get("purged");
                var skipped = params.get("skipped");
                var expected = params.get("expected");
                if (purged !== null || skipped !== null || expected !== null) {
                    errText += " (удалено: " + (purged || "0") +
                        ", пропущено: " + (skipped || "0") +
                        ", из " + (expected || "?") + ")";
                }
            }
            errDiv.textContent = errText;
            container.appendChild(errDiv);
        }
        if (warningText) {
            var warnDiv = document.createElement("div");
            warnDiv.className = "alert alert-warning";
            warnDiv.setAttribute("role", "alert");
            warnDiv.textContent = warningText;
            container.appendChild(warnDiv);
        }
        if (code || errCode || warningText) {
            params.delete("msg");
            params.delete("error");
            params.delete("warning");
            // hard_delete_partial carries breakdown counts in the URL — drop
            // them after rendering so they don't linger as stale state.
            params.delete("purged");
            params.delete("skipped");
            params.delete("expected");
            params.delete("count");
            var qs = params.toString();
            var url = window.location.pathname + (qs ? "?" + qs : "") + window.location.hash;
            window.history.replaceState(null, "", url);
            setTimeout(function() { container.innerHTML = ""; }, 5000);
        }
    }

    var themeBtn = document.getElementById("theme-toggle");
    var THEME_MODES = ["auto", "light", "dark"];
    var THEME_ICONS = {auto: "\u25D0", light: "\u2600\uFE0F", dark: "\uD83C\uDF19"};
    var THEME_TITLES = {auto: "Тема: авто", light: "Тема: светлая", dark: "Тема: тёмная"};
    function getThemeMode() { return localStorage.getItem("theme") || "auto"; }
    function resolveTheme(mode) {
        return mode === "auto" ? (matchMedia("(prefers-color-scheme:dark)").matches ? "dark" : "light") : mode;
    }
    function applyThemeMode(mode) {
        document.documentElement.setAttribute("data-bs-theme", resolveTheme(mode));
        if (themeBtn) {
            themeBtn.textContent = THEME_ICONS[mode];
            themeBtn.title = THEME_TITLES[mode];
        }
    }
    applyThemeMode(getThemeMode());
    matchMedia("(prefers-color-scheme:dark)").addEventListener("change", function() {
        if (getThemeMode() === "auto") applyThemeMode("auto");
    });
    if (themeBtn) {
        themeBtn.addEventListener("click", function() {
            var cur = getThemeMode();
            var next = THEME_MODES[(THEME_MODES.indexOf(cur) + 1) % 3];
            if (next === "auto") localStorage.removeItem("theme"); else localStorage.setItem("theme", next);
            applyThemeMode(next);
        });
    }

    var path = window.location.pathname;
    var sidebar = document.getElementById("app-sidebar");
    var sidebarStorageKey = "tg-agent-sidebar-collapsed";
    var sidebarToggles = document.querySelectorAll("[data-sidebar-toggle]");
    var sidebarBackdrop = document.querySelector("[data-sidebar-backdrop]");
    var topbarTitle = document.getElementById("app-topbar-title");
    var settingsBtn = document.querySelector(".app-settings-btn");

    function isDesktopSidebar() {
        return window.matchMedia("(min-width: 992px)").matches;
    }

    function applySidebarState() {
        if (!sidebar) return;
        document.body.classList.toggle("sidebar-collapsed", localStorage.getItem(sidebarStorageKey) === "1");
        if (isDesktopSidebar()) {
            document.body.classList.remove("sidebar-open");
        }
    }

    function toggleSidebar() {
        if (!sidebar) return;
        if (isDesktopSidebar()) {
            var collapsed = !document.body.classList.contains("sidebar-collapsed");
            document.body.classList.toggle("sidebar-collapsed", collapsed);
            if (collapsed) {
                localStorage.setItem(sidebarStorageKey, "1");
            } else {
                localStorage.removeItem(sidebarStorageKey);
            }
            return;
        }
        document.body.classList.toggle("sidebar-open");
    }

    applySidebarState();
    sidebarToggles.forEach(function(btn) {
        btn.addEventListener("click", toggleSidebar);
    });
    if (sidebarBackdrop) {
        sidebarBackdrop.addEventListener("click", function() {
            document.body.classList.remove("sidebar-open");
        });
    }
    window.addEventListener("resize", applySidebarState);

    var links = document.querySelectorAll("#mainNav .app-sidebar-link");
    function isActiveLink(path, href) {
        if (href === "/") {
            return path === "/";
        }
        if (path === href) {
            return true;
        }
        return path.startsWith(href + "/");
    }
    var activeLink = null;
    links.forEach(function(a) {
        var href = a.getAttribute("href");
        if (isActiveLink(path, href)) {
            if (!activeLink || href.length > activeLink.getAttribute("href").length) {
                activeLink = a;
            }
        }
    });
    if (activeLink) {
        activeLink.classList.add("active");
        if (topbarTitle) {
            var label = activeLink.querySelector("span");
            topbarTitle.textContent = label ? label.textContent.trim() : document.title.split("—")[0].trim();
        }
    } else if (topbarTitle) {
        topbarTitle.textContent = document.title.split("—")[0].trim() || "TG Agent";
    }
    if (settingsBtn && isActiveLink(path, "/settings")) {
        settingsBtn.classList.add("active");
        settingsBtn.setAttribute("aria-current", "page");
    }
    links.forEach(function(a) {
        a.addEventListener("click", function() {
            if (!isDesktopSidebar()) {
                document.body.classList.remove("sidebar-open");
            }
        });
    });

    function convertLocalDates(root) {
        (root || document).querySelectorAll('.local-dt[data-utc]').forEach(function(el) {
            var d = new Date(el.dataset.utc);
            if (isNaN(d)) return;
            var fmt = el.dataset.fmt || 'datetime';
            if (fmt === 'time') {
                el.textContent = d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
            } else if (fmt === 'date') {
                el.textContent = d.toLocaleDateString();
            } else {
                el.textContent = d.toLocaleString([], {year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'});
            }
        });
    }
    document.addEventListener('DOMContentLoaded', function() { convertLocalDates(); });
    document.body.addEventListener('htmx:afterSwap', function(e) { convertLocalDates(e.detail.target); });
    window.convertLocalDates = convertLocalDates;

    var _clockInterval = null;
    function initServerClock() {
        var el = document.querySelector('.server-clock[data-utc]');
        if (!el) return;
        var serverStart = new Date(el.dataset.utc).getTime();
        if (isNaN(serverStart)) return;
        // Clear any prior interval: an htmx:afterSwap of hx-target="body" (e.g. the
        // /dialogs/ account switcher) replaces the navbar, so the old .server-clock node
        // is detached. Without re-init the clock would freeze; without clearing first we
        // would leak a tick() writing to a detached node.
        if (_clockInterval !== null) {
            clearInterval(_clockInterval);
            _clockInterval = null;
        }
        var offset = serverStart - Date.now(); // server - client
        var out = el.querySelector('.server-clock-value') || el;
        function tick() {
            var d = new Date(Date.now() + offset);
            var hh = String(d.getUTCHours()).padStart(2, '0');
            var mm = String(d.getUTCMinutes()).padStart(2, '0');
            var ss = String(d.getUTCSeconds()).padStart(2, '0');
            out.textContent = hh + ':' + mm + ':' + ss + ' UTC';
        }
        tick();
        _clockInterval = setInterval(tick, 1000);
    }
    document.addEventListener('DOMContentLoaded', initServerClock);
    // Re-init only when the swap actually replaced the clock (e.g. hx-target="body"
    // on /dialogs/). Re-initialising on every partial swap would needlessly clear and
    // restart the interval, making the seconds counter visibly stutter. Mirrors how
    // convertLocalDates scopes its work to e.detail.target.
    document.body.addEventListener('htmx:afterSwap', function(e) {
        var t = e.detail && e.detail.target;
        if (!t) return;
        if ((t.classList && t.classList.contains('server-clock')) ||
            (t.querySelector && t.querySelector('.server-clock'))) {
            initServerClock();
        }
    });

    function tgdlgCreate(message, choices) {
        return new Promise(function(resolve) {
            var modal = document.createElement('div');
            modal.className = 'tgdlg-overlay';
            var box = document.createElement('div');
            box.className = 'tgdlg-box';
            box.tabIndex = -1;
            var msgEl = document.createElement('div');
            msgEl.className = 'tgdlg-message';
            msgEl.textContent = message;
            box.appendChild(msgEl);
            var menu = document.createElement('div');
            choices.forEach(function(c, i) {
                var item = document.createElement('div');
                item.className = 'tgdlg-item';
                item.tabIndex = 0;
                var cur = document.createElement('span');
                cur.className = 'tgdlg-cursor';
                cur.textContent = ' ';
                item.appendChild(cur);
                item.appendChild(document.createTextNode(' ' + (i + 1) + '. ' + c.label));
                menu.appendChild(item);
            });
            box.appendChild(menu);
            var hint = document.createElement('div');
            hint.className = 'tgdlg-hint';
            hint.textContent = choices.length > 1
                ? '↑↓ навигация \u00B7 Enter выбор \u00B7 Esc = ' + choices[choices.length - 1].label
                : 'Enter / Esc — закрыть';
            box.appendChild(hint);
            modal.appendChild(box);
            document.body.appendChild(modal);

            var items = menu.querySelectorAll('.tgdlg-item');
            var selected = 0;
            function highlight() {
                items.forEach(function(el, i) {
                    el.querySelector('.tgdlg-cursor').textContent = i === selected ? '>' : ' ';
                    el.classList.toggle('tgdlg-selected', i === selected);
                });
            }
            function pick(value) {
                if (modal.parentNode) modal.parentNode.removeChild(modal);
                document.removeEventListener('keydown', onKey, true);
                resolve(value);
            }
            function onKey(e) {
                var len = choices.length;
                if (e.key === 'ArrowUp' || e.key === 'k') {
                    selected = (selected - 1 + len) % len; highlight(); e.preventDefault();
                } else if (e.key === 'ArrowDown' || e.key === 'j') {
                    selected = (selected + 1) % len; highlight(); e.preventDefault();
                } else if (e.key === 'Enter') {
                    pick(choices[selected].value); e.preventDefault();
                } else if (e.key === 'Escape') {
                    pick(choices[len - 1].value); e.preventDefault();
                } else {
                    for (var i = 0; i < len; i++) {
                        if (e.key === String(i + 1)) { pick(choices[i].value); e.preventDefault(); return; }
                    }
                }
            }
            items.forEach(function(el, i) {
                el.addEventListener('click', function() { pick(choices[i].value); });
            });
            document.addEventListener('keydown', onKey, true);
            highlight();
            setTimeout(function() { box.focus(); }, 0);
        });
    }
    window.TGConfirm = {
        show: function(message, opts) {
            opts = opts || {};
            return tgdlgCreate(message, [
                {label: opts.confirmText || 'Подтвердить', value: true},
                {label: opts.cancelText || 'Отмена', value: false}
            ]);
        },
        alert: function(message) {
            return tgdlgCreate(message, [{label: 'OK', value: true}]).then(function() {});
        }
    };

    function tgSubmitConfirmed(form, submitter) {
        form._confirming = true;
        try {
            if (typeof form.requestSubmit === 'function') {
                form.requestSubmit(submitter || undefined);
            } else {
                form.submit();
            }
        } finally {
            delete form._confirming;
        }
    }

    document.addEventListener('submit', async function(e) {
        var form = e.target;
        if (form._confirming || form._confirmPending) return;
        var reassignRaw = form.dataset && form.dataset.notifyReassign;
        var msg = form.dataset && form.dataset.confirm;
        if (!reassignRaw && !msg) return;
        e.preventDefault();
        var submitter = e.submitter;
        form._confirmPending = true;
        try {
            if (reassignRaw) {
                var accounts = [];
                try { accounts = JSON.parse(reassignRaw) || []; } catch (err) { accounts = []; }
                var choices = accounts.map(function(a) {
                    return {label: a.label, value: a.value};
                });
                choices.push({label: 'Primary (по умолчанию)', value: ''});
                choices.push({label: 'Отмена', value: null});
                var picked = await tgdlgCreate(
                    'Этот аккаунт используется для уведомлений — на какой переназначить?',
                    choices
                );
                if (picked !== null) {
                    var input = form.querySelector('input[name="notify_to"]');
                    if (input) input.value = picked;
                    tgSubmitConfirmed(form, submitter);
                }
            } else {
                var ok = await window.TGConfirm.show(msg);
                if (ok) {
                    tgSubmitConfirmed(form, submitter);
                }
            }
        } finally {
            delete form._confirmPending;
        }
    }, true);

    document.addEventListener('change', async function(e) {
        var el = e.target;
        if (!el || el._confirming || el.type !== 'checkbox' || !el.checked) return;
        var msg = el.dataset && el.dataset.confirmCheck;
        if (!msg) return;
        el.checked = false;
        var ok = await window.TGConfirm.show(msg);
        if (ok) {
            el._confirming = true;
            el.checked = true;
            el.dispatchEvent(new Event('change', {bubbles: true}));
            delete el._confirming;
        }
    }, true);

    window.showToast = function(text) {
        var el = document.getElementById("live-toast");
        if (!el) return;
        var body = el.querySelector(".toast-body");
        if (body) body.textContent = text;
        var t = bootstrap.Toast.getOrCreateInstance(el);
        t.show();
    };
})();
