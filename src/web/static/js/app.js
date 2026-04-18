(function() {
    var msgs = window.__flashMessages || {};
    var errors = window.__flashErrors || {};
    var params = new URLSearchParams(window.location.search);
    var code = params.get("msg");
    var errCode = params.get("error");
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
            if (errors[errCode]) {
                errDiv.textContent = errors[errCode];
            } else {
                errDiv.textContent = errCode;
            }
            container.appendChild(errDiv);
        }
        if (code || errCode) {
            params.delete("msg");
            params.delete("error");
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
    var links = document.querySelectorAll("#mainNav .nav-link");
    function isActiveLink(path, href) {
        if (href === "/") {
            return path === "/";
        }
        if (path === href) {
            return true;
        }
        return path.startsWith(href + "/");
    }
    links.forEach(function(a) {
        var href = a.getAttribute("href");
        if (isActiveLink(path, href)) {
            a.classList.add("active");
        }
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

    document.addEventListener('submit', async function(e) {
        var form = e.target;
        if (form._confirming || form._confirmPending) return;
        var msg = form.dataset && form.dataset.confirm;
        if (!msg) return;
        e.preventDefault();
        var submitter = e.submitter;
        form._confirmPending = true;
        try {
            var ok = await window.TGConfirm.show(msg);
            if (ok) {
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
