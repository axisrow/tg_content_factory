/**
 * Searchable multi-select picker.
 *
 * Modes:
 *   Static — add [data-picker-name="field"] + <label data-picker-option ...> elements.
 *   AJAX   — add [data-picker-name="field" data-picker-url="/api/search?q="].
 *            Results are fetched on type (>1 char). Pre-selected items via
 *            <input data-picker-preselected value="..." data-title="...">.
 */
(function () {
    "use strict";

    function initPicker(container) {
        var name = container.dataset.pickerName;
        var isMulti = container.dataset.pickerMulti !== "false";
        var ajaxUrl = container.dataset.pickerUrl || "";
        var searchInput = container.querySelector("[data-picker-search]");
        var listEl = container.querySelector(".picker-list");
        var filterButtons = Array.from(container.querySelectorAll("[data-filter]"));
        var emptyEl = container.querySelector("[data-picker-empty]");
        var selectedContainer = container.querySelector(".picker-selected");
        var activeFilter = "all";

        // Track all options: static + dynamically added
        var options = Array.from(container.querySelectorAll("[data-picker-option]"));

        // Pre-selected items (AJAX mode) — rendered as badges immediately
        var preselectedEls = Array.from(container.querySelectorAll("[data-picker-preselected]"));
        var preselectedMap = {}; // value -> title
        preselectedEls.forEach(function (el) {
            preselectedMap[el.value] = el.dataset.title || el.value;
        });

        // AJAX debounce timer
        var ajaxTimer = null;

        function getCheckbox(opt) {
            return opt.querySelector("input[type=checkbox]");
        }

        function syncHiddenInputs() {
            container.querySelectorAll("input[type=hidden][data-picker-hidden]").forEach(function (el) {
                el.remove();
            });
            options.forEach(function (opt) {
                if (getCheckbox(opt).checked) {
                    addHiddenInput(opt.dataset.value);
                }
            });
            // Also include pre-selected that don't have a visible option yet
            preselectedEls.forEach(function (el) {
                var already = options.some(function (opt) {
                    return opt.dataset.value === el.value && getCheckbox(opt).checked;
                });
                if (!already) {
                    addHiddenInput(el.value);
                }
            });
            renderBadges();
        }

        function addHiddenInput(value) {
            var inp = document.createElement("input");
            inp.type = "hidden";
            inp.name = name;
            inp.value = value;
            inp.setAttribute("data-picker-hidden", "");
            container.appendChild(inp);
        }

        function renderBadges() {
            if (!selectedContainer) return;
            selectedContainer.innerHTML = "";

            // Badges for checked options
            options.forEach(function (opt) {
                if (!getCheckbox(opt).checked) return;
                addBadge(opt.dataset.searchText.split(" ")[0] || opt.dataset.value, opt);
            });
            // Badges for pre-selected without visible option
            preselectedEls.forEach(function (el) {
                var hasVisible = options.some(function (opt) {
                    return opt.dataset.value === el.value;
                });
                if (hasVisible) return;
                addBadge(el.dataset.title || el.value, null, el.value);
            });
        }

        function addBadge(text, opt, fallbackValue) {
            var badge = document.createElement("span");
            badge.className = "badge bg-primary me-1 mb-1";
            badge.style.cursor = "pointer";
            badge.title = "\u0423\u0431\u0440\u0430\u0442\u044c";
            badge.textContent = text;
            badge.addEventListener("click", function () {
                if (opt) {
                    getCheckbox(opt).checked = false;
                }
                if (fallbackValue) {
                    // Remove pre-selected element
                    preselectedEls = preselectedEls.filter(function (el) {
                        return el.value !== fallbackValue || (el.parentNode && el.parentNode.removeChild(el), false);
                    });
                    delete preselectedMap[fallbackValue];
                }
                syncHiddenInputs();
            });
            selectedContainer.appendChild(badge);
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

        function bindOptionEvents(opt) {
            getCheckbox(opt).addEventListener("change", function () {
                if (!isMulti) {
                    options.forEach(function (other) {
                        if (other !== opt) getCheckbox(other).checked = false;
                    });
                }
                syncHiddenInputs();
            });
        }

        // Bind events for initial static options
        options.forEach(bindOptionEvents);

        // ---- AJAX mode ----
        function fetchAjaxResults(query) {
            if (!ajaxUrl || !listEl) return;
            // Short queries → fetch default batch (top 50) instead of clearing
            if (query.length < 2) {
                query = "";
            }

            // Check sessionStorage cache
            var cacheKey = "picker:" + ajaxUrl + query;
            try {
                var cached = sessionStorage.getItem(cacheKey);
                if (cached) {
                    renderAjaxResults(JSON.parse(cached));
                    return;
                }
            } catch (e) { /* ignore */ }

            fetch(ajaxUrl + encodeURIComponent(query), {
                headers: {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
            })
            .then(function (r) { return r.json(); })
            .then(function (data) {
                try { sessionStorage.setItem(cacheKey, JSON.stringify(data)); } catch (e) { /* ignore */ }
                renderAjaxResults(data);
            })
            .catch(function () {
                if (emptyEl) {
                    emptyEl.textContent = "\u041e\u0448\u0438\u0431\u043a\u0430 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438";
                    emptyEl.classList.remove("d-none");
                }
            });
        }

        function renderAjaxResults(items) {
            if (!listEl) return;
            listEl.innerHTML = "";
            options = options.filter(function (o) { return o.isConnected; });
            var count = 0;
            (items || []).forEach(function (item) {
                var label = document.createElement("label");
                label.className = "list-group-item list-group-item-action d-flex align-items-center gap-2";
                label.setAttribute("data-picker-option", "");
                label.setAttribute("data-value", item.value);
                label.setAttribute("data-group", item.group || "");
                label.setAttribute("data-search-text", (item.title + " " + (item.username || "")).toLowerCase());

                var cb = document.createElement("input");
                cb.type = "checkbox";
                cb.className = "form-check-input mt-0";
                // Pre-check if this value was pre-selected
                if (preselectedMap[item.value]) {
                    cb.checked = true;
                }
                label.appendChild(cb);

                var span = document.createElement("span");
                span.textContent = item.title;
                label.appendChild(span);

                if (item.username) {
                    var small = document.createElement("small");
                    small.className = "text-muted";
                    small.textContent = "@" + item.username;
                    span.appendChild(document.createTextNode(" "));
                    span.appendChild(small);
                }

                listEl.appendChild(label);
                options.push(label);
                bindOptionEvents(label);
                count++;
            });
            if (emptyEl) emptyEl.classList.toggle("d-none", count !== 0);
            syncHiddenInputs();
        }

        if (searchInput) {
            if (ajaxUrl) {
                searchInput.addEventListener("input", function () {
                    clearTimeout(ajaxTimer);
                    ajaxTimer = setTimeout(function () {
                        fetchAjaxResults(searchInput.value.trim());
                    }, 200);
                });
            } else {
                searchInput.addEventListener("input", applyFilters);
            }
        }

        filterButtons.forEach(function (btn) {
            btn.addEventListener("click", function () {
                activeFilter = btn.dataset.filter;
                filterButtons.forEach(function (b) {
                    b.classList.toggle("active", b === btn);
                });
                applyFilters();
            });
        });

        syncHiddenInputs();
        if (ajaxUrl) {
            // AJAX mode: load initial batch immediately
            fetchAjaxResults("");
        } else {
            applyFilters();
        }
    }

    document.addEventListener("DOMContentLoaded", function () {
        document.querySelectorAll("[data-picker-name]").forEach(initPicker);
    });
})();
