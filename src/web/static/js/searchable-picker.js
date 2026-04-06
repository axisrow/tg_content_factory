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
            container.querySelectorAll("input[type=hidden][data-picker-hidden]").forEach(function (el) {
                el.remove();
            });
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
                badge.title = "\u0423\u0431\u0440\u0430\u0442\u044c";
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

        syncHiddenInputs();
        applyFilters();
    }

    document.addEventListener("DOMContentLoaded", function () {
        document.querySelectorAll("[data-picker-name]").forEach(initPicker);
    });
})();
