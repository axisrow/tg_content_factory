// Lightweight top progress bar so navigation gives immediate feedback instead
// of "nothing happens" while a slow page/request loads (issue #756). No deps.
(function () {
  "use strict";

  var bar = document.getElementById("app-progress");
  if (!bar) return;

  var timer = null;
  var visible = false;

  function set(pct) {
    bar.style.width = pct + "%";
  }

  function start() {
    if (visible) return;
    visible = true;
    bar.classList.add("is-active");
    set(8);
    var pct = 8;
    // Trickle towards (but never reaching) 100% so the bar keeps moving while
    // we wait on the server.
    timer = window.setInterval(function () {
      pct += Math.max(0.5, (90 - pct) * 0.08);
      if (pct > 90) pct = 90;
      set(pct);
    }, 200);
  }

  function done() {
    if (timer) {
      window.clearInterval(timer);
      timer = null;
    }
    if (!visible) return;
    set(100);
    window.setTimeout(function () {
      bar.classList.remove("is-active");
      set(0);
      visible = false;
    }, 250);
  }

  // --- Full-page navigation (sidebar links, forms) ---
  function isProgressLink(a) {
    if (!a || a.target === "_blank" || a.hasAttribute("download")) return false;
    if (a.dataset.noProgress !== undefined) return false;
    var href = a.getAttribute("href");
    if (!href || href.charAt(0) === "#" || href.indexOf("javascript:") === 0) return false;
    if (a.hasAttribute("hx-get") || a.hasAttribute("hx-post")) return false; // HTMX handles its own
    // Same-origin only.
    return a.origin === window.location.origin;
  }

  document.addEventListener("click", function (e) {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    var a = e.target.closest && e.target.closest("a");
    if (a && isProgressLink(a)) start();
  });

  document.addEventListener("submit", function (e) {
    var form = e.target;
    if (form && form.tagName === "FORM" && !form.hasAttribute("hx-post") && !form.hasAttribute("hx-get")) {
      start();
    }
  });

  // The new document finishes loading → complete the bar; bfcache restore resets it.
  window.addEventListener("load", done);
  window.addEventListener("pageshow", function (e) {
    if (e.persisted) done();
  });
  window.addEventListener("beforeunload", start);

  // --- HTMX requests (partial swaps) ---
  document.body.addEventListener("htmx:beforeRequest", start);
  document.body.addEventListener("htmx:afterRequest", done);
  document.body.addEventListener("htmx:responseError", done);
  document.body.addEventListener("htmx:sendError", done);
})();
