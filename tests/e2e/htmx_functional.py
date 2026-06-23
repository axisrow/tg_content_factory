"""Functional HTMX e2e checks for the web panel (issue #1015).

Why this exists beyond ``console_smoke`` (#792): the console smoke walks full-page
URLs and only asserts zero JS console errors. For lazyload pages (#756: an empty
skeleton + ``hx-trigger="load"``) a *failed* fragment request still renders a clean
skeleton, so the page passes "green" while its content never loaded. This module
asserts the **functionality**, not just the absence of errors:

1. Lazyload filled — after load, the fragment container actually replaced its
   skeleton (the ``loading()`` macro's hourglass icon is gone) AND the fragment GET
   returned HTTP 200. Applies to ``/jobs``, ``/analytics/channels``, ``/moderation``,
   ``/dashboard``. This is the "did #756 really run" check.
2. ``/analytics/channels/rate`` swap (#999) — clicking "Запустить судью" performs an
   HTMX swap of the ``_rating_verdict.html`` fragment into ``#rate-result``.
3. Collect-button OOB swap — clicking a channel's desktop collect button OOB-swaps
   BOTH the desktop and mobile buttons into the disabled "queued" state.

Driven by the **Playwright Python API**, the same engine ``console_smoke`` migrated
to in #1014 (we reuse its ``browser_session`` / ``login`` / ``_goto`` helpers). The
native API gives direct assertions — ``page.expect_response`` reads the fragment's
HTTP status straight off the network event (no log parsing), ``page.wait_for_function``
waits on a DOM predicate with a built-in timeout — exactly the "much simpler on
pytest-playwright" the issue called for. It is **opt-in and local-only**: it needs a
live ``serve`` plus the Playwright Chromium build and is never wired into CI.

Detecting "filled vs skeleton" robustly (the issue's deepest pitfall): on an EMPTY
DB a fragment legitimately returns a 200 with an empty-state body (``/jobs`` →
``.alert-info`` "Нет фоновых задач."; ``/rate`` without an LLM provider →
``.alert-warning``). That is a *valid filled container*, not a stuck skeleton. So
"filled" is keyed on the skeleton marker disappearing — the ``loading()`` macro
renders ``<i class="bi bi-hourglass-split">``; once HTMX swaps the fragment in, that
icon is gone. We assert its absence (structural, locale-independent) rather than
matching the "Загрузка…" text or waiting for a specific table that an empty DB never
produces.

Run it by hand or via pytest (``tests/e2e/test_htmx_functional.py``)::

    python -m src.main serve --web-pass secret      # in another terminal
    RUN_E2E_CONSOLE_SMOKE=1 E2E_BASE_URL=http://localhost:8080 WEB_PASS=secret \\
        pytest tests/e2e/test_htmx_functional.py -m e2e
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tests.e2e.console_smoke import (
    RedirectedToLoginError,
    _goto,
    _is_login_path,
    _path_of,
    browser_session,
    login,
)

if TYPE_CHECKING:
    from playwright.sync_api import Page

# Timeout (ms) for HTMX waits. A lazyload fragment doing a COUNT(*) over millions of
# rows can take a few seconds, so this is generous; a real hang still fails (the
# wait_for_function raises a TimeoutError, which we map to a loud assertion).
HTMX_WAIT_TIMEOUT_MS = 10_000

# --- lazyload page specs ------------------------------------------------------


@dataclass(frozen=True)
class LazyloadSpec:
    """A lazyloaded page: its URL, the container that swaps, and how to tell it filled."""

    path: str  # full-page URL to visit
    container_sel: str  # CSS selector of the lazyload container (the skeleton host)
    fragment_url: str  # substring identifying the fragment GET (matched on the network event)
    marker_sel: str  # selector present ONLY once the fragment swapped in


# container_sel: containers without an id are targeted via their ``hx-get`` attribute.
# marker_sel: a content node the fragment renders — kept permissive (``table``/``.alert``/
# ``.card``/``select``) so an empty DB's valid empty-state still counts as "filled".
LAZYLOAD_SPECS: tuple[LazyloadSpec, ...] = (
    LazyloadSpec(
        path="/jobs",
        container_sel="#jobs-table",
        fragment_url="/jobs/fragments/list",
        marker_sel="#jobs-table table, #jobs-table .alert",
    ),
    LazyloadSpec(
        path="/analytics/channels",
        container_sel="[hx-get^='/analytics/channels/fragments/selector']",
        fragment_url="/analytics/channels/fragments/selector",
        marker_sel="#channel-select",
    ),
    LazyloadSpec(
        path="/moderation",
        container_sel="#moderation-content",
        fragment_url="/moderation/fragments/table",
        marker_sel="#moderation-content table, #moderation-content .alert, #moderation-content .card",
    ),
    LazyloadSpec(
        path="/dashboard",
        container_sel="[hx-get='/dashboard/fragments/overview']",
        fragment_url="/dashboard/fragments/overview",
        marker_sel="[hx-get='/dashboard/fragments/overview'] .row, [hx-get='/dashboard/fragments/overview'] .card",
    ),
)

# Pages that require seeded state to even render (so a clean DB can't silently pass).
RATINGS_PATH = "/analytics/channels/ratings"
CHANNELS_PATH = "/channels"
CHANNELS_FRAGMENT_URL = "/channels/fragments/list"
RATE_POST_URL = "/analytics/channels/rate"


# --- result models ------------------------------------------------------------


@dataclass(frozen=True)
class LazyloadResult:
    """Outcome of checking one lazyload page."""

    path: str
    filled: bool  # skeleton replaced (no hourglass-split icon left in the container)
    marker_present: bool  # the expected content selector appeared
    fragment_status: int | None  # HTTP status of the fragment GET (200 expected)
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.filled and self.marker_present and self.fragment_status == 200


@dataclass(frozen=True)
class RateSwapResult:
    """Outcome of the /rate HTMX swap check (#999)."""

    swapped: bool  # #rate-result became non-empty with a verdict fragment
    kind: str  # "verdict" (.card) | "error" (.alert) | "empty" | "missing" | "other"
    post_status: int | None  # HTTP status of the POST /rate (200 expected)
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.swapped and self.post_status == 200


@dataclass(frozen=True)
class CollectOobResult:
    """Outcome of the collect-button OOB swap check (desktop + mobile)."""

    channel_pk: int | None  # DB pk of the channel whose button was clicked (None = no channels)
    desktop_disabled: bool
    mobile_disabled: bool
    # True only when the /channels table genuinely lazyloaded but held no channel rows
    # (a clean DB) — the test skips this case. A FAILED lazyload (fragment !=200 /
    # stuck skeleton) must NOT land here: that is a real regression and must fail, not
    # skip (a skip would hide the exact failure mode this whole module exists to catch).
    no_channels: bool = False
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.desktop_disabled and self.mobile_disabled


@dataclass(frozen=True)
class HtmxCheckResults:
    """All functional results from one browser session."""

    lazyload: list[LazyloadResult]
    rate: RateSwapResult
    collect: CollectOobResult


# --- JS predicates (page.wait_for_function / page.evaluate) --------------------
# Plain JS strings passed to Playwright's evaluate/wait_for_function. wait_for_function
# accepts an ``arg`` for parameterised predicates (used by the OOB check).

# Skeleton gone: the loading() macro renders <i class="bi bi-hourglass-split">; once the
# fragment swaps in, that icon is no longer inside the container. Structural (locale-proof).
_JS_FILLED = (
    "(sel) => { const c = document.querySelector(sel);"
    " return !!c && !c.querySelector('.bi-hourglass-split'); }"
)

# #rate-result holds a verdict fragment (.card on success, .alert on the safe no-provider
# / no-posts path). The htmx-indicator spinner is a .spinner-border, so it never matches
# .card/.alert — no false positive.
_JS_RATE_SWAPPED = (
    "() => { const c = document.querySelector('#rate-result');"
    " return !!c && !!c.querySelector('.card, .alert'); }"
)

# Classify what landed in #rate-result.
_JS_RATE_KIND = (
    "() => { const c = document.querySelector('#rate-result');"
    " if (!c) return 'missing';"
    " if (c.querySelector('.card')) return 'verdict';"
    " if (c.querySelector('.alert')) return 'error';"
    " return (c.textContent || '').trim() ? 'other' : 'empty'; }"
)

# First channel's DB pk from the DOM. ``[id^='collect-btn-']`` also matches the mobile
# (``collect-btn-m-…``) and the page-level ``collect-all-btn``; exclude mobile and require
# a numeric tail so only a real desktop per-row button (``collect-btn-<pk>``) matches.
_JS_FIRST_PK = (
    "() => { const els = document.querySelectorAll("
    "\"[id^='collect-btn-']:not([id^='collect-btn-m-'])\");"
    " for (const el of els) { const m = el.id.match(/^collect-btn-(\\d+)$/);"
    " if (m) return parseInt(m[1], 10); } return -1; }"
)

# Both collect buttons reached the disabled "queued/running" state (OOB swap of desktop
# AND mobile). Checking BOTH is the point — a single-target check would miss an OOB
# desync, which is exactly the mechanic this guards.
_JS_BOTH_DISABLED = (
    "(pk) => { const d = document.querySelector('#collect-btn-' + pk + ' button');"
    " const m = document.querySelector('#collect-btn-m-' + pk + ' button');"
    " return !!d && d.disabled && !!m && m.disabled; }"
)

# Read one collect button's disabled flag (per-side reporting on failure).
_JS_BTN_DISABLED = "(sel) => { const b = document.querySelector(sel + ' button'); return !!b && b.disabled; }"


# --- helpers ------------------------------------------------------------------


def _wait_filled(page: "Page", container_sel: str) -> bool:
    """Wait for ``container_sel`` to stop showing the loading skeleton; False on timeout.

    A timed-out wait means the container never filled — map it to ``False`` so the
    calling check yields a readable assertion ("container never filled") rather than a
    raw Playwright stack trace.
    """
    from playwright.sync_api import Error as PlaywrightError

    try:
        page.wait_for_function(_JS_FILLED, arg=container_sel, timeout=HTMX_WAIT_TIMEOUT_MS)
        return True
    except PlaywrightError:
        return False


def _landed_off(page: "Page", path: str) -> str | None:
    """Return the landed path if the navigation redirected away from ``path``, else None.

    Bouncing to ``/login`` is an unauthenticated session — raise loudly. Any other
    redirect (e.g. ``/dashboard`` → ``/settings`` with no accounts) is returned so the
    caller can report it.
    """
    landed = _path_of(page.url)
    if _is_login_path(landed) and not _is_login_path(path):
        raise RedirectedToLoginError(f"navigation to {path!r} bounced to {landed!r}: session is not authenticated")
    if landed.rstrip("/") != path.rstrip("/"):
        return landed
    return None


# --- checks -------------------------------------------------------------------


def check_lazyload(page: "Page", base_url: str, spec: LazyloadSpec) -> LazyloadResult:
    """Visit a lazyload page, capture the fragment status, wait for the container to fill.

    ``page.expect_response`` wraps the ``goto`` so the fragment GET (fired by
    ``hx-trigger="load"`` right after the page loads) is captured directly off the
    network — its HTTP status, no log parsing. A redirect away from the page (e.g.
    ``/dashboard`` → ``/settings`` on an account-less DB) is reported instead of timing
    out on a missing container.
    """
    from playwright.sync_api import Error as PlaywrightError

    base = base_url.rstrip("/")
    status: int | None = None
    try:
        with page.expect_response(
            lambda r: spec.fragment_url in r.url, timeout=HTMX_WAIT_TIMEOUT_MS
        ) as resp_info:
            _goto(page, f"{base}{spec.path}")
        status = resp_info.value.status
    except PlaywrightError:
        # The fragment request never fired — usually because the page redirected before
        # the lazyload could start. _landed_off below turns that into a clear detail.
        _goto(page, f"{base}{spec.path}")
    redirected = _landed_off(page, spec.path)
    if redirected is not None:
        return LazyloadResult(
            path=spec.path,
            filled=False,
            marker_present=False,
            fragment_status=status,
            detail=f"redirected to {redirected!r} (needs seeded state — e.g. /dashboard wants an account)",
        )
    filled = _wait_filled(page, spec.container_sel)
    marker = _marker_present(page, spec.marker_sel)
    detail = ""
    if not filled:
        detail = "container still showing the loading skeleton"
    elif not marker:
        detail = f"content marker {spec.marker_sel!r} never appeared"
    elif status != 200:
        detail = f"fragment {spec.fragment_url} returned status={status}"
    return LazyloadResult(
        path=spec.path,
        filled=filled,
        marker_present=marker,
        fragment_status=status,
        detail=detail,
    )


def _marker_present(page: "Page", marker_sel: str) -> bool:
    """Wait for the content marker to attach to the DOM; False on timeout."""
    from playwright.sync_api import Error as PlaywrightError

    try:
        page.locator(marker_sel).first.wait_for(state="attached", timeout=HTMX_WAIT_TIMEOUT_MS)
        return True
    except PlaywrightError:
        return False


def check_rate_swap(page: "Page", base_url: str) -> RateSwapResult:
    """On /analytics/channels/ratings: fill channel_id, submit, await the verdict swap (#999).

    Returns a valid verdict fragment whether or not an LLM is configured, and never spends:

    - With NO provider configured, ``rate_channel`` returns the ``.alert-warning`` error
      fragment before any provider work.
    - Even WITH a provider configured, the route's no-posts guard (``analytics.py``:
      ``sample_posts`` fires *before* ``classify_channel``) returns an error fragment for
      ``channel_id=1`` — which matches no posts (the project's channel_ids are bare-positive
      Telegram IDs, always large), so no real LLM call happens.

    Either way the ``#rate-result`` container gets a ``_rating_verdict.html`` fragment, so we
    exercise the write-flow's HTMX swap deterministically on a clean/test DB without secrets
    or spend. ``channel_id`` is required by the form, so we fill ``1``.
    """
    from playwright.sync_api import Error as PlaywrightError

    base = base_url.rstrip("/")
    _goto(page, f"{base}{RATINGS_PATH}")
    _landed_off(page, RATINGS_PATH)  # bounce-to-login guard
    post_status: int | None = None
    # Fill + submit + capture the POST status under one guard: a failed form
    # interaction (e.g. a missing field) becomes ok=False with a clear detail
    # rather than a raw Playwright error — consistent with the other checks.
    try:
        page.fill("input[name='channel_id']", "1")
        with page.expect_response(
            lambda r: RATE_POST_URL in r.url and r.request.method == "POST", timeout=HTMX_WAIT_TIMEOUT_MS
        ) as resp_info:
            page.locator("button.btn-warning").click()
        post_status = resp_info.value.status
    except PlaywrightError:
        post_status = None
    swapped = _wait_rate_swapped(page)
    kind = page.evaluate(_JS_RATE_KIND)
    detail = ""
    if not swapped:
        detail = f"#rate-result never got a verdict fragment (kind={kind})"
    elif post_status != 200:
        detail = f"POST {RATE_POST_URL} returned status={post_status}"
    return RateSwapResult(swapped=swapped, kind=kind, post_status=post_status, detail=detail)


def _wait_rate_swapped(page: "Page") -> bool:
    from playwright.sync_api import Error as PlaywrightError

    try:
        page.wait_for_function(_JS_RATE_SWAPPED, timeout=HTMX_WAIT_TIMEOUT_MS)
        return True
    except PlaywrightError:
        return False


def check_collect_oob(page: "Page", base_url: str) -> CollectOobResult:
    """On /channels: wait lazyload, click the first channel's desktop collect, await OOB swap.

    Asserts BOTH ``#collect-btn-{pk}`` (desktop) and ``#collect-btn-m-{pk}`` (mobile) reach
    the disabled state — the OOB swap updates both, and checking only one would miss a
    desync. Needs ≥1 channel in the DB (per-row buttons have no empty-state fallback).

    A genuine empty DB (the table lazyloaded fine but rendered no channel rows) returns
    ``no_channels=True`` so the caller skips with a clear reason. A *broken* lazyload — the
    ``/channels`` table never filled or its fragment didn't return 200 — instead returns
    ``ok=False`` with ``no_channels=False`` so the test FAILS: that is the very regression
    this module exists to catch, and turning it into a skip would hide it (Codex review).
    """
    from playwright.sync_api import Error as PlaywrightError

    base = base_url.rstrip("/")
    table_status: int | None = None
    try:
        with page.expect_response(
            lambda r: CHANNELS_FRAGMENT_URL in r.url, timeout=HTMX_WAIT_TIMEOUT_MS
        ) as resp_info:
            _goto(page, f"{base}{CHANNELS_PATH}")
        table_status = resp_info.value.status
    except PlaywrightError:
        _goto(page, f"{base}{CHANNELS_PATH}")
    _landed_off(page, CHANNELS_PATH)  # bounce-to-login guard
    table_filled = _wait_filled(page, "[hx-get^='/channels/fragments/list']")
    if not table_filled or table_status != 200:
        # A failed prerequisite lazyload — NOT an empty DB. Fail loudly (no_channels stays
        # False) instead of skipping, so a regression in /channels' own lazyload is caught.
        reason = "still showing skeleton" if not table_filled else f"fragment status={table_status}"
        return CollectOobResult(
            channel_pk=None,
            desktop_disabled=False,
            mobile_disabled=False,
            no_channels=False,
            detail=f"/channels table lazyload failed ({reason}) — cannot reach the collect buttons",
        )
    pk = int(page.evaluate(_JS_FIRST_PK))
    if pk < 0:
        # Table loaded successfully but holds no channel rows: a genuine empty DB → skip.
        return CollectOobResult(
            channel_pk=None,
            desktop_disabled=False,
            mobile_disabled=False,
            no_channels=True,
            detail="no channels in DB — OOB collect needs real channel data",
        )
    # Click the desktop button (the mobile cards are hidden on a wide viewport, which is
    # Playwright's default ~1280px, so the desktop button is the visible/clickable one).
    page.locator(f"#collect-btn-{pk} button").click()
    both = False
    try:
        page.wait_for_function(_JS_BOTH_DISABLED, arg=pk, timeout=HTMX_WAIT_TIMEOUT_MS)
        both = True
    except PlaywrightError:
        both = False
    # Re-read each side individually so a partial OOB swap reports which half failed.
    desktop = both or bool(page.evaluate(_JS_BTN_DISABLED, f"#collect-btn-{pk}"))
    mobile = both or bool(page.evaluate(_JS_BTN_DISABLED, f"#collect-btn-m-{pk}"))
    detail = "" if (desktop and mobile) else f"OOB swap incomplete (desktop={desktop}, mobile={mobile})"
    return CollectOobResult(
        channel_pk=pk,
        desktop_disabled=desktop,
        mobile_disabled=mobile,
        detail=detail,
    )


def run_htmx_checks(
    base_url: str,
    password: str | None = None,
    *,
    headless: bool = True,
    timeout_ms: int = HTMX_WAIT_TIMEOUT_MS,
) -> HtmxCheckResults:
    """Open a browser, (log in,) run every functional HTMX check, return the results.

    The browser is always closed (even on error) via ``browser_session``, so a failed
    run leaves no zombie process — mirroring ``console_smoke.run_smoke``. A fresh page is
    used per check so each starts from a clean navigation. The login cookie is set on the
    shared context, so every later page is authenticated.
    """
    base = base_url.rstrip("/")
    with browser_session(headless=headless) as browser:
        context = browser.new_context()
        context.set_default_timeout(timeout_ms)
        context.set_default_navigation_timeout(timeout_ms)
        try:
            if password:
                login(context.new_page(), base, password)
            lazyload = [check_lazyload(context.new_page(), base, spec) for spec in LAZYLOAD_SPECS]
            rate = check_rate_swap(context.new_page(), base)
            collect = check_collect_oob(context.new_page(), base)
            return HtmxCheckResults(lazyload=lazyload, rate=rate, collect=collect)
        finally:
            context.close()


def format_summary(results: HtmxCheckResults) -> str:
    """Render a human-readable summary of every functional check."""
    lines: list[str] = ["Lazyload:"]
    for r in results.lazyload:
        mark = "✓" if r.ok else "✗"
        lines.append(
            f"  {mark} {r.path}  filled={r.filled} marker={r.marker_present} status={r.fragment_status}"
            + (f"  — {r.detail}" if r.detail else "")
        )
    rate = results.rate
    rate_line = f"Rate swap (#999): {'✓' if rate.ok else '✗'} kind={rate.kind} post={rate.post_status}"
    lines.append(rate_line + (f"  — {rate.detail}" if rate.detail else ""))
    collect = results.collect
    lines.append(
        f"Collect OOB: {'✓' if collect.ok else '✗'} pk={collect.channel_pk} "
        f"desktop={collect.desktop_disabled} mobile={collect.mobile_disabled}"
        + (f"  — {collect.detail}" if collect.detail else "")
    )
    return "\n".join(lines)
