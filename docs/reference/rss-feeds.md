# RSS / Atom feeds

The web app exposes two read-only syndication endpoints:

- `GET /rss.xml` — RSS 2.0 feed of recently collected messages
- `GET /atom.xml` — Atom 1.0 feed of the same content

## Public by design

Both endpoints are **intentionally unauthenticated**. They are listed in
`is_public_path()` (`src/web/panel_auth.py`), so the Basic-auth middleware lets
them through without a session.

This is deliberate: RSS/Atom readers cannot perform interactive HTTP Basic auth,
so requiring a session would make the feeds unusable by their intended clients.

## Security implication

Because the feeds are public, the **message text they contain is world-readable**
to anyone who can reach the web app. If feed content must stay confidential:

- keep the deployment private (e.g. on a VPN / internal network), or
- front the app with a reverse proxy that enforces a feed token / IP allow-list,
  or
- do not expose the panel publicly.

The decision is pinned by tests in `tests/test_panel_auth.py`
(`is_public_path("/rss.xml")` / `is_public_path("/atom.xml")` are asserted to be
`True`) so it cannot be changed silently.
