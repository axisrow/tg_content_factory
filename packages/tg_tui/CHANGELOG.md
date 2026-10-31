# Changelog

Keep a Changelog format, SemVer.

## [0.1.0] — 2026-10-31

### Added

- Package split out of the core (#1720 wave 2): `tg_agent/cli/commands/agent_tui.py`
  and `agent_tui.tcss` moved verbatim to `tg_tui.app` / `tg_tui/agent_tui.tcss`;
  the TUI tests moved into `packages/tg_tui/tests`.
