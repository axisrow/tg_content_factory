# Changelog

Keep a Changelog format, SemVer.

## [0.1.0] — 2026-10-31

### Added

- Package split out of the core (#1720 wave 3): `tg_agent/web` moved to
  `tg_webui` (routes, templates, static, bootstrap, embedded worker); the web
  tests moved into `packages/tg_webui/tests`.

### Removed

- Compatibility re-exports of core modules (`timing`, `log_handler`,
  `runtime_shims`, the runtime helpers in `bootstrap`, the data-path helpers in
  `paths`): import them from `tg_agent.utils.timing`, `tg_agent.runtime.*` and
  `tg_agent.paths`.

### Changed

- Log categories: the web loggers moved from `tg_agent.web.*` to `tg_webui.*`, and
  the core container bootstrap logs as `tg_agent.runtime.bootstrap` (was the
  historical `tg_agent.web.bootstrap`). Update operator log filters or level
  overrides keyed on the old names.
- `serve`'s password policy (`is_strong_password`) moved to the core
  (`tg_agent.security.password_policy`): it is CLI input validation, so neither
  the core CLI nor its tests need this package for it.

