# Changelog

Keep a Changelog format, SemVer. Consumers pin exact versions — the
`tg-messenger` precedent (`>=0.1.0` with no tags/changelog) is not repeated.

## [0.2.0] — 2026-10-26

### Added

- Wave A module migration (#1421, epic #1416 stage 1.2): zero-coupling
  factory modules moved verbatim — `auth`, `session_materializer`,
  `botfather`, `identity`, `media`. The factory now imports them from
  `tg_shared`; `tests/test_auth.py`, `test_session_materializer.py`,
  `test_identity.py` and the botfather unit tests moved into the package.
- Dependencies: `telethon>=1.43.2`, `tenacity>=9.1.4` (mirroring the factory).

## [0.1.0] — 2026-10-21

### Added

- Package scaffold (#1420, epic #1416 stage 1.1): `SharedAccount` model,
  `SharedAccountStore` storage Protocol, `JsonFileStore` default
  implementation for the standalone single-account messenger, `py.typed`.
- No factory code moved yet — wave A (#1421) and wave B (#1422) follow.
