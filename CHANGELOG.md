# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [0.2.3] - 2026-02-25

### Added
- Added a richer live folder-upload dashboard in `mega-up` with phase and overall progress sections.
- Added CLI subscriptions for hash/check/sync progress events so current activity is visible during long runs.

### Changed
- Improved folder upload phase updates to emit detailed progress for set processing and parallel file uploads.
- Improved process-level callback handling to surface in-phase activity messages (including long analysis/upload steps).
- Bumped project version from `0.2.2` to `0.2.3`.

## [0.2.2] - 2026-02-25

### Fixed
- Adapted managed account refresh/session handling to the new `MegaClient` namespace API (`client.account.*`) to avoid legacy `client.start()`/`client.get_account_info()` failures.
- Updated managed storage path and lookup operations to use namespace-aware APIs (`client.tree.*`, `client.files.*`) with safe legacy fallback behavior.
- Fixed account-space refresh errors caused by invoking removed top-level methods on newer `megapy` versions.

### Changed
- Bumped project version from `0.2.1` to `0.2.2`.

## [0.2.1] - 2026-02-25

### Fixed
- Fixed `mega-up --help` crashing on environments where `megapy` was not installed.
- Removed a hard runtime import of `megapy` from `uploader.services.storage` by moving it to type-checking only.

### Changed
- Added `megapy` and `mega-account` as direct project dependencies so `pipx install` includes required runtime packages for CLI usage.
- Bumped project version from `0.2.0` to `0.2.1`.

## [0.2.0] - 2026-02-25

### Added
- Added integrated CLI command `mega-up` from the same package.
- Added rich terminal UI for upload progress and startup configuration summary.
- Added support for loading environment variables from `.env` or `--env-file`.
- Added CLI tests covering destination normalization, env loading, and logging behavior.

### Changed
- Renamed package metadata from `uploader` to `mega-media-uploader` (import path stays `uploader` for compatibility).
- Updated default CLI logging behavior to silent mode unless `--debug` or `--log-level` is provided.
- Bumped project version from `0.1.0` to `0.2.0`.
