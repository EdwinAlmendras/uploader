# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [0.2.9] - 2026-02-25

### Added
- Added a live timeline line stream in folder uploads with timestamped events for completed/failed files, completed/failed sets, and DB sync actions.

### Changed
- Timeline file events now include human-readable sizes (KB/MB/GB) when available.
- Bumped project version from `0.2.8` to `0.2.9`.

## [0.2.8] - 2026-02-25

### Fixed
- Removed unsupported `include_embedding` argument from `uploader` photo analysis calls to match current `mediakit.analyze_photo` signature.
- Fixed set image analysis failures caused by `TypeError: analyze_photo() got an unexpected keyword argument 'include_embedding'`.

### Changed
- Bumped project version from `0.2.7` to `0.2.8`.

## [0.2.7] - 2026-02-25

### Fixed
- Restored MEGA-first existence checks in the active folder workflow for individual files.
- Added MEGA->DB metadata synchronization when a file already exists in MEGA but is missing in datastore.
- Prevented unnecessary re-uploads in MEGA-present/DB-missing cases by syncing metadata first and only uploading truly pending files.

### Changed
- Included MEGA-path skips in total skip accounting and progress summary for folder runs.
- Bumped project version from `0.2.6` to `0.2.7`.

## [0.2.6] - 2026-02-25

### Added
- Added per-run detached log files by default (`run` and `error`) even when console logging mode is `silent`.
- Added log file paths to the startup configuration summary.

### Changed
- Improved folder/file failure reporting to print `ERROR Cause:` and failed item counts on CLI failures.
- Improved fallback progress text units from raw bytes to human-readable units (KB/MB/GB).
- Bumped project version from `0.2.5` to `0.2.6`.

## [0.2.5] - 2026-02-25

### Fixed
- Fixed folder CLI `Overall` progress denominator being overwritten to `100` by percent-style phase callbacks.
- Kept `total_files` as the real workload size while preserving callback-level progress fields for live phase display.

### Changed
- Bumped project version from `0.2.4` to `0.2.5`.

## [0.2.4] - 2026-02-25

### Changed
- Increased `source_id` length from 12 to 16 characters specifically for image documents generated inside set processing.
- Bumped project version from `0.2.3` to `0.2.4`.

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
