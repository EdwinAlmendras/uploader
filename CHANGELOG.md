# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

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
