# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/alexrudy/chateau/compare/v0.3.0...v0.3.1) - 2026-01-24

### Other

- Enable all features on docsrs

## [0.3.0](https://github.com/alexrudy/chateau/compare/v0.2.1...v0.3.0) - 2026-01-21

### Other

- Add client manager to changelog.
- Merge pull request #20 from alexrudy/dependabot/cargo/rustls-0.23.35
- Bump rustls from 0.23.29 to 0.23.35
- fix some leftover clippy lints
- Connection Manager
- Implement PoolableConnection for FramedConnection
- Separate connection manager to accompany pool.
- Bump tokio-util from 0.7.15 to 0.7.17
- Remove unused pin! macro import.
- Merge pull request #15 from alexrudy/dependabot/cargo/tokio-1.48.0
- Merge pull request #14 from alexrudy/dependabot/cargo/webpki-roots-1.0.4
- Merge pull request #13 from alexrudy/dependabot/cargo/parking_lot-0.12.5
- Merge pull request #12 from alexrudy/dependabot/cargo/thiserror-2.0.17
- Bump thiserror from 2.0.12 to 2.0.17
- enable dependabot

### Added

- *(client)* Split the connection pool to have a connection manager (for a single host), and a pool for many connections.
- *(chore)* Update dependencies.

## [0.2.1](https://github.com/alexrudy/chateau/compare/v0.2.0...v0.2.1) - 2025-12-10

### Added

- *(client)* Simplify tcp configuration

### Fixed

- Add back the connect_to_addrs module-level function for API

### Other

- Add release-plz to CI
