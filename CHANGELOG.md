# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.10.0] - 2026-04-19

### Added
- TTL-based idle connection eviction in `HttpClient` connection pool (#16)

## [1.9.0] - 2026-04-19

### Fixed
- gRPC wiring: `GrpcConfig` is now loaded unconditionally so gRPC transport always works (#15)

## [1.8.0] - 2026-04-19

### Added
- Retry logic with exponential backoff for transient broker errors (#14)
- Full README documentation covering all client features

## [1.7.0] - 2026-04-18

### Added
- `bin/bump-version` script for managing gem version bumps
- Release gates: unit and integration tests must pass before publishing

## [1.6.0] - 2026-04-18

### Added
- `ClientConfig` validation with descriptive error messages (#12)
- Instrumentation hooks for observability and metrics integration (#12)

### Fixed
- Duplicate `query_timeout_ms` declaration in `ClientConfig`

## [1.5.0] - 2026-04-18

### Added
- Per-request query timeout configuration via `timeoutMs` (#11)

## [1.4.0] - 2026-04-18

### Added
- ZooKeeper-based dynamic broker discovery strategy (#10)

## [1.3.0] - 2026-04-18

### Added
- gRPC transport support (#9)

## [1.2.0] - 2026-04-18

### Changed
- Updated README with new features and usage examples (#8)
- Dropped Ruby 3.1 from CI matrix (#8)

## [1.1.0] - 2026-04-18

### Added
- Custom error types for clearer error handling: `Pinot::BrokerNotFoundError`, `Pinot::TableNotFoundError`, `Pinot::TransportError`, `Pinot::PreparedStatementClosedError`, `Pinot::ConfigurationError` (#3)
- Configurable logging — global logger via `Pinot::Logging.logger=` and per-client logger via `ClientConfig` (#4)
- TLS/certificate configuration via `Pinot::TlsConfig` — supports CA cert, client cert/key, and `insecure_skip_verify` (#5)
- HTTP connection pooling for persistent connections using `Net::HTTP` keep-alive (#6)
- `http_timeout` wired from `ClientConfig` through to `Net::HTTP` open/read/write timeouts (#7)
- Auto minor version release workflow triggered on push to `main`
- `ClientConfig` multistage engine upfront configuration example in README

### Fixed
- `http_timeout` setting was accepted by `ClientConfig` but not applied to the underlying HTTP connection
- Transport specs: stubbed `keep_alive_timeout=` and `start`/`finish` on `Net::HTTP` doubles

### Changed
- Gem published to both RubyGems.org and GitHub Packages from a single release workflow

## [1.0.2] - 2026-04-18

### Changed
- Renamed gem from `ruby-pinot-client` to `pinot-client`

## [1.0.1] - 2026-04-18

### Changed
- Publish gem to RubyGems.org using `RUBYGEMS_API_KEY`
- Publish gem to GitHub Packages on release

## [1.0.0] - 2026-04-18

### Added
- Initial release of the Ruby Apache Pinot client gem
- HTTP JSON transport for communicating with Pinot brokers
- Broker selection strategies: static broker list and controller-based dynamic discovery
- `Pinot.from_broker_list`, `Pinot.from_controller`, and `Pinot.from_config` factory methods
- `ClientConfig` and `ControllerConfig` for structured client configuration
- Parameterized queries via `execute_sql_with_params` using `?` placeholders
- Prepared statements (`Pinot::PreparedStatement`) — thread-safe, reusable across executions
- Typed result accessors on `ResultTable`: `get_string`, `get_int`, `get_long`, `get_float`, `get_double`
- Multistage engine support via `use_multistage_engine` flag
- Query tracing via `open_trace` / `close_trace`
- `BrokerResponse` with stat fields: `time_used_ms`, `num_docs_scanned`, `total_docs`, `exceptions`, `num_groups_limit_reached`
- CI workflow with unit tests
- Integration tests against a live Pinot cluster (Docker-based quickstart)
- README with installation, usage, and API documentation
- Release workflow for GitHub Packages

[Unreleased]: https://github.com/startreedata/ruby-pinot-client/compare/v1.10.0...HEAD
[1.10.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/startreedata/ruby-pinot-client/compare/v1.0.2...v1.1.0
[1.0.2]: https://github.com/startreedata/ruby-pinot-client/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/startreedata/ruby-pinot-client/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/startreedata/ruby-pinot-client/releases/tag/v1.0.0
