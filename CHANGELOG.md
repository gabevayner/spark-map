# Changelog

All notable changes to Spark Map will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure
- `pyproject.toml` with modern Python packaging (PEP 621)
- CLI skeleton with Click
- Pydantic models for type-safe schemas

### Changed
- Nothing yet

### Deprecated
- Nothing yet

### Removed
- Nothing yet

### Fixed
- Nothing yet

### Security
- Nothing yet

---

## [0.1.0] - Unreleased

### Added

#### Core Analysis
- Event log parser supporting Spark 3.x JSON format
- Metrics extraction from `SparkListenerTaskEnd`, `SparkListenerStageCompleted` events
- Six bottleneck detectors:
  - **Stage Skew**: Detects when max task time >> median task time
  - **Shuffle Explosion**: Detects when shuffle bytes >> input bytes
  - **Spill to Disk**: Detects memory pressure via spill metrics
  - **Partition Inefficiency**: Detects too many small tasks
  - **I/O Bottleneck**: Detects when read time dominates stage duration
  - **Driver Bottleneck**: Detects large result serialization, scheduling delays

#### LLM Integration
- Pluggable LLM adapter interface
- Ollama integration (local, free, default)
- LLM receives only structured findings (never raw logs)

#### Output Formats
- HTML report with styled findings
- Markdown report
- JSON export for programmatic use
- Terminal output with Rich formatting

#### CLI
- `spark-map analyze` - Main analysis command
- `spark-map doctor` - Verify installation and dependencies
- Support for local file input

#### Python API
- `analyze()` function for library usage
- `Report` object with `.to_html()`, `.to_json()`, `.to_markdown()` methods
- Typed models for all data structures

### Security
- No outbound network calls unless LLM explicitly enabled
- No credential storage
- Read-only operation (never modifies cluster or data)

---

## Version History Summary

| Version | Date | Highlights |
|---------|------|------------|
| 0.1.0 | TBD | Initial release with core analysis and Ollama support |

---

## Upgrade Guide

### Upgrading to 0.1.0

This is the initial release. No upgrade steps required.

---

## Links

- [Full Documentation](https://github.com/yourusername/spark-map#readme)
- [Issue Tracker](https://github.com/yourusername/spark-map/issues)
- [PyPI](https://pypi.org/project/spark-map/)
