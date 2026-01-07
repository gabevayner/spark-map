# Spark Map

[![PyPI version](https://badge.fury.io/py/spark-map.svg)](https://badge.fury.io/py/spark-map)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Analyze Spark event logs to identify performance bottlenecks and explain them clearly.**

Spark Map is a Python CLI and library that reads Spark event logs, detects performance issues using deterministic rules, and optionally generates human-readable explanations using LLMs.

## Philosophy

- **Deterministic first, AI second** — All bottlenecks are detected using metrics and rules. LLMs only explain findings.
- **Zero-trust by default** — No data leaves your environment unless explicitly configured.
- **Works everywhere Spark runs** — Databricks, EMR, Kubernetes, local.

## Installation

```bash
# Core installation (no LLM dependencies)
pip install spark-map

# With Ollama support (local LLM, recommended)
pip install spark-map[ollama]

# With OpenAI support
pip install spark-map[openai]

# With all LLM providers
pip install spark-map[all-llm]

# With cloud storage support (S3, Azure, GCS)
pip install spark-map[s3]
pip install spark-map[all-cloud]

# Everything
pip install spark-map[all-llm,all-cloud]
```

## Quick Start

### CLI Usage

```bash
# Analyze a local event log
spark-map analyze --event-log ./spark-eventlog.json

# Output to HTML report
spark-map analyze --event-log ./spark-eventlog.json --out report.html

# With LLM explanations (Ollama - local, free)
spark-map analyze --event-log ./spark-eventlog.json --llm ollama

# With LLM explanations (OpenAI)
spark-map analyze --event-log ./spark-eventlog.json --llm openai --llm-api-key $OPENAI_API_KEY

# Compare two runs
spark-map diff --before old-eventlog.json --after new-eventlog.json

# Check your setup
spark-map doctor
```

### Python API

```python
from spark_map import analyze

# Basic analysis
report = analyze("path/to/eventlog.json")

# Print findings
for finding in report.findings:
    print(f"[{finding.severity}] {finding.title}")
    print(f"  Stage: {finding.stage_id}")
    print(f"  {finding.description}")

# Export to HTML
report.to_html("report.html")

# Export to JSON (for programmatic use)
report.to_json("report.json")

# Get raw metrics
print(report.metrics.total_duration_ms)
print(report.metrics.stages[0].shuffle_write_bytes)
```

## What Spark Map Detects

| Issue | What It Means | How It's Detected |
|-------|---------------|-------------------|
| **Stage Skew** | Some tasks take much longer than others | max task time >> median task time |
| **Shuffle Explosion** | Shuffle data is huge relative to input | shuffle bytes >> input bytes |
| **Spill to Disk** | Memory pressure forcing disk writes | spill metrics in task data |
| **Partition Inefficiency** | Too many small tasks | high task count + low per-task runtime |
| **I/O Bottleneck** | Reading data dominates execution | read time >> compute time |
| **Driver Bottleneck** | Driver is the limiting factor | large result serialization, scheduling delays |

Each finding includes:
- **Severity**: `critical`, `warning`, `info`
- **Affected stage(s)**
- **Raw metrics** that triggered detection
- **Mitigation tags** (e.g., `repartition`, `broadcast-join`, `increase-memory`)

## LLM Integration

LLMs are **optional** and only used to explain findings — they never detect problems.

### Supported Providers

| Provider | Model | Setup |
|----------|-------|-------|
| **Ollama** (default) | codellama, llama2, mistral | [Install Ollama](https://ollama.ai), run `ollama pull codellama` |
| **OpenAI** | gpt-4o, gpt-4-turbo, gpt-3.5-turbo | Set `--llm-api-key` or `OPENAI_API_KEY` |
| **Anthropic** | claude-3-opus, claude-3-sonnet | Set `--llm-api-key` or `ANTHROPIC_API_KEY` |

### LLM Contract

The LLM receives only:
- Verified metrics
- Detected bottlenecks
- Severity scores
- Suggested mitigation tags

The LLM returns:
- Plain English explanation
- Prioritized recommendations
- **No new facts** (it cannot invent problems)

## Configuration

Create a `.sparkmaprc` file in your project or home directory:

```yaml
# .sparkmaprc
llm:
  provider: ollama
  model: codellama:7b-instruct

thresholds:
  skew_ratio: 10.0          # max/median task time ratio
  shuffle_explosion: 5.0     # shuffle/input size ratio
  min_spill_mb: 100          # minimum spill to flag

output:
  format: html
  include_raw_metrics: false
```

Or use environment variables:

```bash
export SPARK_MAP_LLM_PROVIDER=ollama
export SPARK_MAP_LLM_MODEL=codellama:7b-instruct
```

## Supported Input Sources

| Source | Example | Extra Install |
|--------|---------|---------------|
| Local file | `./eventlog.json` | None |
| S3 | `s3://bucket/path/eventlog.json` | `pip install spark-map[s3]` |
| Azure Blob | `az://container/path/eventlog.json` | `pip install spark-map[azure]` |
| GCS | `gs://bucket/path/eventlog.json` | `pip install spark-map[gcs]` |
| DBFS | `dbfs:/path/eventlog.json` | Via Databricks runtime |

## Output Formats

```bash
# HTML report (default)
spark-map analyze --event-log log.json --out report.html

# Markdown
spark-map analyze --event-log log.json --out report.md --format markdown

# JSON (for CI/programmatic use)
spark-map analyze --event-log log.json --out report.json --format json

# Stdout (quick look)
spark-map analyze --event-log log.json
```

## Development

```bash
# Clone the repo
git clone https://github.com/yourusername/spark-map.git
cd spark-map

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in development mode with all dependencies
pip install -e ".[dev,all-llm]"

# Run tests
pytest

# Run linter
ruff check .

# Run type checker
mypy spark_map

# Run all checks (what CI runs)
pre-commit run --all-files
```

## Project Structure

```
spark_map/
├── core/
│   ├── parser.py          # Event log parsing
│   ├── metrics.py         # Metrics extraction
│   ├── detectors/         # Bottleneck detection rules
│   │   ├── base.py
│   │   ├── skew.py
│   │   ├── shuffle.py
│   │   ├── spill.py
│   │   ├── partition.py
│   │   ├── io.py
│   │   └── driver.py
│   └── findings.py        # Finding data structures
├── explain/
│   ├── base.py            # LLM interface
│   ├── ollama.py
│   ├── openai.py
│   └── anthropic.py
├── cli/
│   ├── main.py            # CLI entry point
│   ├── analyze.py
│   ├── diff.py
│   └── doctor.py
├── render/
│   ├── html.py
│   ├── markdown.py
│   └── json.py
└── models/
    └── schemas.py         # Pydantic models
```

## Safety Guarantees

- **Read-only** — Never modifies your cluster or data
- **No credentials stored** — API keys used only for the current session
- **No outbound calls** — Unless you explicitly enable LLM providers
- **Deterministic core** — Same input always produces same findings (sans LLM)

## Roadmap

See [CHANGELOG.md](CHANGELOG.md) for version history.

### v0.1 (Current)
- [x] Event log parsing
- [x] Core 6 detectors
- [x] HTML/Markdown/JSON output
- [x] Ollama integration
- [x] CLI interface

### v0.2 (Planned)
- [ ] OpenAI and Anthropic adapters
- [ ] S3/Azure/GCS input support
- [ ] `spark-map diff` command
- [ ] Configurable thresholds

### Future
- [ ] Historical trend analysis
- [ ] Databricks job auto-discovery
- [ ] CI integration helpers

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
