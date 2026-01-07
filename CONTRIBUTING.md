# Contributing to Spark Map

Thank you for your interest in contributing to Spark Map! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- Git

### Setting Up Your Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/spark-map.git
cd spark-map

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install in development mode with all dependencies
pip install -e ".[dev,all-llm]"

# Install pre-commit hooks
pre-commit install
```

## Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=spark_map --cov-report=html

# Run specific test file
pytest tests/test_detectors.py

# Run specific test
pytest tests/test_detectors.py::TestSkewDetector::test_detects_skew
```

### Code Quality

```bash
# Run linter
ruff check .

# Run linter with auto-fix
ruff check . --fix

# Run formatter
ruff format .

# Run type checker
mypy spark_map

# Run all checks (what CI runs)
pre-commit run --all-files
```

### Building Documentation

```bash
# Install docs dependencies
pip install -e ".[docs]"

# Build docs locally
mkdocs serve
```

## Project Structure

```
spark_map/
├── core/              # Core analysis logic
│   ├── analyzer.py    # Main analysis entry point
│   ├── parser.py      # Event log parsing
│   ├── findings.py    # Finding data structures
│   ├── report.py      # Report generation
│   └── detectors/     # Bottleneck detectors
├── explain/           # LLM integration
│   ├── base.py        # Abstract interface
│   ├── ollama.py      # Ollama provider
│   ├── openai.py      # OpenAI provider
│   └── anthropic.py   # Anthropic provider
├── cli/               # Command-line interface
├── render/            # Output formatters
└── models/            # Pydantic schemas
```

## Adding a New Detector

1. Create a new file in `spark_map/core/detectors/`:

```python
# spark_map/core/detectors/my_detector.py
from spark_map.core.detectors.base import BaseDetector
from spark_map.core.findings import Finding, MitigationTag, Severity
from spark_map.models.schemas import SparkMetrics

class MyDetector(BaseDetector):
    name = "my-detector"
    description = "Detects XYZ issues"

    def detect(self, metrics: SparkMetrics) -> list[Finding]:
        findings = []
        # Your detection logic here
        return findings
```

2. Register it in `spark_map/core/detectors/__init__.py`:

```python
from spark_map.core.detectors.my_detector import MyDetector

def get_all_detectors():
    return [
        # ... existing detectors
        MyDetector,
    ]
```

3. Add tests in `tests/test_detectors.py`

4. Update the README if the detector is user-facing

## Adding a New LLM Provider

1. Create a new file in `spark_map/explain/`:

```python
# spark_map/explain/my_provider.py
from spark_map.explain.base import LLMProvider

class MyProvider(LLMProvider):
    name = "my-provider"

    def explain_finding(self, finding_summary: dict) -> str:
        # Implementation
        pass

    def summarize(self, analysis_summary: dict) -> str:
        # Implementation
        pass
```

2. Update the CLI in `spark_map/cli/main.py` to support the new provider

3. Add the dependency to `pyproject.toml` under `[project.optional-dependencies]`

## Commit Guidelines

We use conventional commits. Format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(detectors): add GC pressure detector
fix(parser): handle missing task metrics gracefully
docs: update README with new CLI options
```

## Pull Request Process

1. **Fork** the repository and create a branch from `main`
2. **Make your changes** following the coding standards
3. **Add tests** for any new functionality
4. **Update documentation** if needed
5. **Run the full test suite** to ensure nothing is broken
6. **Submit a PR** with a clear description of the changes

### PR Checklist

- [ ] Tests pass locally (`pytest`)
- [ ] Code passes linting (`ruff check .`)
- [ ] Types check (`mypy spark_map`)
- [ ] Documentation updated (if applicable)
- [ ] CHANGELOG.md updated (for user-facing changes)

## Code Style

- Use type hints for all function signatures
- Use Pydantic models for data structures
- Keep functions focused and small
- Write docstrings for public APIs
- Follow existing patterns in the codebase

## Reporting Issues

When reporting issues, please include:

1. **Spark version** and environment (Databricks, EMR, local, etc.)
2. **Spark Map version** (`spark-map --version`)
3. **Minimal reproduction steps**
4. **Expected vs actual behavior**
5. **Relevant event log snippet** (sanitized of sensitive data)

## Getting Help

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
