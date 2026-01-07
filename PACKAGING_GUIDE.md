# Python Packaging Guide for Spark Map

This guide explains the packaging setup for Spark Map and key concepts for Python packages.

## Table of Contents

1. [Modern Python Packaging](#modern-python-packaging)
2. [pyproject.toml Explained](#pyprojecttoml-explained)
3. [Package Structure](#package-structure)
4. [Entry Points (CLI)](#entry-points-cli)
5. [Optional Dependencies](#optional-dependencies)
6. [Development Workflow](#development-workflow)
7. [Publishing to PyPI](#publishing-to-pypi)

---

## Modern Python Packaging

Python packaging has evolved significantly. The modern approach uses:

- **`pyproject.toml`** - Single configuration file (replaces `setup.py`, `setup.cfg`, `requirements.txt`)
- **PEP 621** - Standard for project metadata
- **Build backends** - Tools like `hatchling`, `setuptools`, `flit` that build your package

### Why pyproject.toml?

```
Old way (multiple files):
├── setup.py          # Build script
├── setup.cfg         # Metadata
├── requirements.txt  # Dependencies
├── MANIFEST.in       # File inclusion
└── tox.ini          # Tool configs

New way (one file):
└── pyproject.toml   # Everything!
```

---

## pyproject.toml Explained

### Build System

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

This tells pip how to build your package. `hatchling` is a modern, fast build backend.

### Project Metadata

```toml
[project]
name = "spark-map"                    # Package name on PyPI
version = "0.1.0"                     # Semantic version
description = "..."                   # Short description
readme = "README.md"                  # Long description file
license = "MIT"
requires-python = ">=3.9"             # Minimum Python version
authors = [{ name = "...", email = "..." }]
keywords = ["spark", "performance"]   # For PyPI search
classifiers = [...]                   # PyPI categories
dependencies = [...]                  # Required packages
```

### Classifiers

Classifiers are standardized tags for PyPI:

```toml
classifiers = [
    "Development Status :: 3 - Alpha",           # Maturity level
    "Environment :: Console",                     # Where it runs
    "Intended Audience :: Developers",            # Who it's for
    "License :: OSI Approved :: MIT License",     # License type
    "Programming Language :: Python :: 3.9",     # Python versions
    "Topic :: System :: Monitoring",              # Category
    "Typing :: Typed",                            # Has type hints
]
```

Development status options:
- `1 - Planning`
- `2 - Pre-Alpha`
- `3 - Alpha`
- `4 - Beta`
- `5 - Production/Stable`

---

## Package Structure

### The `__init__.py` Files

Every directory that should be a Python package needs an `__init__.py`:

```
spark_map/
├── __init__.py           # Makes spark_map a package
├── core/
│   ├── __init__.py       # Makes spark_map.core a package
│   └── parser.py
```

### What goes in `__init__.py`?

```python
# spark_map/__init__.py

# Version - important for `spark-map --version`
__version__ = "0.1.0"

# Public API - what users import
from spark_map.core.analyzer import analyze
from spark_map.core.report import Report

# Control what `from spark_map import *` exports
__all__ = ["analyze", "Report", "__version__"]
```

### Import Patterns

```python
# Users can do:
from spark_map import analyze              # Clean, recommended
from spark_map.core.analyzer import analyze  # Also works

# Because __init__.py re-exports it
```

---

## Entry Points (CLI)

### How `spark-map` Command Works

In `pyproject.toml`:

```toml
[project.scripts]
spark-map = "spark_map.cli.main:cli"
```

This means:
- When user types `spark-map`, Python runs...
- The `cli` function from `spark_map/cli/main.py`

### The CLI Function

```python
# spark_map/cli/main.py
import click

@click.group()
def cli():
    """Main entry point."""
    pass

@cli.command()
def analyze():
    """Analyze command."""
    pass
```

### How It's Installed

When you run `pip install spark-map`:

1. pip reads `[project.scripts]`
2. Creates an executable script in your PATH
3. Script calls `spark_map.cli.main:cli()`

---

## Optional Dependencies

### Defining Optional Deps

```toml
[project.optional-dependencies]
# LLM providers
ollama = ["ollama>=0.1.0"]
openai = ["openai>=1.0"]
all-llm = ["ollama>=0.1.0", "openai>=1.0"]

# Development
dev = ["pytest>=7.0", "ruff>=0.1.0"]
```

### Installing Optional Deps

```bash
pip install spark-map              # Core only
pip install spark-map[ollama]      # Core + Ollama
pip install spark-map[all-llm]     # Core + all LLM providers
pip install spark-map[dev]         # Core + dev tools
pip install "spark-map[ollama,dev]"  # Multiple extras
```

### Handling Missing Optional Deps

```python
# In your code
def get_ollama_provider():
    try:
        import ollama
        return OllamaProvider()
    except ImportError:
        raise ImportError(
            "Ollama not installed. Run: pip install spark-map[ollama]"
        )
```

---

## Development Workflow

### Editable Install

```bash
pip install -e ".[dev]"
```

The `-e` flag means "editable" - changes to source files take effect immediately without reinstalling.

### Version Management

Keep version in one place:

```python
# spark_map/__init__.py
__version__ = "0.1.0"
```

Reference it elsewhere:

```python
# CLI
from spark_map import __version__

@click.version_option(version=__version__)
def cli():
    pass
```

### Pre-commit Hooks

`.pre-commit-config.yaml` runs checks before each commit:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    hooks:
      - id: ruff        # Linting
      - id: ruff-format # Formatting
```

Install with: `pre-commit install`

---

## Publishing to PyPI

### 1. Build the Package

```bash
pip install build
python -m build
```

Creates:
```
dist/
├── spark_map-0.1.0-py3-none-any.whl   # Wheel (preferred)
└── spark_map-0.1.0.tar.gz              # Source distribution
```

### 2. Upload to Test PyPI (Practice)

```bash
pip install twine
twine upload --repository testpypi dist/*
```

Test install:
```bash
pip install --index-url https://test.pypi.org/simple/ spark-map
```

### 3. Upload to Real PyPI

```bash
twine upload dist/*
```

### 4. Users Can Install

```bash
pip install spark-map
```

### PyPI Account Setup

1. Create account at https://pypi.org/account/register/
2. Enable 2FA (required for new projects)
3. Create API token at https://pypi.org/manage/account/token/
4. Configure `~/.pypirc`:

```ini
[pypi]
username = __token__
password = pypi-your-token-here
```

---

## Common Issues

### "Module not found" after install

Check that all `__init__.py` files exist:
```bash
find spark_map -name "*.py" -type f
```

### CLI command not found

Ensure entry point is correct:
```toml
[project.scripts]
spark-map = "spark_map.cli.main:cli"  # module:function
```

### Import errors in tests

Install in editable mode:
```bash
pip install -e ".[dev]"
```

### Version mismatch

Keep version in one place (`__init__.py`) and reference it.

---

## Quick Reference

| Task | Command |
|------|---------|
| Install for development | `pip install -e ".[dev]"` |
| Run tests | `pytest` |
| Run linter | `ruff check .` |
| Format code | `ruff format .` |
| Type check | `mypy spark_map` |
| Build package | `python -m build` |
| Upload to PyPI | `twine upload dist/*` |

---

## Resources

- [Python Packaging User Guide](https://packaging.python.org/)
- [PEP 621 - Project Metadata](https://peps.python.org/pep-0621/)
- [Hatch Documentation](https://hatch.pypa.io/)
- [Click Documentation](https://click.palletsprojects.com/)
