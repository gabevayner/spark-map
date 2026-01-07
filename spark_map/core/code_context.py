"""
Code context extraction for LLM-powered analysis.

Extracts source code snippets from Spark stage names to give
the LLM actual code context for its explanations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CodeContext:
    """Source code context for a Spark stage."""

    file_path: str
    line_number: int
    operation: str  # e.g., "join", "groupBy", "read"
    code_snippet: str  # The actual code lines
    start_line: int
    end_line: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "file": self.file_path,
            "line": self.line_number,
            "operation": self.operation,
            "code": self.code_snippet,
            "lines": f"{self.start_line}-{self.end_line}",
        }


def extract_code_location(stage_name: str) -> tuple[str | None, str | None, int | None]:
    """
    Extract file path, operation, and line number from a Spark stage name.

    Spark stage names typically look like:
    - "join at etl_job.py:47"
    - "map at <stdin>:1"
    - "parquet at NativeMethodAccessorImpl.java:0"
    - "count at transform.py:123"

    Returns:
        (operation, file_path, line_number) or (None, None, None) if unparseable
    """
    # Pattern: "operation at file:line"
    pattern = r"^(\w+)\s+at\s+(.+):(\d+)$"
    match = re.match(pattern, stage_name.strip())

    if match:
        operation = match.group(1)
        file_path = match.group(2)
        line_number = int(match.group(3))

        # Skip Java/Scala internals
        if file_path.endswith(".java") or file_path.endswith(".scala"):
            if "spark" in file_path.lower() or "Accessor" in file_path:
                return operation, None, None

        return operation, file_path, line_number

    return None, None, None


def get_code_snippet(
    file_path: str,
    line_number: int,
    context_lines: int = 5,
    source_root: Path | None = None,
) -> CodeContext | None:
    """
    Read source code around the specified line.

    Args:
        file_path: File name from stage name (e.g., "etl_job.py")
        line_number: Line number from stage name
        context_lines: Number of lines to include before/after
        source_root: Root directory to search for source files

    Returns:
        CodeContext with the code snippet, or None if file not found
    """
    # Try to find the file
    resolved_path = _resolve_file_path(file_path, source_root)
    if not resolved_path or not resolved_path.exists():
        return None

    try:
        lines = resolved_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return None

    # Calculate range (1-indexed to 0-indexed)
    start = max(0, line_number - context_lines - 1)
    end = min(len(lines), line_number + context_lines)

    # Build snippet with line numbers
    snippet_lines = []
    for i in range(start, end):
        line_num = i + 1
        marker = "→ " if line_num == line_number else "  "
        snippet_lines.append(f"{marker}{line_num:4d}│ {lines[i]}")

    # Extract operation from stage name pattern
    operation = _infer_operation(lines[line_number - 1] if line_number <= len(lines) else "")

    return CodeContext(
        file_path=str(resolved_path),
        line_number=line_number,
        operation=operation,
        code_snippet="\n".join(snippet_lines),
        start_line=start + 1,
        end_line=end,
    )


def _resolve_file_path(file_path: str, source_root: Path | None) -> Path | None:
    """
    Try to resolve a file path to an actual file.

    Searches in:
    1. Exact path
    2. source_root / file_path
    3. source_root / **/ file_path (recursive)
    4. Current directory / file_path
    """
    # Skip stdin and other special paths
    if file_path.startswith("<") or file_path == "stdin":
        return None

    path = Path(file_path)

    # Try exact path
    if path.exists():
        return path

    # Try relative to source root
    if source_root:
        candidate = source_root / file_path
        if candidate.exists():
            return candidate

        # Try recursive search
        matches = list(source_root.rglob(path.name))
        if len(matches) == 1:
            return matches[0]

    # Try current directory
    candidate = Path.cwd() / file_path
    if candidate.exists():
        return candidate

    return None


def _infer_operation(code_line: str) -> str:
    """Infer the Spark operation from a line of code."""
    operations = [
        "join",
        "groupBy",
        "groupByKey",
        "reduceByKey",
        "aggregateByKey",
        "repartition",
        "coalesce",
        "collect",
        "count",
        "take",
        "read",
        "write",
        "filter",
        "map",
        "flatMap",
        "distinct",
        "union",
        "sort",
        "orderBy",
        "cache",
        "persist",
        "broadcast",
    ]

    code_lower = code_line.lower()
    for op in operations:
        if op.lower() in code_lower:
            return op

    return "unknown"


def enrich_findings_with_code(
    findings: list[dict[str, Any]],
    stages: list[Any],  # StageMetrics
    source_root: Path | None = None,
) -> list[dict[str, Any]]:
    """
    Add code context to findings based on stage names.

    Args:
        findings: List of finding dicts
        stages: List of StageMetrics with stage_name
        source_root: Root directory to search for source files

    Returns:
        Findings enriched with 'code_context' field
    """
    # Build stage_id -> stage_name mapping
    stage_names = {s.stage_id: s.stage_name for s in stages}

    enriched = []
    for finding in findings:
        finding = dict(finding)  # Copy

        # Get code context for affected stages
        code_contexts = []
        for stage_id in finding.get("stage_ids", []):
            stage_name = stage_names.get(stage_id, "")
            operation, file_path, line_number = extract_code_location(stage_name)

            if file_path and line_number:
                ctx = get_code_snippet(file_path, line_number, source_root=source_root)
                if ctx:
                    code_contexts.append(ctx.to_dict())

        if code_contexts:
            finding["code_context"] = code_contexts

        enriched.append(finding)

    return enriched


# Template for LLM prompt with code context
CODE_AWARE_PROMPT = """You are a Spark performance expert. Analyze this performance issue WITH the actual source code.

## Finding
Title: {title}
Severity: {severity}
Affected Stages: {stage_ids}

## Technical Details
{description}

## Source Code
{code_snippets}

## Your Task
1. Explain what the code is doing at the problematic line
2. Explain WHY this causes the performance issue
3. Show a concrete code fix (not just a suggestion)

Be specific. Reference the actual variable names and operations in the code.
"""
