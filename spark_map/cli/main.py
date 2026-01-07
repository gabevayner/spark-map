"""
Main CLI entry point for Spark Map.

Usage:
    spark-map analyze --event-log <path>
    spark-map diff --before <path> --after <path>
    spark-map doctor
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from spark_map import __version__

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="spark-map")
def cli() -> None:
    """
    Spark Map - Analyze Spark event logs to identify performance bottlenecks.

    Run 'spark-map analyze --help' for analysis options.
    """
    pass


@cli.command()
@click.option(
    "--event-log",
    "-e",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to Spark event log JSON file",
)
@click.option(
    "--out",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path (format inferred from extension: .html, .md, .json)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["html", "markdown", "json", "text"]),
    default=None,
    help="Output format (default: inferred from --out, or text)",
)
@click.option(
    "--llm",
    type=click.Choice(["none", "ollama", "openai", "anthropic"]),
    default="none",
    help="LLM provider for explanations (default: none)",
)
@click.option(
    "--llm-model",
    default=None,
    help="LLM model to use (default: provider-specific)",
)
@click.option(
    "--llm-api-key",
    envvar=["OPENAI_API_KEY", "ANTHROPIC_API_KEY"],
    default=None,
    help="API key for LLM provider (or set via environment variable)",
)
@click.option(
    "--metrics-only",
    is_flag=True,
    help="Only extract metrics, skip bottleneck detection",
)
def analyze(
    event_log: Path,
    out: Path | None,
    format: str | None,
    llm: str,
    llm_model: str | None,
    llm_api_key: str | None,
    metrics_only: bool,
) -> None:
    """
    Analyze a Spark event log for performance bottlenecks.

    Examples:

        spark-map analyze --event-log ./eventlog.json

        spark-map analyze -e ./eventlog.json --out report.html

        spark-map analyze -e ./eventlog.json --llm ollama
    """
    from spark_map.core.analyzer import analyze as run_analysis

    console.print(f"\n[bold]Spark Map[/bold] v{__version__}\n")

    # Validate event log exists
    if not event_log.exists():
        console.print(f"[red]Error:[/red] Event log not found: {event_log}")
        sys.exit(1)

    console.print(f"Analyzing: [cyan]{event_log}[/cyan]\n")

    # Set up LLM provider if requested
    llm_provider = None
    if llm != "none":
        llm_provider = _create_llm_provider(llm, llm_model, llm_api_key)
        if llm_provider:
            console.print(f"LLM: [green]{llm_provider.name}[/green]\n")

    # Run analysis
    with console.status("[bold green]Analyzing event log..."):
        try:
            report = run_analysis(event_log, llm_provider=llm_provider)
        except Exception as e:
            console.print(f"[red]Error during analysis:[/red] {e}")
            sys.exit(1)

    # Display results
    _display_report(report, metrics_only)

    # Write output file if requested
    if out:
        output_format = format or _infer_format(out)
        _write_output(report, out, output_format)
        console.print(f"\n[green]Report saved to:[/green] {out}")


@cli.command()
@click.option(
    "--before",
    "-b",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to first (before) event log",
)
@click.option(
    "--after",
    "-a",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to second (after) event log",
)
def diff(before: Path, after: Path) -> None:
    """
    Compare two Spark event logs to see changes.

    Example:

        spark-map diff --before old.json --after new.json
    """
    console.print(f"\n[bold]Spark Map Diff[/bold] v{__version__}\n")
    console.print("[yellow]Note:[/yellow] Diff feature coming in v0.2\n")

    # Placeholder for v0.2
    console.print(f"Before: {before}")
    console.print(f"After: {after}")
    console.print("\n[dim]Diff analysis not yet implemented.[/dim]")


@cli.command()
def doctor() -> None:
    """
    Check Spark Map installation and dependencies.

    Verifies that all required and optional dependencies are available.
    """
    console.print(f"\n[bold]Spark Map Doctor[/bold] v{__version__}\n")

    checks = []

    # Core dependencies
    checks.append(_check_import("pydantic", "Pydantic (data validation)"))
    checks.append(_check_import("click", "Click (CLI framework)"))
    checks.append(_check_import("rich", "Rich (terminal output)"))

    # Optional: LLM providers
    checks.append(_check_import("ollama", "Ollama (local LLM)", optional=True))
    checks.append(_check_import("openai", "OpenAI", optional=True))
    checks.append(_check_import("anthropic", "Anthropic", optional=True))

    # Optional: Cloud storage
    checks.append(_check_import("boto3", "boto3 (AWS S3)", optional=True))
    checks.append(_check_import("azure.storage.blob", "Azure Blob Storage", optional=True))
    checks.append(_check_import("google.cloud.storage", "Google Cloud Storage", optional=True))

    # Display results
    table = Table(title="Dependency Check")
    table.add_column("Component", style="cyan")
    table.add_column("Status")
    table.add_column("Required")

    for name, status, required in checks:
        status_str = "[green]OK[/green]" if status else "[red]Missing[/red]"
        req_str = "[yellow]Yes[/yellow]" if required else "No"
        table.add_row(name, status_str, req_str)

    console.print(table)

    # Summary
    required_ok = all(status for _, status, required in checks if required)
    if required_ok:
        console.print("\n[green]All required dependencies installed![/green]")
    else:
        console.print("\n[red]Some required dependencies are missing.[/red]")
        console.print("Run: pip install spark-map")
        sys.exit(1)


def _check_import(module: str, name: str, optional: bool = False) -> tuple[str, bool, bool]:
    """Check if a module can be imported."""
    try:
        __import__(module)
        return (name, True, not optional)
    except ImportError:
        return (name, False, not optional)


def _create_llm_provider(provider: str, model: str | None, api_key: str | None):
    """Create an LLM provider instance."""
    if provider == "ollama":
        try:
            from spark_map.explain.ollama import OllamaProvider

            return OllamaProvider(model=model or "codellama:7b-instruct")
        except ImportError:
            console.print("[yellow]Warning:[/yellow] Ollama not installed. Run: pip install spark-map[ollama]")
            return None

    elif provider == "openai":
        if not api_key:
            console.print("[red]Error:[/red] OpenAI requires --llm-api-key or OPENAI_API_KEY")
            return None
        try:
            from spark_map.explain.openai import OpenAIProvider

            return OpenAIProvider(api_key=api_key, model=model or "gpt-4o")
        except ImportError:
            console.print("[yellow]Warning:[/yellow] OpenAI not installed. Run: pip install spark-map[openai]")
            return None

    elif provider == "anthropic":
        if not api_key:
            console.print("[red]Error:[/red] Anthropic requires --llm-api-key or ANTHROPIC_API_KEY")
            return None
        try:
            from spark_map.explain.anthropic import AnthropicProvider

            return AnthropicProvider(api_key=api_key, model=model or "claude-3-sonnet-20240229")
        except ImportError:
            console.print("[yellow]Warning:[/yellow] Anthropic not installed. Run: pip install spark-map[anthropic]")
            return None

    return None


def _display_report(report, metrics_only: bool) -> None:
    """Display report in terminal."""
    from spark_map.core.findings import Severity

    # Summary panel
    summary_text = (
        f"App: {report.metrics.app_name or report.metrics.app_id}\n"
        f"Duration: {report.metrics.total_duration_ms / 1000:.1f}s\n"
        f"Stages: {report.metrics.num_stages} ({report.metrics.num_failed_stages} failed)\n"
        f"Tasks: {report.metrics.num_tasks} ({report.metrics.num_failed_tasks} failed)"
    )
    console.print(Panel(summary_text, title="Summary", border_style="blue"))

    if metrics_only:
        return

    # Findings
    if not report.findings:
        console.print("\n[green]No performance issues detected![/green]")
        return

    console.print(f"\n[bold]Findings ({len(report.findings)} total)[/bold]\n")

    severity_colors = {
        Severity.CRITICAL: "red",
        Severity.WARNING: "yellow",
        Severity.INFO: "blue",
    }

    for finding in report.findings.sorted_by_severity():
        color = severity_colors.get(finding.severity, "white")
        console.print(f"[{color}][{finding.severity}][/{color}] {finding.title}")
        if finding.stage_ids:
            console.print(f"  [dim]Stages: {finding.stage_ids}[/dim]")
        console.print(f"  {finding.description[:200]}...")
        if finding.mitigation_hint:
            console.print(f"  [green]Hint:[/green] {finding.mitigation_hint}")
        if finding.llm_explanation:
            console.print(f"  [cyan]AI:[/cyan] {finding.llm_explanation[:200]}...")
        console.print()

    # LLM summary
    if report.llm_summary:
        console.print(Panel(report.llm_summary, title="AI Summary", border_style="cyan"))


def _infer_format(path: Path) -> str:
    """Infer output format from file extension."""
    suffix = path.suffix.lower()
    return {
        ".html": "html",
        ".htm": "html",
        ".md": "markdown",
        ".markdown": "markdown",
        ".json": "json",
    }.get(suffix, "text")


def _write_output(report, path: Path, format: str) -> None:
    """Write report to file."""
    if format == "html":
        report.to_html(path)
    elif format == "markdown":
        report.to_markdown(path)
    elif format == "json":
        report.to_json(path)
    else:
        path.write_text(report.summary())


if __name__ == "__main__":
    cli()
