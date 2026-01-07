"""Tests for CLI commands."""

from click.testing import CliRunner

from spark_map.cli.main import cli


def test_cli_version():
    """Test --version flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "spark-map" in result.output


def test_cli_help():
    """Test --help flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "analyze" in result.output
    assert "diff" in result.output
    assert "doctor" in result.output


def test_analyze_help():
    """Test analyze --help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--help"])
    assert result.exit_code == 0
    assert "--event-log" in result.output
    assert "--llm" in result.output


def test_doctor_runs():
    """Test that doctor command runs."""
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor"])
    # Should succeed since core deps are installed
    assert result.exit_code == 0
    assert "Dependency Check" in result.output


def test_analyze_missing_file():
    """Test analyze with non-existent file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--event-log", "/nonexistent/file.json"])
    assert result.exit_code != 0
