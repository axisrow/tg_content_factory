"""CLI startup must work on platforms without the Unix resource module."""

import subprocess
import sys

import pytest


@pytest.mark.integration
@pytest.mark.parametrize("flag, expected", [("--version", "tg-agent"), ("--help", "Usage")])
def test_cli_without_resource(flag, expected):
    result = subprocess.run(
        [sys.executable, "-c",
         "import sys, runpy; sys.modules['resource'] = None; "
         "sys.argv = ['tg-agent', sys.argv[1]]; runpy.run_module('tg_agent.main', run_name='__main__')",
         flag],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout
