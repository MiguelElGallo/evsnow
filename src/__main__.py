"""
Entry point for running evsnow as a module.

Usage:
    python -m evsnow [COMMAND] [OPTIONS]
"""

from .main import cli_main

if __name__ == "__main__":
    cli_main()
