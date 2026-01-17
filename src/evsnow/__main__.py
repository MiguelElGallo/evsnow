"""Entry point for running EvSnow as a module.

Usage:
    python -m evsnow [COMMAND] [OPTIONS]

This delegates to the Typer CLI defined in the top-level `main` module.
"""

from main import cli_main


def main() -> None:
    cli_main()


if __name__ == "__main__":
    main()
