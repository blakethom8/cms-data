"""Command-line entry point for production release management."""

from .production_manager import main


if __name__ == "__main__":
    raise SystemExit(main())
