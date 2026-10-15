"""Entry point: python -m llm_proxy (env config, see .env.example)."""
from .config import ConfigError, load
from .server import run

if __name__ == "__main__":
    import os
    import sys

    try:
        run(load())
    except ConfigError as err:
        print(f"llm_proxy: {err}", file=sys.stderr)
        sys.exit(1)
