import json
import sys
from pathlib import Path

from src.api.app import app


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: export_openapi.py <output>", file=sys.stderr)
        return 2
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
