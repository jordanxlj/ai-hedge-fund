#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore


def read_pyproject_version() -> str | None:
    path = Path("pyproject.toml")
    if not path.exists():
        return None
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return data.get("tool", {}).get("poetry", {}).get("version")


def read_package_version() -> str | None:
    path = Path("package.json")
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("version")


def main() -> int:
    version = read_pyproject_version() or read_package_version()
    if not version:
        print("0.0.0", end="")
        return 0
    print(version, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
