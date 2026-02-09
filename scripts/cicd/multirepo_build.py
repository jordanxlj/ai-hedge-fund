#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable


def load_config(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        print(f"Config not found: {path}. Nothing to build.", file=sys.stderr)
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    repos = data.get("repositories", [])
    if not isinstance(repos, list):
        raise ValueError("Config field 'repositories' must be a list.")
    return repos


def clone_repo(repo: str, ref: str | None, dest: Path, token: str | None) -> None:
    if dest.exists():
        print(f"Path already exists, skipping clone: {dest}")
        return
    url = f"https://github.com/{repo}.git"
    args: list[str] = ["git", "clone", "--depth", "1"]
    if ref:
        args += ["--branch", ref]
    if token:
        token_bytes = f"x-access-token:{token}".encode("utf-8")
        header = base64.b64encode(token_bytes).decode("utf-8")
        args = ["git", "-c", f"http.extraheader=AUTHORIZATION: basic {header}"] + args[1:]
    args += [url, str(dest)]
    print(f"Cloning {repo}{'@' + ref if ref else ''} -> {dest}")
    subprocess.run(args, check=True)


def normalize_build_commands(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    raise ValueError("'build' must be a string or list of strings.")


def run_command(cmd: str, cwd: Path) -> float:
    start = time.time()
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True, cwd=str(cwd), env=os.environ.copy())
    return time.time() - start


def print_summary(rows: Iterable[tuple[str, str, float]]) -> None:
    print("\nBuild summary:")
    for repo, cmd, duration in rows:
        print(f"- {repo}: {cmd} ({duration:.2f}s)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build multiple repositories.")
    parser.add_argument(
        "--config",
        default=".github/cicd/multirepo.json",
        help="Path to multi-repo config JSON file.",
    )
    parser.add_argument(
        "--workspace",
        default=".cicd/workspace",
        help="Workspace directory for cloned repositories.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    repos = load_config(config_path)
    if not repos:
        print("No repositories configured. Nothing to build.")
        return 0

    workspace = Path(args.workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    token = os.getenv("CICD_GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    version = os.getenv("CICD_VERSION")
    if version:
        print(f"Using CICD_VERSION={version}")

    summary: list[tuple[str, str, float]] = []

    for entry in repos:
        repo = entry.get("repo")
        if not repo:
            print("Skipping entry without 'repo' field.", file=sys.stderr)
            continue
        ref = entry.get("ref")
        path = entry.get("path") or repo.split("/")[-1]
        dest = workspace / path
        dest.parent.mkdir(parents=True, exist_ok=True)

        clone_repo(repo, ref, dest, token)

        for cmd in normalize_build_commands(entry.get("build")):
            duration = run_command(cmd, dest)
            summary.append((repo, cmd, duration))

    print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
