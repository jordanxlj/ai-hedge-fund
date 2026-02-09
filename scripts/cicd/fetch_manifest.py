import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RepoSpec:
    url: str
    ref: str
    path: str
    depth: int | None = 1


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


def _authed_url(url: str) -> str:
    """
    If GITHUB_TOKEN exists and the url is https://github.com/<org>/<repo>.git,
    rewrite to token-based URL so private repos can be cloned in CI.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        return url
    prefix = "https://github.com/"
    if not url.startswith(prefix):
        return url
    # Avoid duplicating credentials if already present.
    if "@" in url.split("://", 1)[-1].split("/", 1)[0]:
        return url
    return url.replace("https://", f"https://x-access-token:{token}@")


def parse_manifest(data: dict[str, Any]) -> list[RepoSpec]:
    repos_raw = data.get("repos")
    if not isinstance(repos_raw, list):
        raise ValueError("manifest must contain a top-level 'repos' list")
    repos: list[RepoSpec] = []
    for i, item in enumerate(repos_raw):
        if not isinstance(item, dict):
            raise ValueError(f"repos[{i}] must be an object")
        url = item.get("url")
        ref = item.get("ref")
        path = item.get("path")
        depth = item.get("depth", 1)
        if not isinstance(url, str) or not url:
            raise ValueError(f"repos[{i}].url must be a non-empty string")
        if not isinstance(ref, str) or not ref:
            raise ValueError(f"repos[{i}].ref must be a non-empty string")
        if not isinstance(path, str) or not path:
            raise ValueError(f"repos[{i}].path must be a non-empty string")
        if depth is not None and not isinstance(depth, int):
            raise ValueError(f"repos[{i}].depth must be an int or null")
        repos.append(RepoSpec(url=url, ref=ref, path=path, depth=depth))
    return repos


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/cicd/fetch_manifest.py <manifest.json>", file=sys.stderr)
        return 2

    manifest_path = Path(sys.argv[1]).resolve()
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    repos = parse_manifest(data)

    for repo in repos:
        target = Path(repo.path)
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and any(target.iterdir()):
            # If already cloned, just fetch and checkout.
            _run(["git", "fetch", "--tags", "--prune"], cwd=target)
        else:
            clone_cmd = ["git", "clone"]
            if repo.depth is not None:
                clone_cmd += ["--depth", str(repo.depth)]
            clone_cmd += [_authed_url(repo.url), str(target)]
            _run(clone_cmd)

        _run(["git", "checkout", repo.ref], cwd=target)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

