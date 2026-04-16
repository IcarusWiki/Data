#!/usr/bin/env python3
"""
Stage configured consumer repo paths for commit using a JSON path list.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage configured consumer paths with git add.",
    )
    parser.add_argument("--repo-dir", type=Path, required=True)
    parser.add_argument("--paths-json", required=True)
    return parser.parse_args()


def load_paths(raw_json: str) -> list[str]:
    try:
        loaded = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        fail(f"Unable to parse paths JSON: {exc}")
    if not isinstance(loaded, list) or not loaded:
        fail("paths-json must decode to a non-empty list")
    paths: list[str] = []
    for index, item in enumerate(loaded, start=1):
        if not isinstance(item, str) or not item:
            fail(f"paths-json[{index}] must be a non-empty string")
        paths.append(item)
    return paths


def main() -> None:
    args = parse_args()
    repo_dir = args.repo_dir.resolve()
    if not repo_dir.is_dir():
        fail(f"repo-dir not found: {repo_dir}")

    paths = load_paths(args.paths_json)
    subprocess.run(
        ["git", "-C", str(repo_dir), "add", "--", *paths],
        check=True,
    )
    print(f"Staged {len(paths)} path(s) in {repo_dir}")


if __name__ == "__main__":
    main()
