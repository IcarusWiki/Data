#!/usr/bin/env python3
"""
Collect downloaded per-pak consumer artifacts into a consumer/finalizer staging
directory for one finalizer job.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare merged consumer artifacts for a finalizer job.",
    )
    parser.add_argument("--download-root", type=Path, required=True)
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument("--artifact-ids-json", required=True)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args()


def load_artifact_ids(raw_json: str) -> list[str]:
    try:
        loaded = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        fail(f"Unable to parse artifact ids JSON: {exc}")
    if not isinstance(loaded, list) or not loaded:
        fail("artifact-ids-json must decode to a non-empty list")
    artifact_ids: list[str] = []
    for index, item in enumerate(loaded, start=1):
        if not isinstance(item, str) or not item:
            fail(f"artifact-ids-json[{index}] must be a non-empty string")
        artifact_ids.append(item)
    return artifact_ids


def write_github_output(path: Path, *, has_artifacts: bool, artifact_dir: Path) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(f"has_artifacts={'true' if has_artifacts else 'false'}\n")
        handle.write(f"artifact_dir={artifact_dir.resolve()}\n")


def main() -> None:
    args = parse_args()
    artifact_ids = load_artifact_ids(args.artifact_ids_json)

    download_root = args.download_root.resolve()
    out_root = args.out_root.resolve()
    if out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    has_artifacts = False
    for artifact_id in artifact_ids:
        destinations_root = out_root / artifact_id
        for source_dir in sorted(download_root.glob(f"*/{args.consumer_id}/{artifact_id}")):
            if not source_dir.is_dir():
                continue
            pak_slug = source_dir.parent.parent.name
            destination = destinations_root / pak_slug
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_dir, destination)
            has_artifacts = True

    if args.github_output:
        write_github_output(
            args.github_output,
            has_artifacts=has_artifacts,
            artifact_dir=out_root,
        )

    print(
        f"Prepared finalizer artifacts for {args.consumer_id}: "
        f"{'found' if has_artifacts else 'no'} matching inputs"
    )


if __name__ == "__main__":
    main()
