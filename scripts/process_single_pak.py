#!/usr/bin/env python3
"""
Download, extract, process, and clean up a single Icarus content pak.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from pak_path_utils import validate_pak_name, validate_pak_path, validate_pak_slug

ROOT = Path(__file__).resolve().parent.parent
APPROVE_INTERVAL_SECONDS = 5
APPROVE_ATTEMPTS = 12


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process a single pak end-to-end.",
    )
    parser.add_argument("--pak-path", required=True)
    parser.add_argument("--pak-name", required=True)
    parser.add_argument("--pak-slug", required=True)
    parser.add_argument("--processors-json", required=True)
    parser.add_argument("--pak-timeout-minutes", type=int, required=True)
    parser.add_argument("--tool-dir", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--steam-app-id", required=True)
    parser.add_argument("--steam-depot-id", required=True)
    return parser.parse_args()


def resolve_user_path(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = ROOT / expanded
    return expanded.resolve()


def ensure_safe_work_root(path: Path, *, pak_slug: str) -> Path:
    root = resolve_user_path(path)
    if root == Path(root.anchor):
        fail(f"Refusing to use filesystem root as work root: {root}")
    if root == ROOT:
        fail(f"Refusing to use repository root as work root: {root}")

    runner_temp = os.environ.get("RUNNER_TEMP")
    if runner_temp:
        expected = resolve_user_path(Path(runner_temp) / "pak-work" / pak_slug)
        if root != expected:
            fail(
                f"Unexpected work root for pak slug {pak_slug!r}: "
                f"{root} (expected {expected})"
            )
    return root


def clear_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


class Deadline:
    def __init__(self, total_seconds: int) -> None:
        self.started = time.monotonic()
        self.total_seconds = total_seconds

    def remaining_seconds(self) -> int:
        elapsed = time.monotonic() - self.started
        remaining = int(self.total_seconds - elapsed)
        return max(0, remaining)

    def ensure_remaining(self, label: str) -> int:
        remaining = self.remaining_seconds()
        if remaining <= 0:
            fail(f"Per-pak timeout reached before {label}")
        return remaining

    def timeout_for(self, label: str, *, limit_seconds: int | None = None) -> int:
        remaining = self.ensure_remaining(label)
        if limit_seconds is None:
            return remaining
        return max(1, min(remaining, limit_seconds))


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    label: str,
    input_text: str | None = None,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    print(f"Running {label}: {' '.join(command)}")
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            env=env,
            input=input_text,
            text=True,
            timeout=timeout_seconds,
            check=False,
            capture_output=capture_output,
        )
    except subprocess.TimeoutExpired as exc:
        fail(f"{label} timed out after {timeout_seconds} seconds")
    if check and completed.returncode != 0:
        if capture_output:
            if completed.stdout:
                print(completed.stdout)
            if completed.stderr:
                print(completed.stderr, file=sys.stderr)
        fail(f"{label} failed with exit code {completed.returncode}")
    return completed


class SteamApprover(threading.Thread):
    def __init__(self, steamguard_path: Path, stop_event: threading.Event) -> None:
        super().__init__(daemon=True)
        self.steamguard_path = steamguard_path
        self.stop_event = stop_event

    def run(self) -> None:
        for attempt in range(1, APPROVE_ATTEMPTS + 1):
            if self.stop_event.is_set():
                return
            completed = subprocess.run(
                [str(self.steamguard_path), "approve", "--dangerously-approve-all"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                text=True,
            )
            if completed.returncode != 0:
                print(
                    f"::warning::steamguard approve failed on attempt {attempt}",
                    file=sys.stderr,
                )
            if self.stop_event.wait(APPROVE_INTERVAL_SECONDS):
                return


def load_processors(raw_json: str) -> list[dict[str, Any]]:
    try:
        loaded = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        fail(f"Unable to parse processors JSON: {exc}")
    if not isinstance(loaded, list):
        fail("processors-json must decode to a list")

    processors: list[dict[str, Any]] = []
    for index, item in enumerate(loaded, start=1):
        if not isinstance(item, dict):
            fail(f"processors[{index}] must be an object")
        for field in ("id", "repo", "ref", "workdir", "command", "timeout_minutes"):
            if field not in item:
                fail(f"processors[{index}] is missing required field: {field}")
        timeout = item["timeout_minutes"]
        if not isinstance(timeout, int) or timeout <= 0:
            fail(f"processors[{index}].timeout_minutes must be a positive integer")
        env = item.get("env", {})
        if not isinstance(env, dict):
            fail(f"processors[{index}].env must be a mapping")
        clone_depth = item.get("clone_depth", 1)
        if not isinstance(clone_depth, int) or clone_depth <= 0:
            fail(f"processors[{index}].clone_depth must be a positive integer")
        processors.append(
            {
                "id": str(item["id"]),
                "repo": str(item["repo"]),
                "ref": str(item["ref"]),
                "workdir": str(item["workdir"]),
                "command": str(item["command"]),
                "timeout_minutes": timeout,
                "clone_depth": clone_depth,
                "env": {str(key): str(value) for key, value in env.items()},
            }
        )
    return processors


def bash_command(command: str) -> list[str]:
    bash_path = shutil.which("bash")
    if bash_path:
        return [bash_path, "-lc", command]

    comspec = os.environ.get("COMSPEC")
    if comspec:
        return [comspec, "/d", "/s", "/c", command]

    fail("Neither bash nor cmd.exe were found for running processor commands")


def clone_target(base_dir: Path, repo: str, ref: str) -> Path:
    digest = hashlib.sha1(f"{repo}@{ref}".encode("utf-8")).hexdigest()[:12]
    safe_repo = repo.replace("/", "__")
    return base_dir / f"{safe_repo}-{digest}"


def ensure_processor_repo(
    *,
    repo: str,
    ref: str,
    clone_depth: int,
    repo_root: Path,
    deadline: Deadline,
    git_env: dict[str, str],
) -> Path:
    target = clone_target(repo_root, repo, ref)
    if target.exists():
        return target

    url = f"https://github.com/{repo}.git"
    clear_directory(target)

    run_command(
        ["git", "init", str(target)],
        env=git_env,
        timeout_seconds=deadline.timeout_for("git init", limit_seconds=120),
        label=f"git init {repo}@{ref}",
    )
    run_command(
        ["git", "-C", str(target), "remote", "add", "origin", url],
        env=git_env,
        timeout_seconds=deadline.timeout_for("git remote add", limit_seconds=60),
        label=f"git remote add {repo}@{ref}",
    )

    fetch = run_command(
        ["git", "-C", str(target), "fetch", "--depth", str(clone_depth), "origin", ref],
        env=git_env,
        timeout_seconds=deadline.timeout_for("git fetch", limit_seconds=600),
        label=f"git fetch {repo}@{ref}",
        check=False,
    )

    if fetch.returncode == 0:
        run_command(
            ["git", "-C", str(target), "checkout", "--force", "FETCH_HEAD"],
            env=git_env,
            timeout_seconds=deadline.timeout_for("git checkout", limit_seconds=120),
            label=f"git checkout {repo}@{ref}",
        )
        return target

    run_command(
        ["git", "-C", str(target), "fetch", "--tags", "origin"],
        env=git_env,
        timeout_seconds=deadline.timeout_for("git fetch fallback", limit_seconds=900),
        label=f"git fetch fallback {repo}@{ref}",
    )
    run_command(
        ["git", "-C", str(target), "checkout", "--force", ref],
        env=git_env,
        timeout_seconds=deadline.timeout_for("git checkout fallback", limit_seconds=120),
        label=f"git checkout fallback {repo}@{ref}",
    )
    return target


def download_pak(
    *,
    pak_path: str,
    args: argparse.Namespace,
    work_root: Path,
    deadline: Deadline,
) -> Path:
    steam_user = os.environ.get("STEAM_USER")
    steam_pass = os.environ.get("STEAM_PASS")
    if not steam_user or not steam_pass:
        fail("STEAM_USER and STEAM_PASS must be set")

    tool_dir = resolve_user_path(args.tool_dir)
    steamguard_path = tool_dir / "steamguard.exe"
    depotdownloader_path = tool_dir / "DepotDownloader.exe"
    if not steamguard_path.is_file():
        fail(f"steamguard.exe not found: {steamguard_path}")
    if not depotdownloader_path.is_file():
        fail(f"DepotDownloader.exe not found: {depotdownloader_path}")

    depot_root = work_root / "depotdownloader"
    download_root = work_root / "game_files"
    clear_directory(depot_root)
    clear_directory(download_root)

    filelist_path = work_root / "filelist.txt"
    filelist_path.write_text(f"{pak_path}\n", encoding="utf-8")

    stop_event = threading.Event()
    approver = SteamApprover(steamguard_path, stop_event)
    approver.start()
    try:
        code = run_command(
            [str(steamguard_path), "code"],
            timeout_seconds=deadline.timeout_for("steamguard code", limit_seconds=60),
            label="steamguard code",
            capture_output=True,
        ).stdout.strip()
        if not code:
            fail("steamguard code did not return a 2FA code")

        command = [
            str(depotdownloader_path),
            "-app",
            args.steam_app_id,
            "-depot",
            args.steam_depot_id,
            "-username",
            steam_user,
            "-remember-password",
            "-no-mobile",
            "-filelist",
            str(filelist_path),
            "-dir",
            str(download_root),
            "-validate",
        ]
        run_command(
            command,
            cwd=depot_root,
            timeout_seconds=deadline.timeout_for("DepotDownloader pak download"),
            label=f"DepotDownloader {pak_path}",
            input_text=f"{steam_pass}\n{code}\n",
        )
    finally:
        stop_event.set()
        approver.join(timeout=1)

    pak_file = download_root / Path(*pak_path.split("/"))
    if not pak_file.is_file():
        fail(f"Downloaded pak not found: {pak_file}")
    return pak_file


def extract_pak(*, pak_file: Path, work_root: Path, deadline: Deadline) -> Path:
    unpack_root = work_root / "unpacked"
    clear_directory(unpack_root)
    run_command(
        [
            sys.executable,
            str(ROOT / "scripts" / "extract_pak.py"),
            "--pak",
            str(pak_file),
            "--out",
            str(unpack_root),
        ],
        timeout_seconds=deadline.timeout_for("pak extraction"),
        label=f"extract {pak_file.name}",
    )

    if not any(unpack_root.rglob("*")):
        fail(f"Pak extraction produced no files: {pak_file}")
    return unpack_root


def run_processors(
    *,
    processors: list[dict[str, Any]],
    pak_name: str,
    pak_path: str,
    pak_file: Path,
    unpack_root: Path,
    work_root: Path,
    deadline: Deadline,
) -> None:
    if not processors:
        print(f"No processors resolved for {pak_name}; skipping processor execution.")
        return

    repo_root = work_root / "processors"
    clear_directory(repo_root)

    git_env = os.environ.copy()
    git_env["GIT_TERMINAL_PROMPT"] = "0"

    shared_env = os.environ.copy()
    shared_env["ICARUS_PAK_NAME"] = pak_name
    shared_env["ICARUS_PAK_DEPOT_PATH"] = pak_path
    shared_env["ICARUS_PAK_FILE"] = str(pak_file)
    shared_env["ICARUS_PAK_UNPACK_DIR"] = str(unpack_root)
    shared_env["ICARUS_PAK_WORK_ROOT"] = str(work_root)
    shared_env["ICARUS_WORKFLOW_REPO"] = os.environ.get("GITHUB_REPOSITORY", "")

    repo_cache: dict[tuple[str, str], Path] = {}
    for index, processor in enumerate(processors, start=1):
        key = (processor["repo"], processor["ref"])
        if key not in repo_cache:
            repo_cache[key] = ensure_processor_repo(
                repo=processor["repo"],
                ref=processor["ref"],
                clone_depth=processor["clone_depth"],
                repo_root=repo_root,
                deadline=deadline,
                git_env=git_env,
            )

        repo_dir = repo_cache[key]
        workdir = (repo_dir / processor["workdir"]).resolve()
        if workdir != repo_dir and repo_dir not in workdir.parents:
            fail(
                f"Processor workdir escapes the checked out repo: "
                f"{processor['repo']} -> {processor['workdir']}"
            )
        if not workdir.is_dir():
            fail(
                f"Processor workdir does not exist: "
                f"{processor['repo']} -> {processor['workdir']}"
            )

        env = shared_env.copy()
        env.update(processor["env"])
        timeout_seconds = deadline.timeout_for(
            f"processor {processor['id']}",
            limit_seconds=processor["timeout_minutes"] * 60,
        )

        print(
            f"Running processor {index}/{len(processors)} "
            f"({processor['id']}) for {pak_name}"
        )
        run_command(
            bash_command(processor["command"]),
            cwd=workdir,
            env=env,
            timeout_seconds=timeout_seconds,
            label=f"processor {processor['id']}",
        )


def main() -> None:
    args = parse_args()
    try:
        pak_path = validate_pak_path(args.pak_path)
        pak_name = validate_pak_name(args.pak_name, pak_path=pak_path)
        pak_slug = validate_pak_slug(args.pak_slug, pak_path=pak_path)
    except ValueError as exc:
        fail(str(exc))

    work_root = ensure_safe_work_root(args.work_root, pak_slug=pak_slug)
    processors = load_processors(args.processors_json)
    deadline = Deadline(args.pak_timeout_minutes * 60)

    print(f"Processing pak: {pak_name}")
    print(f"Depot path: {pak_path}")
    print(f"Per-pak timeout: {args.pak_timeout_minutes} minutes")
    print(f"Resolved processors: {len(processors)}")

    clear_directory(work_root)
    try:
        pak_file = download_pak(
            pak_path=pak_path,
            args=args,
            work_root=work_root,
            deadline=deadline,
        )
        unpack_root = extract_pak(
            pak_file=pak_file,
            work_root=work_root,
            deadline=deadline,
        )
        run_processors(
            processors=processors,
            pak_name=pak_name,
            pak_path=pak_path,
            pak_file=pak_file,
            unpack_root=unpack_root,
            work_root=work_root,
            deadline=deadline,
        )
        print(f"Finished processing pak: {pak_name}")
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


if __name__ == "__main__":
    main()
