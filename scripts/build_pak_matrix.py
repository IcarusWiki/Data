#!/usr/bin/env python3
"""
Discover content paks from a DepotDownloader manifest dump and build a GitHub
Actions matrix with the processors that should run for each pak.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
PAK_PATTERN = re.compile(rb"Icarus/Content/Paks/[^\r\n\x00\"']+?\.pak", re.IGNORECASE)
REPO_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MANIFEST_TEXT_PATTERN = re.compile(r"^manifest_\d+_\d+\.txt$", re.IGNORECASE)


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the per-pak GitHub Actions matrix.",
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        required=True,
        help="Directory containing DepotDownloader manifest output/log files.",
    )
    parser.add_argument(
        "--rules",
        type=Path,
        required=True,
        help="YAML rules file that defines run_on_all and targeted processors.",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        help="Optional path to the GITHUB_OUTPUT file.",
    )
    return parser.parse_args()


def resolve_user_path(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = ROOT / expanded
    return expanded.resolve()


def ensure_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be a mapping")
    return value


def ensure_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        fail(f"{label} must be a list")
    return value


def ensure_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{label} must be a non-empty string")
    return value


def ensure_positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        fail(f"{label} must be a positive integer")
    return value


def ensure_string_list(value: Any, label: str) -> list[str]:
    items = ensure_list(value, label)
    normalized: list[str] = []
    for index, item in enumerate(items, start=1):
        normalized.append(ensure_string(item, f"{label}[{index}]"))
    if not normalized:
        fail(f"{label} must not be empty")
    return normalized


def normalize_optional_string_list(value: Any, label: str) -> list[str]:
    items = ensure_list(value, label)
    normalized: list[str] = []
    for index, item in enumerate(items, start=1):
        normalized.append(ensure_string(item, f"{label}[{index}]"))
    return normalized


def normalize_env(value: Any, label: str) -> dict[str, str]:
    if value is None:
        return {}
    mapping = ensure_mapping(value, label)
    normalized: dict[str, str] = {}
    for key, raw in mapping.items():
        env_key = ensure_string(key, f"{label} key")
        if not isinstance(raw, (str, int, float, bool)) or raw is None:
            fail(f"{label}.{env_key} must be a scalar string/int/float/bool")
        normalized[env_key] = str(raw)
    return normalized


def normalize_defaults(raw_defaults: Any) -> dict[str, int]:
    defaults = ensure_mapping(raw_defaults or {}, "defaults")
    pak_timeout = defaults.get("pak_timeout_minutes", 90)
    processor_timeout = defaults.get("processor_timeout_minutes", 20)
    clone_depth = defaults.get("git_clone_depth", 1)
    return {
        "pak_timeout_minutes": ensure_positive_int(
            pak_timeout,
            "defaults.pak_timeout_minutes",
        ),
        "processor_timeout_minutes": ensure_positive_int(
            processor_timeout,
            "defaults.processor_timeout_minutes",
        ),
        "git_clone_depth": ensure_positive_int(
            clone_depth,
            "defaults.git_clone_depth",
        ),
    }


def normalize_processor(
    raw_processor: Any,
    *,
    label: str,
    defaults: dict[str, int],
) -> dict[str, Any]:
    processor = ensure_mapping(raw_processor, label)
    processor_id = ensure_string(processor.get("id"), f"{label}.id")
    repo = ensure_string(processor.get("repo"), f"{label}.repo")
    if not REPO_PATTERN.fullmatch(repo):
        fail(f"{label}.repo must be in owner/name form")
    ref = ensure_string(processor.get("ref"), f"{label}.ref")
    command = ensure_string(processor.get("command"), f"{label}.command")
    workdir = ensure_string(processor.get("workdir", "."), f"{label}.workdir")
    timeout = ensure_positive_int(
        processor.get("timeout_minutes", defaults["processor_timeout_minutes"]),
        f"{label}.timeout_minutes",
    )
    clone_depth = ensure_positive_int(
        processor.get("clone_depth", defaults["git_clone_depth"]),
        f"{label}.clone_depth",
    )
    return {
        "id": processor_id,
        "repo": repo,
        "ref": ref,
        "workdir": workdir,
        "command": command,
        "timeout_minutes": timeout,
        "clone_depth": clone_depth,
        "env": normalize_env(processor.get("env"), f"{label}.env"),
    }


def load_rules(path: Path) -> dict[str, Any]:
    rules_path = resolve_user_path(path)
    if not rules_path.is_file():
        fail(f"Rules file not found: {rules_path}")

    try:
        loaded = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        fail(f"Unable to parse rules YAML: {exc}")

    data = ensure_mapping(loaded or {}, "rules")
    version = data.get("version")
    if version != 1:
        fail("rules.version must be 1")

    defaults = normalize_defaults(data.get("defaults"))

    raw_run_on_all = ensure_list(data.get("run_on_all", []), "run_on_all")
    run_on_all = [
        normalize_processor(
            processor,
            label=f"run_on_all[{index}]",
            defaults=defaults,
        )
        for index, processor in enumerate(raw_run_on_all, start=1)
    ]

    raw_targeted = ensure_list(data.get("targeted", []), "targeted")
    targeted: list[dict[str, Any]] = []
    seen_rule_ids: set[str] = set()
    for index, raw_rule in enumerate(raw_targeted, start=1):
        label = f"targeted[{index}]"
        rule = ensure_mapping(raw_rule, label)
        rule_id = ensure_string(rule.get("id"), f"{label}.id")
        if rule_id in seen_rule_ids:
            fail(f"Duplicate targeted rule id: {rule_id}")
        seen_rule_ids.add(rule_id)
        include_globs = ensure_string_list(rule.get("include_globs"), f"{label}.include_globs")
        exclude_globs = normalize_optional_string_list(
            rule.get("exclude_globs", []),
            f"{label}.exclude_globs",
        ) if "exclude_globs" in rule else []
        raw_processors = ensure_list(rule.get("processors"), f"{label}.processors")
        if not raw_processors:
            fail(f"{label}.processors must not be empty")
        processors = [
            normalize_processor(
                processor,
                label=f"{label}.processors[{processor_index}]",
                defaults=defaults,
            )
            for processor_index, processor in enumerate(raw_processors, start=1)
        ]
        targeted.append(
            {
                "id": rule_id,
                "include_globs": include_globs,
                "exclude_globs": exclude_globs,
                "processors": processors,
            }
        )

    return {
        "defaults": defaults,
        "run_on_all": run_on_all,
        "targeted": targeted,
    }


def discover_paks(manifest_root: Path) -> list[str]:
    root = resolve_user_path(manifest_root)
    if not root.is_dir():
        fail(f"Manifest root not found: {root}")

    discovered: set[str] = set()
    manifest_text_files = sorted(
        candidate
        for candidate in root.rglob("*")
        if candidate.is_file() and MANIFEST_TEXT_PATTERN.fullmatch(candidate.name)
    )

    for candidate in manifest_text_files:
        try:
            with candidate.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    marker = " Icarus/Content/Paks/"
                    marker_index = line.find(marker)
                    if marker_index == -1:
                        continue
                    pak_path = line[marker_index + 1 :].strip()
                    if any(char in pak_path for char in "*?[]"):
                        continue
                    if pak_path.lower().endswith(".pak"):
                        discovered.add(pak_path)
        except OSError:
            continue

    if discovered:
        return sorted(discovered)

    for candidate in root.rglob("*"):
        if not candidate.is_file():
            continue
        try:
            size = candidate.stat().st_size
        except OSError:
            continue
        if size > 25 * 1024 * 1024:
            continue
        try:
            payload = candidate.read_bytes()
        except OSError:
            continue
        for match in PAK_PATTERN.findall(payload):
            path_text = match.decode("utf-8")
            if any(char in path_text for char in "*?[]"):
                continue
            discovered.add(path_text)

    if not discovered:
        fail(
            "No pak paths were discovered in the DepotDownloader manifest output. "
            "Check the manifest-only step and parsing regex."
        )

    return sorted(discovered)


def rule_matches(pak_path: str, rule: dict[str, Any]) -> bool:
    if not any(fnmatch.fnmatchcase(pak_path, pattern) for pattern in rule["include_globs"]):
        return False
    if any(fnmatch.fnmatchcase(pak_path, pattern) for pattern in rule["exclude_globs"]):
        return False
    return True


def resolve_processors(pak_path: str, rules: dict[str, Any]) -> list[dict[str, Any]]:
    resolved = [dict(processor) for processor in rules["run_on_all"]]
    for rule in rules["targeted"]:
        if not rule_matches(pak_path, rule):
            continue
        resolved.extend(dict(processor) for processor in rule["processors"])
    return resolved


def build_matrix(discovered_paks: list[str], rules: dict[str, Any]) -> dict[str, Any]:
    include: list[dict[str, Any]] = []
    pak_timeout = rules["defaults"]["pak_timeout_minutes"]

    for pak_path in discovered_paks:
        processors = resolve_processors(pak_path, rules)
        if not processors:
            continue
        pak_name = Path(pak_path).stem
        include.append(
            {
                "pak_path": pak_path,
                "pak_name": pak_name,
                "pak_timeout_minutes": pak_timeout,
                "processor_count": len(processors),
                "processors_json": json.dumps(processors, separators=(",", ":")),
            }
        )

    return {
        "include": include,
        "discovered_count": len(discovered_paks),
        "scheduled_count": len(include),
    }


def write_github_output(path: Path, matrix: dict[str, Any]) -> None:
    payload = json.dumps({"include": matrix["include"]}, separators=(",", ":"))
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(f"matrix={payload}\n")
        handle.write(f"discovered_count={matrix['discovered_count']}\n")
        handle.write(f"scheduled_count={matrix['scheduled_count']}\n")


def main() -> None:
    args = parse_args()
    rules = load_rules(args.rules)
    discovered_paks = discover_paks(args.manifest_root)
    matrix = build_matrix(discovered_paks, rules)

    print(f"Discovered {matrix['discovered_count']} pak files.")
    print(f"Scheduled {matrix['scheduled_count']} pak jobs.")

    if args.github_output:
        write_github_output(args.github_output, matrix)
    else:
        print(json.dumps({"include": matrix["include"]}, indent=2))


if __name__ == "__main__":
    main()
