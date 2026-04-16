#!/usr/bin/env python3
"""
Discover content paks from a DepotDownloader manifest dump and build GitHub
Actions matrices for both per-pak processors and consumer finalizers.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from pak_path_utils import derive_pak_name, derive_pak_slug, normalize_pak_path, validate_pak_path

ROOT = Path(__file__).resolve().parent.parent
PAK_PATTERN = re.compile(
    rb"Icarus[\\/]+Content[\\/]+Paks[\\/]+[^\r\n\x00\"']+?\.pak",
    re.IGNORECASE,
)
REPO_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
MANIFEST_TEXT_PATTERN = re.compile(r"^manifest_\d+_\d+\.txt$", re.IGNORECASE)
TEXT_PAK_PATTERN = re.compile(
    r"Icarus[\\/]+Content[\\/]+Paks[\\/]+[^\r\n]+?\.pak",
    re.IGNORECASE,
)


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the per-pak and finalizer GitHub Actions matrices.",
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
        "--consumers",
        type=Path,
        required=True,
        help="YAML consumer file that defines finalizer repos and commit behavior.",
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


def ensure_identifier(value: Any, label: str) -> str:
    identifier = ensure_string(value, label)
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        fail(f"{label} must match {IDENTIFIER_PATTERN.pattern}")
    return identifier


def ensure_identifier_list(value: Any, label: str) -> list[str]:
    items = ensure_list(value, label)
    normalized: list[str] = []
    for index, item in enumerate(items, start=1):
        normalized.append(ensure_identifier(item, f"{label}[{index}]"))
    if not normalized:
        fail(f"{label} must not be empty")
    return normalized


def ensure_repo_relative_path(value: Any, label: str) -> str:
    raw_path = ensure_string(value, label)
    normalized = PurePosixPath(raw_path)
    if normalized.is_absolute():
        fail(f"{label} must be relative")
    if any(part == ".." for part in normalized.parts):
        fail(f"{label} must not escape the repository")
    return raw_path


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


def normalize_finalizer(raw_finalizer: Any, *, label: str) -> dict[str, Any]:
    finalizer = ensure_mapping(raw_finalizer, label)
    return {
        "id": ensure_identifier(finalizer.get("id"), f"{label}.id"),
        "artifact_ids": ensure_identifier_list(
            finalizer.get("artifact_ids"),
            f"{label}.artifact_ids",
        ),
        "workdir": ensure_repo_relative_path(
            finalizer.get("workdir", "."),
            f"{label}.workdir",
        ),
        "command": ensure_string(finalizer.get("command"), f"{label}.command"),
        "commit_paths": [
            ensure_repo_relative_path(path, f"{label}.commit_paths[{index}]")
            for index, path in enumerate(
                ensure_string_list(finalizer.get("commit_paths"), f"{label}.commit_paths"),
                start=1,
            )
        ],
        "commit_message": ensure_string(
            finalizer.get("commit_message"),
            f"{label}.commit_message",
        ),
    }


def load_consumers(path: Path) -> dict[str, Any]:
    consumers_path = resolve_user_path(path)
    if not consumers_path.is_file():
        fail(f"Consumers file not found: {consumers_path}")

    try:
        loaded = yaml.safe_load(consumers_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        fail(f"Unable to parse consumers YAML: {exc}")

    data = ensure_mapping(loaded or {}, "consumers")
    if data.get("version") != 1:
        fail("consumers.version must be 1")

    raw_consumers = ensure_list(data.get("consumers", []), "consumers.consumers")
    consumers: list[dict[str, Any]] = []
    consumer_ids: set[str] = set()
    finalizer_ids: set[str] = set()

    for index, raw_consumer in enumerate(raw_consumers, start=1):
        label = f"consumers.consumers[{index}]"
        consumer = ensure_mapping(raw_consumer, label)
        consumer_id = ensure_identifier(consumer.get("id"), f"{label}.id")
        if consumer_id in consumer_ids:
            fail(f"Duplicate consumer id: {consumer_id}")
        consumer_ids.add(consumer_id)

        repo = ensure_string(consumer.get("repo"), f"{label}.repo")
        if not REPO_PATTERN.fullmatch(repo):
            fail(f"{label}.repo must be in owner/name form")

        raw_finalizers = ensure_list(consumer.get("finalizers"), f"{label}.finalizers")
        if not raw_finalizers:
            fail(f"{label}.finalizers must not be empty")

        finalizers: list[dict[str, Any]] = []
        for finalizer_index, raw_finalizer in enumerate(raw_finalizers, start=1):
            normalized = normalize_finalizer(
                raw_finalizer,
                label=f"{label}.finalizers[{finalizer_index}]",
            )
            if normalized["id"] in finalizer_ids:
                fail(f"Duplicate finalizer id: {normalized['id']}")
            finalizer_ids.add(normalized["id"])
            finalizers.append(normalized)

        consumers.append(
            {
                "id": consumer_id,
                "repo": repo,
                "ref": ensure_string(consumer.get("ref"), f"{label}.ref"),
                "checkout_path": ensure_repo_relative_path(
                    consumer.get("checkout_path"),
                    f"{label}.checkout_path",
                ),
                "finalizers": finalizers,
            }
        )

    return {
        "consumers": consumers,
        "consumer_ids": consumer_ids,
    }


def normalize_processor(
    raw_processor: Any,
    *,
    label: str,
    defaults: dict[str, int],
    consumer_ids: set[str],
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

    raw_consumer_id = processor.get("consumer_id")
    raw_artifact_id = processor.get("artifact_id")
    consumer_id: str | None = None
    artifact_id: str | None = None
    if raw_consumer_id is not None or raw_artifact_id is not None:
        consumer_id = ensure_identifier(raw_consumer_id, f"{label}.consumer_id")
        if consumer_id not in consumer_ids:
            fail(f"{label}.consumer_id references unknown consumer: {consumer_id}")
        artifact_id = ensure_identifier(raw_artifact_id, f"{label}.artifact_id")

    return {
        "id": processor_id,
        "repo": repo,
        "ref": ref,
        "workdir": workdir,
        "command": command,
        "timeout_minutes": timeout,
        "clone_depth": clone_depth,
        "env": normalize_env(processor.get("env"), f"{label}.env"),
        "consumer_id": consumer_id,
        "artifact_id": artifact_id,
    }


def load_rules(path: Path, *, consumer_ids: set[str]) -> dict[str, Any]:
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
            consumer_ids=consumer_ids,
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
                consumer_ids=consumer_ids,
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


def validate_consumers_against_rules(consumers: dict[str, Any], rules: dict[str, Any]) -> None:
    produced_artifacts_by_consumer: dict[str, set[str]] = {
        consumer["id"]: set() for consumer in consumers["consumers"]
    }
    for processor in rules["run_on_all"]:
        if processor["consumer_id"] and processor["artifact_id"]:
            produced_artifacts_by_consumer[processor["consumer_id"]].add(processor["artifact_id"])
    for rule in rules["targeted"]:
        for processor in rule["processors"]:
            if processor["consumer_id"] and processor["artifact_id"]:
                produced_artifacts_by_consumer[processor["consumer_id"]].add(processor["artifact_id"])

    for consumer in consumers["consumers"]:
        known_artifacts = produced_artifacts_by_consumer[consumer["id"]]
        for finalizer in consumer["finalizers"]:
            for artifact_id in finalizer["artifact_ids"]:
                if artifact_id not in known_artifacts:
                    fail(
                        f"Consumer {consumer['id']} finalizer {finalizer['id']} "
                        f"references unknown artifact_id {artifact_id!r}"
                    )


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
                    match = TEXT_PAK_PATTERN.search(line)
                    if not match:
                        continue
                    pak_path = normalize_pak_path(match.group(0))
                    if any(char in pak_path for char in "*?[]"):
                        continue
                    try:
                        discovered.add(validate_pak_path(pak_path))
                    except ValueError as exc:
                        fail(str(exc))
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
            path_text = normalize_pak_path(match.decode("utf-8"))
            if any(char in path_text for char in "*?[]"):
                continue
            try:
                discovered.add(validate_pak_path(path_text))
            except ValueError as exc:
                fail(str(exc))

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


def build_finalizer_include(
    consumers: dict[str, Any],
    scheduled_artifacts_by_consumer: dict[str, set[str]],
) -> list[dict[str, Any]]:
    include: list[dict[str, Any]] = []
    for consumer in consumers["consumers"]:
        scheduled_artifacts = scheduled_artifacts_by_consumer.get(consumer["id"], set())
        if not scheduled_artifacts:
            continue
        for finalizer in consumer["finalizers"]:
            if not any(artifact_id in scheduled_artifacts for artifact_id in finalizer["artifact_ids"]):
                continue
            include.append(
                {
                    "consumer_id": consumer["id"],
                    "consumer_repo": consumer["repo"],
                    "consumer_ref": consumer["ref"],
                    "consumer_checkout_path": consumer["checkout_path"],
                    "finalizer_id": finalizer["id"],
                    "finalizer_artifact_ids_json": json.dumps(
                        finalizer["artifact_ids"],
                        separators=(",", ":"),
                    ),
                    "finalizer_workdir": finalizer["workdir"],
                    "finalizer_command": finalizer["command"],
                    "finalizer_commit_paths_json": json.dumps(
                        finalizer["commit_paths"],
                        separators=(",", ":"),
                    ),
                    "finalizer_commit_message": finalizer["commit_message"],
                }
            )
    return include


def build_matrices(
    discovered_paks: list[str],
    rules: dict[str, Any],
    consumers: dict[str, Any],
) -> dict[str, Any]:
    pak_include: list[dict[str, Any]] = []
    pak_timeout = rules["defaults"]["pak_timeout_minutes"]
    scheduled_artifacts_by_consumer: dict[str, set[str]] = {
        consumer["id"]: set() for consumer in consumers["consumers"]
    }

    for pak_path in discovered_paks:
        processors = resolve_processors(pak_path, rules)
        if not processors:
            continue
        pak_name = derive_pak_name(pak_path)
        pak_slug = derive_pak_slug(pak_path)
        pak_include.append(
            {
                "pak_path": pak_path,
                "pak_name": pak_name,
                "pak_slug": pak_slug,
                "pak_timeout_minutes": pak_timeout,
                "processor_count": len(processors),
                "processors_json": json.dumps(processors, separators=(",", ":")),
            }
        )
        for processor in processors:
            if processor["consumer_id"] and processor["artifact_id"]:
                scheduled_artifacts_by_consumer[processor["consumer_id"]].add(processor["artifact_id"])

    finalizer_include = build_finalizer_include(consumers, scheduled_artifacts_by_consumer)

    return {
        "pak_include": pak_include,
        "finalizer_include": finalizer_include,
        "discovered_count": len(discovered_paks),
        "scheduled_count": len(pak_include),
        "finalizer_count": len(finalizer_include),
    }


def write_github_output(path: Path, matrices: dict[str, Any]) -> None:
    pak_payload = json.dumps({"include": matrices["pak_include"]}, separators=(",", ":"))
    finalizer_payload = json.dumps(
        {"include": matrices["finalizer_include"]},
        separators=(",", ":"),
    )
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(f"matrix={pak_payload}\n")
        handle.write(f"discovered_count={matrices['discovered_count']}\n")
        handle.write(f"scheduled_count={matrices['scheduled_count']}\n")
        handle.write(f"finalizer_matrix={finalizer_payload}\n")
        handle.write(f"finalizer_count={matrices['finalizer_count']}\n")


def main() -> None:
    args = parse_args()
    consumers = load_consumers(args.consumers)
    rules = load_rules(args.rules, consumer_ids=consumers["consumer_ids"])
    validate_consumers_against_rules(consumers, rules)
    discovered_paks = discover_paks(args.manifest_root)
    matrices = build_matrices(discovered_paks, rules, consumers)

    print(f"Discovered {matrices['discovered_count']} pak files.")
    print(f"Scheduled {matrices['scheduled_count']} pak jobs.")
    print(f"Scheduled {matrices['finalizer_count']} finalizer jobs.")

    if args.github_output:
        write_github_output(args.github_output, matrices)
    else:
        print(
            json.dumps(
                {
                    "paks": {"include": matrices["pak_include"]},
                    "finalizers": {"include": matrices["finalizer_include"]},
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
