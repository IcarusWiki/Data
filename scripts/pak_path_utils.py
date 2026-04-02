#!/usr/bin/env python3
"""
Shared helpers for validating and deriving pak identifiers from depot paths.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath

PAK_ROOT_PARTS = ("Icarus", "Content", "Paks")
FORBIDDEN_PAK_CHARS = set("*?[]\0\r\n")
FORBIDDEN_TEXT_CHARS = {"\0", "\r", "\n"}
SAFE_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
SLUG_REPLACEMENT_PATTERN = re.compile(r"[^a-z0-9._-]+")


def normalize_pak_path(path_text: str) -> str:
    normalized = path_text.strip().replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized


def validate_pak_path(path_text: str) -> str:
    normalized = normalize_pak_path(path_text)
    if not normalized:
        raise ValueError("pak path must be a non-empty string")
    if any(char in normalized for char in FORBIDDEN_PAK_CHARS):
        raise ValueError(f"pak path contains forbidden characters: {normalized!r}")

    path = PurePosixPath(normalized)
    parts = path.parts
    if len(parts) <= len(PAK_ROOT_PARTS):
        raise ValueError(f"pak path must include a filename under {'/'.join(PAK_ROOT_PARTS)}")
    if parts[: len(PAK_ROOT_PARTS)] != PAK_ROOT_PARTS:
        raise ValueError(
            f"pak path must live under {'/'.join(PAK_ROOT_PARTS)}: {normalized}"
        )
    if path.suffix.lower() != ".pak":
        raise ValueError(f"pak path must end with .pak: {normalized}")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"pak path contains invalid path segments: {normalized}")
    return str(path)


def derive_pak_name(pak_path: str) -> str:
    normalized = validate_pak_path(pak_path)
    pak_name = PurePosixPath(normalized).stem
    if not pak_name:
        raise ValueError(f"unable to derive pak name from path: {normalized}")
    return pak_name


def validate_pak_name(name: str, *, pak_path: str | None = None) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("pak name must be a non-empty string")
    normalized = name.strip()
    if any(char in normalized for char in FORBIDDEN_TEXT_CHARS):
        raise ValueError(f"pak name contains forbidden characters: {normalized!r}")
    if pak_path is not None:
        expected = derive_pak_name(pak_path)
        if normalized != expected:
            raise ValueError(
                f"pak name does not match pak path stem: {normalized!r} != {expected!r}"
            )
    return normalized


def derive_pak_slug(pak_path: str) -> str:
    normalized = validate_pak_path(pak_path)
    pak_name = derive_pak_name(normalized).lower()
    safe_base = SLUG_REPLACEMENT_PATTERN.sub("-", pak_name)
    safe_base = safe_base.strip("._-")
    if not safe_base:
        safe_base = "pak"
    safe_base = safe_base[:48]
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    return f"{safe_base}-{digest}"


def validate_pak_slug(slug: str, *, pak_path: str | None = None) -> str:
    if not isinstance(slug, str) or not slug:
        raise ValueError("pak slug must be a non-empty string")
    normalized = slug.strip()
    if not SAFE_SLUG_PATTERN.fullmatch(normalized):
        raise ValueError(f"pak slug contains unsupported characters: {normalized!r}")
    if len(normalized) > 80:
        raise ValueError(f"pak slug is too long: {len(normalized)} characters")
    if pak_path is not None:
        expected = derive_pak_slug(pak_path)
        if normalized != expected:
            raise ValueError(
                f"pak slug does not match derived slug for path {pak_path!r}: "
                f"{normalized!r} != {expected!r}"
            )
    return normalized
