#!/usr/bin/env python3
"""
Publish ICARUS Steam community announcements into the wiki.gg News namespace.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from wiki_client import MediaWikiClient, MediaWikiError, normalize_text
except ModuleNotFoundError:  # pragma: no cover - supports repo-root imports
    from scripts.wiki_client import MediaWikiClient, MediaWikiError, normalize_text

DEFAULT_STEAM_APP_ID = "1149460"
DEFAULT_WIKI_API_URL = "https://icarus.wiki.gg/api.php"
STEAM_NEWS_API_URL = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
STEAM_FEED_NAME = "steam_community_announcements"
STEAM_NEWS_PAGE_SIZE = 1000
STEAM_CLAN_IMAGE_TOKEN = "{STEAM_CLAN_IMAGE}"
STEAM_CLAN_IMAGE_BASE_URL = "https://clan.akamai.steamstatic.com/images"
USER_AGENT = "IcarusSteamNewsSync/1.0 (GitHub Actions; github.com/IcarusWiki/Data)"
REQUEST_TIMEOUT_SECONDS = 30
WIKIGG_USERNAME_ENV = "WIKIGG_USERNAME"
WIKIGG_APP_PASSWORD_ENV = "WIKIGG_APP_PASSWORD"
NEWS_NAMESPACE_PREFIX = "News:"
NEWS_INFO_TEMPLATE_NAME = "NewsInfo"
WIKI_EMBED_IMAGE_WIDTH_PX = 1000
STEAM_META_OG_IMAGE_PATTERN = re.compile(
    r'<meta property=["\']og:image["\'] content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
STEAM_EVENT_JSONDATA_PATTERN = re.compile(
    r'&quot;event_name&quot;:&quot;(?P<event_name>.*?)&quot;.*?'
    r'&quot;jsondata&quot;:&quot;(?P<jsondata>\{.*?\})&quot;',
    re.DOTALL,
)
STEAM_IMAGE_BASE_URL_PATTERN = re.compile(
    r"(https://clan(?:\.[a-z]+)?\.steamstatic\.com/images/\d+)",
    re.IGNORECASE,
)
IMAGE_TAG_PATTERN = re.compile(r"\[img\](.*?)\[/img\]", re.IGNORECASE | re.DOTALL)
STANDALONE_URL_PATTERN = re.compile(r"^https?://\S+$", re.IGNORECASE)
VALID_IMAGE_SUFFIX_PATTERN = re.compile(r"\.[a-z0-9]{2,5}$", re.IGNORECASE)
MAX_MEDIAWIKI_TITLE_LENGTH = 255
GID_MARKER_PATTERN = re.compile(r"<!--\s*ICARUS_STEAM_NEWS_GID:(\d+)\s*-->")
UNSAFE_TITLE_PATTERN = re.compile(r'[\\/:#<>\[\]{}|"]+')
WHITESPACE_PATTERN = re.compile(r"\s+")
TITLE_TRIM_CHARS = " .,:;!?\t\r\n-"
VERSION_PATTERNS = [
    re.compile(r"(?i)\bhotfix(?:\s+version)?[: ]+\s*v?(\d+\.\d+\.\d+\.\d+)"),
    re.compile(r"(?i)\bchangelog\s+v?(\d+\.\d+\.\d+\.\d+)"),
    re.compile(r"(?i)\bversion\s+v?(\d+\.\d+\.\d+\.\d+)"),
]


def fail(message: str) -> "NoReturn":
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"Warning: {message}", file=sys.stderr)


@dataclass(slots=True)
class SteamNewsItem:
    gid: str
    title: str
    url: str
    author: str
    contents: str
    feedlabel: str
    date: int


@dataclass(slots=True)
class SteamAnnouncementPageData:
    subtitle: str | None = None
    hero_image_url: str | None = None


@dataclass(slots=True)
class BodyImageRenderPlan:
    source_url: str | None
    file_title: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish ICARUS Steam community announcements into wiki.gg News pages.",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("backfill_all", "incremental"),
        help="Whether to publish the full archive or only posts newer than --last-gid.",
    )
    parser.add_argument(
        "--last-gid",
        help="Last successfully published Steam gid. Required for incremental mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render and diff pages without editing the wiki.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        help=(
            "Optional staged publish cap. When set, the run processes the "
            "oldest pending slice so state can advance safely."
        ),
    )
    parser.add_argument(
        "--wiki-api-url",
        default=DEFAULT_WIKI_API_URL,
        help=f"MediaWiki API URL (default: {DEFAULT_WIKI_API_URL})",
    )
    parser.add_argument(
        "--steam-app-id",
        default=DEFAULT_STEAM_APP_ID,
        help=f"Steam app ID (default: {DEFAULT_STEAM_APP_ID})",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        help="Optional path to the GITHUB_OUTPUT file.",
    )
    return parser.parse_args()


def ensure_positive_int(value: int, label: str) -> int:
    if value <= 0:
        fail(f"{label} must be a positive integer")
    return value


def validate_gid(gid: str | None, *, label: str) -> str:
    if gid is None or not gid.strip():
        fail(f"{label} is required")
    candidate = gid.strip()
    if not candidate.isdigit():
        fail(f"{label} must contain only digits")
    return candidate


def request_json(url: str, params: dict[str, str]) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            raw_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        fail(f"Steam API HTTP error {exc.code}: {exc.reason}")
    except urllib.error.URLError as exc:
        fail(f"Failed to reach Steam API: {exc}")
    except socket.timeout:
        fail(
            "Timed out while waiting for the Steam API "
            f"after {REQUEST_TIMEOUT_SECONDS} seconds."
        )

    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        fail(f"Steam API returned invalid JSON: {exc}")

    return data


def request_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code}: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to reach URL: {exc}") from exc
    except socket.timeout as exc:
        raise RuntimeError(
            f"Timed out after {REQUEST_TIMEOUT_SECONDS} seconds while fetching {url}"
        ) from exc


def request_bytes(url: str) -> tuple[bytes, str | None]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "image/*,*/*;q=0.8",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.read(), response.headers.get("Content-Type")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code}: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to reach URL: {exc}") from exc
    except socket.timeout as exc:
        raise RuntimeError(
            f"Timed out after {REQUEST_TIMEOUT_SECONDS} seconds while fetching {url}"
        ) from exc


def parse_news_item(raw_item: Any) -> SteamNewsItem:
    if not isinstance(raw_item, dict):
        fail("Steam API returned a malformed news item")

    gid = str(raw_item.get("gid", "")).strip()
    title = str(raw_item.get("title", "")).strip()
    url = str(raw_item.get("url", "")).strip()
    author = str(raw_item.get("author", "")).strip()
    contents = normalize_text(str(raw_item.get("contents", "")))
    feedlabel = str(raw_item.get("feedlabel", "")).strip() or "Community Announcements"
    date_value = raw_item.get("date")

    if not gid.isdigit():
        fail(f"Steam API returned an invalid gid: {gid!r}")
    if not title:
        fail(f"Steam API returned an empty title for gid {gid}")
    if not url:
        fail(f"Steam API returned an empty URL for gid {gid}")
    if not isinstance(date_value, int) or date_value <= 0:
        fail(f"Steam API returned an invalid publish date for gid {gid}")

    return SteamNewsItem(
        gid=gid,
        title=title,
        url=url,
        author=author or "Unknown",
        contents=contents,
        feedlabel=feedlabel,
        date=date_value,
    )


def fetch_steam_news_page(app_id: str, *, enddate: int | None) -> tuple[list[SteamNewsItem], int | None]:
    params = {
        "appid": app_id,
        "count": str(STEAM_NEWS_PAGE_SIZE),
        "maxlength": "0",
        "feeds": STEAM_FEED_NAME,
    }
    if enddate is not None:
        params["enddate"] = str(enddate)

    payload = request_json(STEAM_NEWS_API_URL, params)
    appnews = payload.get("appnews")
    if not isinstance(appnews, dict):
        fail("Steam API response did not include appnews")

    raw_items = appnews.get("newsitems")
    if not isinstance(raw_items, list):
        fail("Steam API response did not include a newsitems list")

    count_value = appnews.get("count")
    total_count = count_value if isinstance(count_value, int) and count_value >= 0 else None
    items = [parse_news_item(raw_item) for raw_item in raw_items]
    return items, total_count


def collect_target_items(
    app_id: str,
    *,
    mode: str,
    last_gid: str | None,
) -> tuple[list[SteamNewsItem], str | None]:
    enddate: int | None = None
    seen_gids: set[str] = set()
    target_items: list[SteamNewsItem] = []
    newest_available_gid: str | None = None
    found_last_gid = False
    total_available_count: int | None = None

    while True:
        page_items, total_count = fetch_steam_news_page(app_id, enddate=enddate)
        if total_available_count is None:
            total_available_count = total_count

        if newest_available_gid is None and page_items:
            newest_available_gid = page_items[0].gid

        fresh_items = [item for item in page_items if item.gid not in seen_gids]
        for item in fresh_items:
            seen_gids.add(item.gid)

        if not fresh_items:
            break

        for item in fresh_items:
            if mode == "incremental" and item.gid == last_gid:
                found_last_gid = True
                break
            target_items.append(item)

        if mode == "incremental" and found_last_gid:
            break

        if len(fresh_items) < STEAM_NEWS_PAGE_SIZE:
            break

        oldest_date = min(item.date for item in fresh_items)
        if oldest_date <= 0:
            break
        if enddate is not None and oldest_date >= enddate:
            fail("Steam API pagination stalled while traversing older news items.")
        enddate = oldest_date - 1

        if total_available_count is not None and len(seen_gids) >= total_available_count:
            break

    if mode == "incremental" and not found_last_gid:
        fail(
            "Incremental mode could not find the stored last gid in the Steam feed. "
            "Run a manual backfill_all bootstrap or verify ICARUS_STEAM_NEWS_LAST_GID."
        )

    return target_items, newest_available_gid


def sanitize_title_text(text: str) -> str:
    title = html.unescape(text).strip()
    title = UNSAFE_TITLE_PATTERN.sub(" - ", title)
    title = WHITESPACE_PATTERN.sub(" ", title)
    title = title.strip(TITLE_TRIM_CHARS)
    return title or "Steam News"


def truncate_title_for_suffix(base_title: str, suffix: str) -> str:
    max_title_len = MAX_MEDIAWIKI_TITLE_LENGTH - len(NEWS_NAMESPACE_PREFIX) - len(suffix)
    if max_title_len <= 0:
        fail("Configured title suffix exceeded MediaWiki's title length limit.")
    if len(base_title) <= max_title_len:
        return base_title
    trimmed = base_title[:max_title_len].rstrip(TITLE_TRIM_CHARS)
    return trimmed or base_title[:max_title_len]


def build_page_title(base_title: str, suffix: str) -> str:
    truncated = truncate_title_for_suffix(base_title, suffix)
    return f"{NEWS_NAMESPACE_PREFIX}{truncated}{suffix}"


def extract_existing_gid(text: str) -> str | None:
    match = GID_MARKER_PATTERN.search(text)
    if match is None:
        return None
    return match.group(1)


def format_date_suffix(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d")


def resolve_page_title(client: MediaWikiClient, item: SteamNewsItem) -> tuple[str, str]:
    base_title = sanitize_title_text(item.title)
    date_suffix = format_date_suffix(item.date)
    candidate_suffixes = [
        "",
        f" ({date_suffix})",
        f" ({date_suffix}) [{item.gid}]",
    ]

    for suffix in candidate_suffixes:
        candidate_title = build_page_title(base_title, suffix)
        page = client.fetch_page(candidate_title)
        if not page.exists:
            return candidate_title, ""

        existing_gid = extract_existing_gid(page.text)
        if existing_gid == item.gid:
            return candidate_title, page.text

    fail(
        "Unable to resolve a unique News page title for Steam post "
        f"{item.gid} ({item.title!r})."
    )


def normalize_external_link_label(text: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", text.strip()) or "link"


def extract_first_localized_string(raw_value: Any) -> str | None:
    if not isinstance(raw_value, list):
        return None
    for candidate in raw_value:
        if not isinstance(candidate, str):
            continue
        normalized = normalize_text(html.unescape(candidate)).strip()
        if normalized:
            return normalized
    return None


def decode_steam_event_jsondata(raw_jsondata: str) -> dict[str, Any]:
    decoded_json = html.unescape(raw_jsondata).encode("utf-8").decode("unicode_escape")
    return json.loads(decoded_json)


def infer_steam_image_base_url(page_html: str) -> str | None:
    match = STEAM_IMAGE_BASE_URL_PATTERN.search(html.unescape(page_html))
    if match is None:
        return None
    return match.group(1)


def extract_announcement_page_data(
    page_html: str,
    *,
    expected_title: str,
) -> SteamAnnouncementPageData:
    hero_image_url: str | None = None
    og_image_match = STEAM_META_OG_IMAGE_PATTERN.search(page_html)
    if og_image_match is not None:
        candidate_url = html.unescape(og_image_match.group(1)).strip()
        if candidate_url:
            hero_image_url = candidate_url

    subtitle: str | None = None
    normalized_title = normalize_text(html.unescape(expected_title)).strip()
    embedded_data: dict[str, Any] | None = None

    for match in STEAM_EVENT_JSONDATA_PATTERN.finditer(page_html):
        event_name = normalize_text(html.unescape(match.group("event_name"))).strip()
        if event_name != normalized_title:
            continue
        embedded_data = decode_steam_event_jsondata(match.group("jsondata"))
        break

    if embedded_data is not None:
        subtitle = extract_first_localized_string(embedded_data.get("localized_subtitle"))
        if hero_image_url is None:
            capsule_filename = extract_first_localized_string(
                embedded_data.get("localized_capsule_image")
            )
            image_base_url = infer_steam_image_base_url(page_html)
            if capsule_filename and image_base_url:
                hero_image_url = f"{image_base_url}/{capsule_filename}"

    return SteamAnnouncementPageData(
        subtitle=subtitle,
        hero_image_url=hero_image_url,
    )


def fetch_announcement_page_data(item: SteamNewsItem) -> SteamAnnouncementPageData:
    try:
        page_html = request_text(item.url)
    except RuntimeError as exc:
        warn(f"Unable to fetch Steam page metadata for {item.gid}: {exc}")
        return SteamAnnouncementPageData()

    try:
        return extract_announcement_page_data(page_html, expected_title=item.title)
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        warn(f"Unable to parse Steam page metadata for {item.gid}: {exc}")
        return SteamAnnouncementPageData()


def strip_file_namespace(file_title: str) -> str:
    if file_title.startswith("File:"):
        return file_title[len("File:") :]
    return file_title


def guess_image_suffix(image_url: str, *, default_suffix: str = ".png") -> str:
    suffix = Path(urllib.parse.urlsplit(image_url).path).suffix.lower()
    if VALID_IMAGE_SUFFIX_PATTERN.fullmatch(suffix):
        return suffix
    return default_suffix


def build_header_image_file_title(item: SteamNewsItem, image_url: str) -> str:
    return f"File:Icarus_Steam_News_{item.gid}_Header{guess_image_suffix(image_url)}"


def build_body_image_file_title(item: SteamNewsItem, index: int, image_url: str) -> str:
    return f"File:Icarus_Steam_News_{item.gid}_Body_{index:02d}{guess_image_suffix(image_url)}"


def build_file_embed(file_title: str) -> str:
    return f"[[{file_title}|center|{WIKI_EMBED_IMAGE_WIDTH_PX}px]]"


def convert_url_tag(match: re.Match[str]) -> str:
    url = match.group(1).strip()
    label = normalize_external_link_label(match.group(2))
    return f"[{url} {label}]"


def convert_bare_url_tag(match: re.Match[str]) -> str:
    url = match.group(1).strip()
    return f"[{url} {url}]"


def convert_heading(match: re.Match[str]) -> str:
    tag = match.group(1).lower()
    inner = normalize_external_link_label(match.group(2))
    level_map = {
        "h1": "==",
        "h2": "===",
        "h3": "====",
    }
    marker = level_map[tag]
    return f"\n{marker} {inner} {marker}\n"


def resolve_steam_image_url(raw_value: str) -> str | None:
    candidate = raw_value.strip()
    if not candidate:
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    if candidate.startswith(STEAM_CLAN_IMAGE_TOKEN + "/"):
        suffix = candidate[len(STEAM_CLAN_IMAGE_TOKEN) :]
        return f"{STEAM_CLAN_IMAGE_BASE_URL}{suffix}"
    return None


def render_external_image_link(image_url: str) -> str:
    return f"\n[{image_url} Steam image]\n"


def build_upload_description(
    *,
    page_title: str,
    item: SteamNewsItem,
    source_url: str,
    role_label: str,
) -> str:
    lines = [
        "== Summary ==",
        "Imported automatically from Steam Community Announcements.",
        "",
        f"* Source announcement: [{item.url} {item.title}]",
        f"* Source image: [{source_url} Steam CDN]",
        f"* Target news page: [[{page_title}]]",
        f"* Steam gid: <code>{item.gid}</code>",
        f"* Image role: {role_label}",
    ]
    return "\n".join(lines) + "\n"


def maybe_upload_news_image(
    client: MediaWikiClient,
    *,
    dry_run: bool,
    upload_cache: dict[str, str | None],
    page_title: str,
    item: SteamNewsItem,
    source_url: str,
    file_title: str,
    role_label: str,
) -> str | None:
    if file_title in upload_cache:
        return upload_cache[file_title]

    if dry_run:
        upload_cache[file_title] = file_title
        return file_title

    try:
        if client.page_exists(file_title):
            print(f"  Reusing uploaded image {file_title}")
            upload_cache[file_title] = file_title
            return file_title
    except MediaWikiError as exc:
        warn(f"Unable to check wiki file {file_title} for {item.gid}: {exc}")
        upload_cache[file_title] = None
        return None

    try:
        image_bytes, content_type = request_bytes(source_url)
    except RuntimeError as exc:
        warn(f"Unable to download Steam image for {item.gid} ({role_label}): {exc}")
        upload_cache[file_title] = None
        return None

    try:
        client.upload_file(
            strip_file_namespace(file_title),
            image_bytes,
            comment=f"Automated Steam news image import ({item.gid}, {role_label})",
            text=build_upload_description(
                page_title=page_title,
                item=item,
                source_url=source_url,
                role_label=role_label,
            ),
            content_type=content_type,
        )
    except MediaWikiError as exc:
        warn(f"Unable to upload wiki image {file_title} for {item.gid}: {exc}")
        upload_cache[file_title] = None
        return None

    print(f"  Uploaded image {file_title}")
    upload_cache[file_title] = file_title
    return file_title


def build_body_image_render_plans(
    item: SteamNewsItem,
    *,
    client: MediaWikiClient,
    dry_run: bool,
    page_title: str,
    upload_cache: dict[str, str | None],
) -> list[BodyImageRenderPlan]:
    plans: list[BodyImageRenderPlan] = []

    for index, match in enumerate(IMAGE_TAG_PATTERN.finditer(item.contents), start=1):
        source_url = resolve_steam_image_url(match.group(1))
        if source_url is None:
            plans.append(BodyImageRenderPlan(source_url=None, file_title=None))
            continue

        file_title = build_body_image_file_title(item, index, source_url)
        uploaded_file_title = maybe_upload_news_image(
            client,
            dry_run=dry_run,
            upload_cache=upload_cache,
            page_title=page_title,
            item=item,
            source_url=source_url,
            file_title=file_title,
            role_label=f"body-{index:02d}",
        )
        plans.append(
            BodyImageRenderPlan(
                source_url=source_url,
                file_title=uploaded_file_title,
            )
        )

    return plans


def convert_quote_tag(match: re.Match[str]) -> str:
    inner = match.group(1).strip()
    if not inner:
        return ""
    return f"\n<blockquote>\n{inner}\n</blockquote>\n"


def strip_unsupported_bbcode(text: str) -> str:
    return re.sub(r"\[/?[A-Za-z][A-Za-z0-9_]*(?:[ =][^\]]*)?\]", "", text)


def cleanup_wikitext(text: str) -> str:
    raw_lines = [line.rstrip() for line in text.splitlines()]
    lines: list[str] = []

    for index, line in enumerate(raw_lines):
        stripped = line.lstrip()
        if stripped.startswith("* "):
            line = "* " + stripped[2:].lstrip()
        elif stripped == "*":
            line = "*"

        if (
            line == ""
            and lines
            and lines[-1].startswith("* ")
        ):
            next_non_empty = ""
            for future_line in raw_lines[index + 1 :]:
                if future_line.strip():
                    next_non_empty = future_line.lstrip()
                    break
            if next_non_empty.startswith("* "):
                continue

        lines.append(line)

    collapsed = "\n".join(lines)
    collapsed = re.sub(r"\n{3,}", "\n\n", collapsed)
    return collapsed.strip()


def convert_special_lines(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue
        if stripped.startswith("^"):
            continuation = stripped[1:].lstrip()
            if continuation:
                lines.append(f"<br>{continuation}")
            continue
        if STANDALONE_URL_PATTERN.fullmatch(stripped):
            lines.append(f"[{stripped} {stripped}]")
            continue
        lines.append(line)
    return "\n".join(lines)


def convert_bbcode_to_wikitext(
    text: str,
    *,
    body_images: list[BodyImageRenderPlan],
) -> str:
    converted = normalize_text(text)
    converted = re.sub(r"\[expand[^\]]*\]", "\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[/expand\]", "\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[hr\](?:\[/hr\])?", "\n----\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[/hr\]", "\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[p\]", "", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[/p\]", "\n\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[br\s*/?\]", "<br>", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[list[^\]]*\]", "\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[/list\]", "\n", converted, flags=re.IGNORECASE)
    converted = re.sub(r"\[\*\]", "\n* ", converted)
    converted = re.sub(r"\[/\*\]", "\n", converted)
    converted = re.sub(
        r"\[url=(.*?)\](.*?)\[/url\]",
        convert_url_tag,
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )
    converted = re.sub(
        r"\[url\](.*?)\[/url\]",
        convert_bare_url_tag,
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )
    image_index = 0

    def convert_image_tag(match: re.Match[str]) -> str:
        nonlocal image_index
        plan = body_images[image_index] if image_index < len(body_images) else None
        image_index += 1

        if plan is not None and plan.file_title:
            return f"\n{build_file_embed(plan.file_title)}\n"
        if plan is not None and plan.source_url:
            return render_external_image_link(plan.source_url)

        image_url = resolve_steam_image_url(match.group(1))
        if image_url is None:
            return ""
        return render_external_image_link(image_url)

    converted = IMAGE_TAG_PATTERN.sub(convert_image_tag, converted)
    converted = re.sub(
        r"\[(h[1-3])\](.*?)\[/\1\]",
        convert_heading,
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )
    converted = re.sub(
        r"\[quote\](.*?)\[/quote\]",
        convert_quote_tag,
        converted,
        flags=re.IGNORECASE | re.DOTALL,
    )
    converted = re.sub(r"\[b\](.*?)\[/b\]", r"'''\1'''", converted, flags=re.IGNORECASE | re.DOTALL)
    converted = re.sub(r"\[i\](.*?)\[/i\]", r"''\1''", converted, flags=re.IGNORECASE | re.DOTALL)
    converted = strip_unsupported_bbcode(converted)
    converted = convert_special_lines(converted)
    return cleanup_wikitext(converted)


def escape_display_title(title: str) -> str:
    return html.escape(title, quote=False).replace("|", "&#124;")


def escape_template_parameter_value(value: str) -> str:
    normalized = normalize_text(value).replace("\n", " ").strip()
    normalized = WHITESPACE_PATTERN.sub(" ", normalized)
    return normalized.replace("|", "{{!}}")


def format_utc_datetime_value(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M:%S")


def format_publish_display_date(timestamp: int) -> str:
    published = datetime.fromtimestamp(timestamp, UTC)
    return published.strftime("%a, %B ") + str(published.day) + published.strftime(", %Y")


def format_publish_year(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y")


def extract_changelog_version(item: SteamNewsItem) -> str | None:
    source_text = f"{item.title}\n{item.contents}"
    for pattern in VERSION_PATTERNS:
        match = pattern.search(source_text)
        if match is not None:
            return match.group(1)
    return None


def resolve_related_changelog(
    item: SteamNewsItem,
    client: MediaWikiClient,
    cache: dict[str, bool],
) -> str | None:
    version = extract_changelog_version(item)
    if version is None:
        return None

    page_title = f"Changelog:V{version}"
    if page_title not in cache:
        cache[page_title] = client.page_exists(page_title)
    if cache[page_title]:
        return page_title
    return None


def build_news_info_template(
    item: SteamNewsItem,
    *,
    related_changelog: str | None,
    subtitle: str | None,
    header_image_file: str | None,
) -> str:
    fields = [
        ("gid", item.gid),
        ("steamTitle", item.title),
        ("publishDateUtc", format_utc_datetime_value(item.date)),
        ("publishDisplayDate", format_publish_display_date(item.date)),
        ("publishYear", format_publish_year(item.date)),
        ("steamUrl", item.url),
        ("author", item.author),
        ("feedLabel", item.feedlabel),
        ("subtitle", subtitle or ""),
        ("headerImageFile", header_image_file or ""),
        ("relatedChangelog", related_changelog or ""),
    ]

    lines = [f"{{{{{NEWS_INFO_TEMPLATE_NAME}"]
    for name, value in fields:
        lines.append(f"|{name}={escape_template_parameter_value(value)}")
    lines.append("}}")
    return "\n".join(lines)


def build_page_text(
    item: SteamNewsItem,
    *,
    related_changelog: str | None,
    subtitle: str | None,
    header_image_file: str | None,
    body_images: list[BodyImageRenderPlan],
) -> str:
    body = convert_bbcode_to_wikitext(item.contents, body_images=body_images)
    lines = [
        f"{{{{DISPLAYTITLE:1={escape_display_title(item.title)}}}}}",
        f"<!-- ICARUS_STEAM_NEWS_GID:{item.gid} -->",
        build_news_info_template(
            item,
            related_changelog=related_changelog,
            subtitle=subtitle,
            header_image_file=header_image_file,
        ),
    ]

    if body:
        lines.extend(["", body])

    lines.extend(
        [
            "",
            "----",
            (
                "''This page was imported automatically from Steam. Images are mirrored from "
                "Steam where available, with original Steam links used as fallback when needed.''"
            ),
        ]
    )
    return cleanup_wikitext("\n".join(lines)) + "\n"


def write_github_output(path: Path, outputs: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def summarize_mode(mode: str, target_count: int, selected_count: int) -> None:
    print(f"Mode: {mode}")
    print(f"Target Steam posts: {target_count}")
    print(f"Selected for this run: {selected_count}")


def select_target_items_for_run(
    target_items: list[SteamNewsItem],
    *,
    max_items: int | None,
) -> list[SteamNewsItem]:
    if max_items is None or max_items >= len(target_items):
        return list(target_items)
    return list(target_items[-max_items:])


def main() -> None:
    args = parse_args()

    if args.max_items is not None:
        ensure_positive_int(args.max_items, "--max-items")

    last_gid = args.last_gid.strip() if args.last_gid else None
    if args.mode == "incremental":
        last_gid = validate_gid(last_gid, label="--last-gid")
    elif last_gid is not None:
        last_gid = validate_gid(last_gid, label="--last-gid")

    target_items, newest_available_gid = collect_target_items(
        args.steam_app_id,
        mode=args.mode,
        last_gid=last_gid,
    )

    target_count = len(target_items)
    selected_target_items = select_target_items_for_run(
        target_items,
        max_items=args.max_items,
    )
    selected_items = list(reversed(selected_target_items))
    selected_count = len(selected_items)
    newest_target_gid = selected_target_items[0].gid if selected_target_items else ""
    advance_state = "false"

    summarize_mode(args.mode, target_count, selected_count)
    if newest_available_gid:
        print(f"Newest available Steam gid: {newest_available_gid}")
    if args.max_items is not None and target_count > selected_count:
        print("Capped run: processing the oldest pending batch so state can advance safely.")
    if newest_target_gid:
        print(f"State advance gid for this batch: {newest_target_gid}")

    username = os.environ.get(WIKIGG_USERNAME_ENV, "").strip()
    password = os.environ.get(WIKIGG_APP_PASSWORD_ENV, "").strip()
    client = MediaWikiClient(
        args.wiki_api_url,
        user_agent=USER_AGENT,
        username=username,
        password=password,
    )

    if not args.dry_run:
        if not username:
            fail(f"Missing required environment variable: {WIKIGG_USERNAME_ENV}")
        if not password:
            fail(f"Missing required environment variable: {WIKIGG_APP_PASSWORD_ENV}")
        try:
            if selected_count > 0:
                print(
                    "Wiki edit pacing: "
                    f"{client.min_edit_interval_seconds:.1f}s minimum between edits, "
                    f"with up to {client.max_rate_limit_retries} ratelimit retries."
                )
            print(f"Logging into wiki API at {args.wiki_api_url} as {username}")
            client.login()
        except MediaWikiError as exc:
            fail(str(exc))

    changelog_cache: dict[str, bool] = {}
    upload_cache: dict[str, str | None] = {}
    changed_count = 0
    skipped_count = 0

    for item in selected_items:
        try:
            page_title, existing_text = resolve_page_title(client, item)
            related_changelog = resolve_related_changelog(item, client, changelog_cache)
        except MediaWikiError as exc:
            fail(str(exc))

        page_data = fetch_announcement_page_data(item)
        header_image_file: str | None = None
        if page_data.hero_image_url:
            header_image_file = maybe_upload_news_image(
                client,
                dry_run=args.dry_run,
                upload_cache=upload_cache,
                page_title=page_title,
                item=item,
                source_url=page_data.hero_image_url,
                file_title=build_header_image_file_title(item, page_data.hero_image_url),
                role_label="header",
            )

        body_images = build_body_image_render_plans(
            item,
            client=client,
            dry_run=args.dry_run,
            page_title=page_title,
            upload_cache=upload_cache,
        )
        page_text = build_page_text(
            item,
            related_changelog=related_changelog,
            subtitle=page_data.subtitle,
            header_image_file=header_image_file,
            body_images=body_images,
        )

        print(f"Processing {item.gid} -> {page_title}")
        if page_data.subtitle:
            print(f"  Subtitle: {page_data.subtitle}")
        if header_image_file:
            print(f"  Header image: {header_image_file}")
        if existing_text == page_text:
            skipped_count += 1
            print("  Unchanged on wiki; skipping edit")
            continue

        if args.dry_run:
            changed_count += 1
            print("  Dry run: page would be created or updated")
            continue

        try:
            client.edit_page(
                page_title,
                page_text,
                summary=f"Automated Steam news import ({item.gid})",
                nocreate=False,
            )
        except MediaWikiError as exc:
            fail(str(exc))

        changed_count += 1
        print("  Updated wiki page")

    if not args.dry_run and selected_count > 0:
        advance_state = "true"

    print(
        f"Finished Steam news sync: {changed_count} changed, "
        f"{skipped_count} unchanged, {target_count} target items"
    )

    if args.github_output:
        outputs = {
            "target_count": str(target_count),
            "selected_count": str(selected_count),
            "changed_count": str(changed_count),
            "skipped_count": str(skipped_count),
            "advance_state": advance_state,
            "newest_target_gid": newest_target_gid,
            "newest_available_gid": newest_available_gid or "",
        }
        write_github_output(args.github_output, outputs)


if __name__ == "__main__":
    main()
