#!/usr/bin/env python3
"""
Minimal MediaWiki API client for wiki.gg automation scripts.
"""

from __future__ import annotations

import http.cookiejar
import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_MIN_EDIT_INTERVAL_SECONDS = 10.0
DEFAULT_RATE_LIMIT_RETRY_SECONDS = 30.0
DEFAULT_MAX_RATE_LIMIT_RETRIES = 3


class MediaWikiError(RuntimeError):
    """Raised when the MediaWiki API request fails."""


class MediaWikiRateLimitError(MediaWikiError):
    """Raised when the MediaWiki API signals that the client is rate limited."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def normalize_text(text: str) -> str:
    return text.replace("\r\n", "\n")


@dataclass(slots=True)
class WikiPage:
    title: str
    exists: bool
    text: str


class MediaWikiClient:
    def __init__(
        self,
        api_url: str,
        *,
        user_agent: str,
        username: str | None = None,
        password: str | None = None,
        min_edit_interval_seconds: float = DEFAULT_MIN_EDIT_INTERVAL_SECONDS,
        rate_limit_retry_seconds: float = DEFAULT_RATE_LIMIT_RETRY_SECONDS,
        max_rate_limit_retries: int = DEFAULT_MAX_RATE_LIMIT_RETRIES,
    ) -> None:
        self.api_url = api_url
        self.user_agent = user_agent
        self.username = (username or "").strip()
        self.password = (password or "").strip()
        self.csrf_token = ""
        self.min_edit_interval_seconds = max(0.0, float(min_edit_interval_seconds))
        self.rate_limit_retry_seconds = max(1.0, float(rate_limit_retry_seconds))
        self.max_rate_limit_retries = max(0, int(max_rate_limit_retries))
        self._last_edit_request_finished_at: float | None = None
        cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookie_jar)
        )

    def _parse_retry_after_seconds(
        self,
        response_headers: urllib.error.HTTPError | Any,
    ) -> float | None:
        retry_after = response_headers.get("Retry-After")
        if retry_after is None:
            return None
        candidate = retry_after.strip()
        try:
            seconds = float(candidate)
        except ValueError:
            return None
        return max(0.0, seconds)

    def _wait_for_edit_slot(self) -> None:
        if self.min_edit_interval_seconds <= 0:
            return
        if self._last_edit_request_finished_at is None:
            return
        elapsed = time.monotonic() - self._last_edit_request_finished_at
        remaining = self.min_edit_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _record_edit_attempt(self) -> None:
        self._last_edit_request_finished_at = time.monotonic()

    def _get_retry_delay_seconds(
        self,
        retry_after_seconds: float | None,
        *,
        attempt_number: int,
    ) -> float:
        if retry_after_seconds is not None:
            return max(retry_after_seconds, self.min_edit_interval_seconds)
        return self.rate_limit_retry_seconds * attempt_number

    def request(self, params: dict[str, str], *, post: bool) -> dict[str, Any]:
        request_headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        payload = dict(params)
        payload["format"] = "json"

        if post:
            request = urllib.request.Request(
                self.api_url,
                data=urllib.parse.urlencode(payload).encode("utf-8"),
                headers=request_headers,
                method="POST",
            )
        else:
            query = urllib.parse.urlencode(payload)
            request = urllib.request.Request(
                f"{self.api_url}?{query}",
                headers=request_headers,
                method="GET",
            )

        try:
            with self.opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                raw_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise MediaWikiRateLimitError(
                    "MediaWiki API rate limit exceeded (HTTP 429).",
                    retry_after_seconds=self._parse_retry_after_seconds(exc.headers),
                ) from exc
            raise MediaWikiError(f"MediaWiki API HTTP error {exc.code}: {exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise MediaWikiError(f"Failed to reach MediaWiki API: {exc}") from exc
        except socket.timeout as exc:
            raise MediaWikiError(
                "Timed out while waiting for the MediaWiki API "
                f"after {REQUEST_TIMEOUT_SECONDS} seconds."
            ) from exc

        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise MediaWikiError(f"MediaWiki API returned invalid JSON: {exc}") from exc

        if "error" in data:
            error = data["error"]
            code = error.get("code", "unknown")
            info = error.get("info", "Unknown API error")
            if code == "ratelimited":
                raise MediaWikiRateLimitError(
                    f"MediaWiki API error ({code}): {info}",
                )
            raise MediaWikiError(f"MediaWiki API error ({code}): {info}")

        return data

    def login(self) -> None:
        if not self.username:
            raise MediaWikiError("Missing MediaWiki username.")
        if not self.password:
            raise MediaWikiError("Missing MediaWiki password.")

        token_data = self.request(
            {
                "action": "query",
                "meta": "tokens",
                "type": "login",
            },
            post=False,
        )
        login_token = token_data["query"]["tokens"]["logintoken"]

        login_data = self.request(
            {
                "action": "login",
                "lgname": self.username,
                "lgpassword": self.password,
                "lgtoken": login_token,
            },
            post=True,
        )
        login_result = login_data.get("login", {})
        if login_result.get("result") != "Success":
            raise MediaWikiError(f"Wiki login failed: {login_result}")

        csrf_data = self.request(
            {
                "action": "query",
                "meta": "tokens",
                "type": "csrf",
            },
            post=False,
        )
        self.csrf_token = csrf_data["query"]["tokens"]["csrftoken"]
        if not self.csrf_token:
            raise MediaWikiError("Failed to obtain CSRF token after login.")

    def fetch_page(self, page_title: str) -> WikiPage:
        page_data = self.request(
            {
                "action": "query",
                "prop": "revisions",
                "titles": page_title,
                "rvprop": "content",
                "rvslots": "main",
                "formatversion": "2",
            },
            post=False,
        )
        pages = page_data["query"]["pages"]
        if not pages:
            raise MediaWikiError(f"Wiki returned no page results for {page_title!r}")

        page = pages[0]
        resolved_title = page.get("title", page_title)
        if page.get("missing"):
            return WikiPage(title=resolved_title, exists=False, text="")

        revisions = page.get("revisions", [])
        if not revisions:
            return WikiPage(title=resolved_title, exists=True, text="")

        slot = revisions[0].get("slots", {}).get("main", {})
        return WikiPage(
            title=resolved_title,
            exists=True,
            text=normalize_text(slot.get("content", "")),
        )

    def page_exists(self, page_title: str) -> bool:
        return self.fetch_page(page_title).exists

    def edit_page(self, page_title: str, text: str, summary: str, *, nocreate: bool) -> None:
        if not self.csrf_token:
            raise MediaWikiError("Cannot edit wiki page before calling login().")

        payload = {
            "action": "edit",
            "title": page_title,
            "text": normalize_text(text),
            "summary": summary,
            "token": self.csrf_token,
        }
        if nocreate:
            payload["nocreate"] = "1"

        for attempt in range(1, self.max_rate_limit_retries + 2):
            self._wait_for_edit_slot()
            try:
                edit_data = self.request(payload, post=True)
            except MediaWikiRateLimitError as exc:
                self._record_edit_attempt()
                if attempt > self.max_rate_limit_retries:
                    raise MediaWikiError(
                        f"Edit rate limit persisted for {page_title}: {exc}"
                    ) from exc
                retry_delay = self._get_retry_delay_seconds(
                    exc.retry_after_seconds,
                    attempt_number=attempt,
                )
                time.sleep(retry_delay)
                continue
            except Exception:
                self._record_edit_attempt()
                raise

            self._record_edit_attempt()
            edit_result = edit_data.get("edit", {})
            if edit_result.get("result") != "Success":
                raise MediaWikiError(f"Edit failed for {page_title}: {edit_result}")
            return
