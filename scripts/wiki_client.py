#!/usr/bin/env python3
"""
Minimal MediaWiki API client for wiki.gg automation scripts.
"""

from __future__ import annotations

import http.cookiejar
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

REQUEST_TIMEOUT_SECONDS = 30


class MediaWikiError(RuntimeError):
    """Raised when the MediaWiki API request fails."""


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
    ) -> None:
        self.api_url = api_url
        self.user_agent = user_agent
        self.username = (username or "").strip()
        self.password = (password or "").strip()
        self.csrf_token = ""
        cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookie_jar)
        )

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

        edit_data = self.request(payload, post=True)
        edit_result = edit_data.get("edit", {})
        if edit_result.get("result") != "Success":
            raise MediaWikiError(f"Edit failed for {page_title}: {edit_result}")
