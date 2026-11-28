#!/usr/bin/env python3
"""
Download pyuepak's Oodle library once, with retries, before anything imports pyuepak.

pyuepak fetches oo2core from GitHub the first time it is imported and caches it
next to its own package. A single transient HTTP error (e.g. 504) on that fetch
used to fail the whole extraction step. Running this right after `pip install`
makes the fetch retryable and lets later steps reuse the cached file.

Usage:
    python scripts/prefetch_oodle.py [--attempts 5] [--delay 5]
"""

from __future__ import annotations

import argparse
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--delay", type=float, default=5.0, help="Seconds before the first retry; doubles each time.")
    args = parser.parse_args()

    try:
        # Only the downloader module: it does not load the DLL or import the pak reader.
        from pyuepak.oodle import OODLE_PLATFORM, fetch_oodle
    except Exception as exc:  # noqa: BLE001
        print(f"::error::Unable to import pyuepak.oodle: {exc}", file=sys.stderr)
        return 1

    delay = args.delay
    for attempt in range(1, args.attempts + 1):
        try:
            path = fetch_oodle()  # no-op if the file is already cached; verifies the SHA-256 otherwise
        except Exception as exc:  # noqa: BLE001
            if attempt == args.attempts:
                print(f"::error::Fetching {OODLE_PLATFORM.name} failed after {attempt} attempts: {exc}", file=sys.stderr)
                return 1
            print(f"::warning::Fetching {OODLE_PLATFORM.name} failed (attempt {attempt}/{args.attempts}): {exc}; retrying in {delay:.0f}s")
            time.sleep(delay)
            delay *= 2
            continue
        print(f"Oodle ready: {path}")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
