"""Shared HTTP utilities for the ingestion clients.

Provides factory for configured httpx.Client with:
- Custom User-Agent
- Sensible timeouts
- JSON defaults

The retry logic isn't handled here—each client manages its own retries
in its own way, because APIs have different error patterns.
"""

from __future__ import annotations

import httpx

# Default timeouts per type of operation
# Without these, httpx has no timeout — it hangs forever.
DEFAULT_TIMEOUT = httpx.Timeout(
    connect=5.0,
    read=30.0,
    write=10.0,
    pool=5.0,
)


def make_http_client(
    *,
    user_agent: str = "ARGOS-Bot/0.1",
    base_url: str = "",
    timeout: httpx.Timeout = DEFAULT_TIMEOUT,
) -> httpx.Client:
    """Creates a configured httpx.Client.

    Args:
        user_agent: User-Agent header for all requests.
        base_url: Optional base URL for relative paths.
        timeout: Custom timeouts.

    Returns:
        Configured httpx.Client. Must be closed manually or with
        context manager (with make_http_client(...) as client:).
    """
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    return httpx.Client(
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    )
