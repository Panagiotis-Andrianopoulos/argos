"""Key generation patterns for the object store.

Provides pure functions that build consistent keys for the data lake.
No I/O — just string building with clear rules.

The general pattern is:

    raw/<source>/<entity>/<identifier>/<date>/<filename>

For example, a FRED series metadata snapshot taken on 2026-04-22:

    raw/fred/series/QGRN628BIS/2026-04-22/metadata.json
"""

from __future__ import annotations

from datetime import date

# ============================================================
# Top-level prefixes
# ============================================================

RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"

# ============================================================
# Source identifiers
# ============================================================

SOURCE_FRED = "fred"
SOURCE_ECB = "ecb"

# ============================================================
# FRED key builders
# ============================================================


def fred_series_metadata_key(series_id: str, *, snapshot_date: date) -> str:
    """Build the key for a FRED series metadata snapshot.

    Args:
        series_id: The FRED series indentifier (e.g., "QGRN628BIS").
        snapshot_date: The date of the snapshot.

    Returns:
        The full S3 key, e.g.
        "raw/fred/series/QGRN628BIS/2026-04-22/metadata.json".
    """
    return (
        f"{RAW_PREFIX}/{SOURCE_FRED}/series/"
        f"{series_id}/{snapshot_date.isoformat()}/metadata.json"
    )


def fred_observations_key(series_id: str, *, snapshot_date: date) -> str:
    """Build the key for a FRED observations snapshot.

    Args:
        series_id: THe FRED series identifier.
        snapshot_date: The date of the snapshot.

    Returns:
        The full S3 key, e.g.
        "raw/fred/observations/QGRN628BIS/2026-04-22/observations.json".
    """
    return (
        f"{RAW_PREFIX}/{SOURCE_FRED}/observations/"
        f"{series_id}/{snapshot_date.isoformat()}/observations.json"
    )


def fred_series_prefix(series_id: str | None = None) -> str:
    """Build the prefix to list all FRED series snapshots.

    Args:
        series_id: Optional. If provided, restrict to a single series.

    Returns:
        A prefix suitable for 'ObjectStore.list_keys()'.

    Examples:
        >>> fred_series_prefix()
        'raw/fred/series/'
        >>> fred_series_prefix("QGRN628BIS")
        'raw/fred/series/QGRN628BIS/'
    """
    base = f"{RAW_PREFIX}/{SOURCE_FRED}/series/"
    if series_id is None:
        return base
    return f"{base}{series_id}/"


def fred_observations_prefix(series_id: str | None = None) -> str:
    """Build a prefix to list all FRED observations snapshots."""
    base = f"{RAW_PREFIX}/{SOURCE_FRED}/observations/"
    if series_id is None:
        return base
    return f"{base}{series_id}/"


# ============================================================
# ECB key builders
# ============================================================


def ecb_series_metadata_key(series_key: str, *, snapshot_date: date) -> str:
    """Build the key for an ECB series metadata request.

    Args:
        series_key: The full ECB series key, e.g.
            "RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX".
        snapshot_date: The date of the snapshot.

    Returns:
        The full S3 key, e.g.
        "raw/ecb/series/RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX/2026-05-05/metadata.json".
    """
    return (
        f"{RAW_PREFIX}/{SOURCE_ECB}/series/"
        f"{series_key}/{snapshot_date.isoformat()}/metadata.json"
    )


def ecb_observations_key(series_key: str, *, snapshot_date: date) -> str:
    """Build the key for ECB observations snapshots."""
    return (
        f"{RAW_PREFIX}/{SOURCE_ECB}/observations/"
        f"{series_key}/{snapshot_date.isoformat()}/observations.json"
    )


def ecb_series_prefix(series_key: str | None = None) -> str:
    """Build the prefix to list all ECB series snapshots.

    Args:
        series_key: Optiona. If provided, restrict to a single series.

    Returns:
        A prefix suitable for 'ObjectStore.list_keys()'.

    Examples:
        >>> ecb_series_prefix()
        'raw/ecb/series/'
        >>> ecb_series_prefix("RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX")
        'raw/ecb/series/RESR.Q.GR._T.N.RTF.TVAL.GR2.TB.N.IX/'
    """
    base = f"{RAW_PREFIX}/{SOURCE_ECB}/series/"
    if series_key is None:
        return base
    return f"{base}{series_key}/"


def ecb_observations_prefix(series_key: str | None = None) -> str:
    """Build the prefix to list all ECB observations snapshots."""
    base = f"{RAW_PREFIX}/{SOURCE_ECB}/observations/"
    if series_key is None:
        return base
    return f"{base}{series_key}/"
