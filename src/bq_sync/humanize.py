"""Human-readable formatting utilities (stdlib-only)."""

from __future__ import annotations


def humanize_bytes(n: int | None) -> str:
    """Convert a byte count to a human-readable string using binary prefixes.

    Args:
        n: Number of bytes, or ``None``.

    Returns:
        Formatted string such as ``"1.5 GiB"`` or ``""`` when *n* is
        ``None``.
    """
    if n is None:
        return ""
    if n < 0:
        return f"{n} B"

    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(n)
    for unit in units[:-1]:
        if abs(value) < 1024.0:
            return f"{value:.1f} {unit}" if unit != "B" else f"{n} B"
        value /= 1024.0
    return f"{value:.1f} {units[-1]}"
