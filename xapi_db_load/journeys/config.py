"""
Checks for ``journeys`` config values, so a bad value fails before any output is written.

Each check returns the value (converted where noted) or raises ValueError naming the key.
"""

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def require(conf: Dict, key: str, prefix: str = "") -> Any:
    """Return ``conf[key]``, or raise ValueError if it's missing."""
    if key not in conf:
        raise ValueError(f"Missing required setting: {prefix}{key}")
    return conf[key]


def check_int(name: str, value: Any, minimum: int = 0) -> int:
    """Check ``value`` is an integer of at least ``minimum``."""
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{name} must be an integer of at least {minimum}, got {value!r}.")
    return value


def check_number(
    name: str, value: Any, minimum: float = 0.0, maximum: Optional[float] = None
) -> float:
    """Check ``value`` is a number in [``minimum``, ``maximum``] and return it as a float."""
    if not _is_number(value) or value < minimum or (maximum is not None and value > maximum):
        bounds = f"between {minimum} and {maximum}" if maximum is not None else f">= {minimum}"
        raise ValueError(f"{name} must be a number {bounds}, got {value!r}.")
    return float(value)


def check_fraction(name: str, value: Any) -> float:
    """Check ``value`` is a probability or fraction between 0 and 1."""
    return check_number(name, value, 0.0, 1.0)


def check_bool(name: str, value: Any) -> bool:
    """Check ``value`` is true or false."""
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be true or false, got {value!r}.")
    return value


def check_range(name: str, value: Any, minimum: int = 0) -> Tuple[int, int]:
    """Check ``value`` is ``[low, high]`` integers with ``minimum <= low <= high``."""
    if (
        not isinstance(value, (list, tuple))
        or len(value) != 2
        or not all(isinstance(v, int) and not isinstance(v, bool) for v in value)
        or not minimum <= value[0] <= value[1]
    ):
        raise ValueError(
            f"{name} must be [low, high] integers with {minimum} <= low <= high, got {value!r}."
        )
    return value[0], value[1]


def check_weights(name: str, value: Any) -> List[float]:
    """Check ``value`` is a non-empty list of non-negative numbers that don't all equal 0."""
    if (
        not isinstance(value, (list, tuple))
        or not value
        or not all(_is_number(v) and v >= 0 for v in value)
        or not sum(value)
    ):
        raise ValueError(
            f"{name} must be a list of non-negative weights, not all 0, got {value!r}."
        )
    return [float(v) for v in value]
