"""
Base class for all fake xAPI events.
"""

from abc import ABC, abstractmethod


class XAPIBase(ABC):
    """
    Abstract base class for all fake xAPI event types.

    Subclasses must declare class-level ``verb`` and ``verb_display`` strings
    and implement :meth:`get_data`.
    """

    verb: str
    verb_display: str

    def __init__(self, load_generator):
        """Initialize with the parent EventGenerator instance."""
        self.parent_load_generator = load_generator

    @abstractmethod
    def get_data(self) -> dict:
        """Return a dict with event_id, verb, actor_id, emission_time, and event JSON."""
