"""
Unit tests for the ``Waiter`` base class used by every backend task.
"""

import logging
from unittest.mock import MagicMock

import pytest

from xapi_db_load.waiter import Waiter


class _TestWaiter(Waiter):
    """Concrete subclass that sets ``task_name`` so ``__init__`` succeeds."""

    task_name = "test"


@pytest.fixture
def waiter() -> Waiter:
    """Return a ready-to-use ``_TestWaiter`` with a stub EventGenerator."""
    event_generator = MagicMock()
    event_generator.setup_complete = True
    return _TestWaiter({}, logging.getLogger("test"), event_generator)


def test_initial_state(waiter):
    """Counters and percentages start at zero, and ``finished`` is False."""
    assert waiter.complete_pct == 0.0
    assert waiter.total_task_count == 0
    assert waiter.completed_task_count == 0
    assert waiter.finished is False
    assert waiter.get_complete() == 0.0


def test_update_counts_drive_percentage(waiter):
    """Updating total then completed counts produces the correct percentage."""
    waiter.update_total_task_count(10)
    waiter.update_completed_task_count(3)

    assert waiter.total_task_count == 10
    assert waiter.completed_task_count == 3
    assert waiter.get_complete() == pytest.approx(0.3)


def test_update_complete_pct_handles_zero_total(waiter, caplog):
    """``update_complete_pct`` swallows ZeroDivisionError and leaves pct unchanged."""
    # total stays at 0 - the next call should not raise
    waiter.completed_task_count = 1
    with caplog.at_level(logging.DEBUG, logger="test"):
        waiter.update_complete_pct()

    # Percentage is unchanged from its initial value.
    assert waiter.complete_pct == 0.0


def test_update_complete_pct_propagates_real_errors(waiter):
    """Unexpected errors should propagate rather than being silently swallowed."""
    waiter.total_task_count = 10
    waiter.completed_task_count = "not a number"  # type: ignore[assignment]
    with pytest.raises(TypeError):
        waiter.update_complete_pct()


def test_finish_marks_complete(waiter):
    """``finish()`` sets ``finished`` and forces percentage to 1.0."""
    waiter.update_total_task_count(5)
    waiter.update_completed_task_count(2)
    waiter.finish()

    assert waiter.finished is True
    assert waiter.complete_pct == 1.0


def test_reset_clears_state(waiter):
    """``reset()`` returns the task to its initial state for re-runs."""
    waiter.update_total_task_count(10)
    waiter.update_completed_task_count(5)
    waiter.finish()

    waiter.reset()

    assert waiter.complete_pct == 0.0
    assert waiter.total_task_count == 0
    assert waiter.completed_task_count == 0
    assert waiter.finished is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name",
    ["_run_task", "_run_db_load_task"],
)
async def test_abstract_methods_raise(waiter, method_name):
    """The default async hooks raise ``NotImplementedError`` until subclassed."""
    with pytest.raises(NotImplementedError):
        await getattr(waiter, method_name)()
