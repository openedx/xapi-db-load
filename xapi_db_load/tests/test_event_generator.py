"""
Validation tests for ``EventGenerator._validate_config`` and basic setup.
"""

import datetime
import logging

import pytest

from xapi_db_load.generate_load_async import EventGenerator


def _base_config(**overrides) -> dict:
    """Return a valid config dict that can be tweaked per-test via overrides."""
    base = {
        "lms_url": "http://localhost:18000",
        "start_date": datetime.date(2023, 1, 1),
        "end_date": datetime.date(2023, 6, 1),
        "course_length_days": 30,
        "num_organizations": 1,
        "num_actors": 5,
        "num_course_sizes": {"small": 1},
        "num_course_publishes": 1,
        "course_size_makeup": {
            "small": {
                "actors": 2,
                "problems": 1,
                "videos": 1,
                "chapters": 1,
                "sequences": 1,
                "verticals": 1,
                "forum_posts": 1,
            }
        },
        "batch_size": 1,
        "num_xapi_batches": 1,
        "num_actor_profile_changes": 1,
    }
    base.update(overrides)
    return base


def _make_generator(config: dict) -> EventGenerator:
    return EventGenerator(config, logging.getLogger("test"), None)


def test_valid_config_does_not_raise():
    """A correctly-formed config produces an EventGenerator without error."""
    _make_generator(_base_config())


@pytest.mark.parametrize(
    "overrides,expected_substring",
    [
        pytest.param(
            {
                "start_date": datetime.date(2023, 6, 1),
                "end_date": datetime.date(2023, 1, 1),
            },
            "Start date must be before end date",
            id="start_after_end",
        ),
        pytest.param(
            {
                "start_date": datetime.date(2023, 1, 1),
                "end_date": datetime.date(2023, 1, 1),
            },
            "Start date must be before end date",
            id="start_equals_end",
        ),
        pytest.param(
            {
                "start_date": datetime.date(2023, 1, 1),
                "end_date": datetime.date(2023, 1, 15),
                "course_length_days": 30,
            },
            "longer than course_length_days",
            id="window_shorter_than_course",
        ),
        pytest.param(
            {
                "num_actors": 2,
                "course_size_makeup": {
                    "small": {
                        "actors": 5,
                        "problems": 1,
                        "videos": 1,
                        "chapters": 1,
                        "sequences": 1,
                        "verticals": 1,
                        "forum_posts": 1,
                    }
                },
            },
            "wants more actors",
            id="actors_exceed_num_actors",
        ),
    ],
)
def test_invalid_config_raises(overrides, expected_substring):
    """Each invalid config combination raises ``ValueError`` with a helpful message."""
    with pytest.raises(ValueError, match=expected_substring):
        _make_generator(_base_config(**overrides))


def test_instances_have_isolated_state():
    """Two generators must not share ``actors``/``courses``/``orgs`` collections."""
    a = _make_generator(_base_config())
    b = _make_generator(_base_config())

    a.actors.append("sentinel")  # type: ignore[arg-type]
    a.orgs.append("sentinel")

    assert b.actors == []
    assert b.orgs == []
