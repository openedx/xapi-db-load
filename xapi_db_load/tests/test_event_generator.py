"""
Validation tests for ``EventGenerator._validate_config`` and basic setup.
"""

import datetime
import logging

import pytest

from xapi_db_load.constants import (
    DEFAULT_FIRST_CATALOG_PATHWAY_ID,
    DEFAULT_NUM_CATALOG_PATHWAYS,
    PATHWAY_CATEGORIES,
)
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

    assert not b.actors
    assert not b.orgs


def test_catalog_pathways_defaults():
    """
    Without pathway settings the default number of pathways is created, starting
    at the default id, and every category is used.
    """
    gen = _make_generator(_base_config(num_organizations=3))
    gen.setup_orgs()
    gen.setup_catalog_pathways()
    pathways = gen.catalog_pathways

    assert len(pathways) == DEFAULT_NUM_CATALOG_PATHWAYS
    assert pathways[0]["id"] == DEFAULT_FIRST_CATALOG_PATHWAY_ID
    assert {p["category_code"] for p in pathways} == {
        code for code, _ in PATHWAY_CATEGORIES
    }


def test_catalog_pathways_first_id():
    """first_catalog_pathway_id moves the id range."""
    gen = _make_generator(
        _base_config(num_catalog_pathways=2, first_catalog_pathway_id=500)
    )
    gen.setup_orgs()
    gen.setup_catalog_pathways()

    assert [p["id"] for p in gen.catalog_pathways] == [500, 501]


@pytest.mark.parametrize("num_orgs", [1, 2, 3, 5])
def test_catalog_pathways_spread_across_orgs_and_categories(num_orgs):
    """
    Pathways rotate through every org, every org gets every category once it has
    as many pathways as there are categories, and ids and codes are unique.
    """
    num_categories = len(PATHWAY_CATEGORIES)
    num_pathways = num_orgs * num_categories
    gen = _make_generator(
        _base_config(num_organizations=num_orgs, num_catalog_pathways=num_pathways)
    )
    gen.setup_orgs()
    gen.setup_catalog_pathways()
    pathways = gen.catalog_pathways

    assert len(pathways) == num_pathways
    assert {p["org"] for p in pathways} == set(gen.orgs)
    assert len({p["id"] for p in pathways}) == num_pathways
    assert len({(p["org"], p["pathway_code"]) for p in pathways}) == num_pathways
    for org in gen.orgs:
        org_categories = {p["category_code"] for p in pathways if p["org"] == org}
        assert len(org_categories) == num_categories
    for p in pathways:
        assert p["created"] <= p["modified"]
        assert gen.start_date <= p["created"].date()
        assert p["modified"].date() <= gen.end_date


def test_catalog_pathways_can_be_disabled():
    """num_catalog_pathways: 0 creates no pathways."""
    gen = _make_generator(_base_config(num_catalog_pathways=0))
    gen.setup_orgs()
    gen.setup_catalog_pathways()

    assert not gen.catalog_pathways
