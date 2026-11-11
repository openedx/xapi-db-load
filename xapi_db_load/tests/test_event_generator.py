"""
Validation tests for ``EventGenerator._validate_config`` and basic setup.
"""

import asyncio
import datetime
import logging
import re
import statistics

import pytest

from xapi_db_load.constants import (
    COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE,
    DEFAULT_FIRST_CATALOG_PATHWAY_ID,
    DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID,
    DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
    DEFAULT_NUM_CATALOG_PATHWAYS,
    DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY,
    PATHWAY_CATEGORIES,
)
from xapi_db_load.generate_load_async import (
    EventGenerator,
    to_sink_datetime,
)


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
        created = datetime.datetime.fromisoformat(p["created"])
        modified = datetime.datetime.fromisoformat(p["modified"])
        assert created <= modified
        assert gen.start_date <= created.date()
        assert modified.date() <= gen.end_date


def test_catalog_pathways_can_be_disabled():
    """num_catalog_pathways: 0 creates no pathways."""
    gen = _make_generator(_base_config(num_catalog_pathways=0))
    gen.setup_orgs()
    gen.setup_catalog_pathways()

    assert not gen.catalog_pathways


# The format DRF's DateTimeField writes, which the platform-plugin-aspects
# sinks send to ClickHouse, e.g. 2026-10-29T21:18:23.286693Z.
SINK_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?Z$")


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            datetime.datetime(2026, 10, 29, 21, 18, 23, 286693, tzinfo=datetime.UTC),
            "2026-10-29T21:18:23.286693Z",
        ),
        # DRF drops zero microseconds, because isoformat() does.
        (
            datetime.datetime(2026, 1, 1, 12, 34, 56, tzinfo=datetime.UTC),
            "2026-01-01T12:34:56Z",
        ),
        # Other offsets are converted to UTC first.
        (
            datetime.datetime(
                2026,
                1,
                1,
                7,
                34,
                56,
                tzinfo=datetime.timezone(datetime.timedelta(hours=-5)),
            ),
            "2026-01-01T12:34:56Z",
        ),
    ],
)
def test_to_sink_datetime(value, expected):
    """Datetimes are formatted the way the LMS sinks write them."""
    assert to_sink_datetime(value) == expected


def test_catalog_pathways_use_sink_datetime_format():
    """created and modified are strings in the format the LMS sink writes."""
    gen = _make_generator(_base_config())
    gen.setup_orgs()
    gen.setup_catalog_pathways()

    for p in gen.catalog_pathways:
        assert SINK_DATETIME_RE.match(p["created"]), p["created"]
        assert SINK_DATETIME_RE.match(p["modified"]), p["modified"]


async def _make_enrolled_generator(**overrides) -> EventGenerator:
    """Run the whole setup, which ends by enrolling learners in the pathways."""
    gen = _make_generator(_base_config(**overrides))
    await gen.run_task()
    return gen


def test_pathway_enrollments_defaults():
    """
    Each pathway gets the default number of distinct learners, ids count up
    from the default first id, and every actor_id is a known actor's xAPI id.
    """
    gen = asyncio.run(_make_enrolled_generator(num_organizations=3))
    enrollments = gen.pathway_enrollments
    per_pathway = min(DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY, len(gen.actors))

    assert len(enrollments) == DEFAULT_NUM_CATALOG_PATHWAYS * per_pathway
    assert [e["id"] for e in enrollments] == list(
        range(
            DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID,
            DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID + len(enrollments),
        )
    )
    actor_ids = {a.id for a in gen.actors}
    pathways = {p["id"]: p for p in gen.catalog_pathways}
    for pathway_id in pathways:
        learners = [
            e["actor_id"] for e in enrollments if e["catalog_pathway_id"] == pathway_id
        ]
        assert len(learners) == len(set(learners)) == per_pathway
        assert set(learners) <= actor_ids

    for e in enrollments:
        assert SINK_DATETIME_RE.match(e["created"]), e["created"]
        assert SINK_DATETIME_RE.match(e["modified"]), e["modified"]
        created = datetime.datetime.fromisoformat(e["created"])
        modified = datetime.datetime.fromisoformat(e["modified"])
        pathway_created = datetime.datetime.fromisoformat(
            pathways[e["catalog_pathway_id"]]["created"]
        )
        assert pathway_created <= created <= modified
        assert modified.date() <= gen.end_date
        if e["is_active"]:
            assert created == modified


def test_pathway_enrollments_prefer_learners_of_the_pathway_org():
    """
    Learners come from the pathway org's courses first, so their xAPI activity
    is in the same org.
    """
    gen = asyncio.run(
        _make_enrolled_generator(
            num_organizations=1,
            num_actors=5,
            num_catalog_pathways=1,
            # Courses pick their actors with replacement, so only one distinct
            # course learner is guaranteed.
            num_enrollments_per_pathway=1,
        )
    )
    course_learners = {ea.actor.id for c in gen.courses for ea in c.actors}

    assert len(gen.pathway_enrollments) == 1
    assert gen.pathway_enrollments[0]["actor_id"] in course_learners


def test_pathway_enrollments_capped_at_num_actors():
    """A pathway can't have more distinct learners than there are actors."""
    gen = asyncio.run(
        _make_enrolled_generator(
            num_actors=3, num_catalog_pathways=2, num_enrollments_per_pathway=10
        )
    )

    assert len(gen.pathway_enrollments) == 2 * 3


def test_pathway_enrollments_first_id_and_inactive():
    """first_pathway_enrollment_id moves the ids, and some learners unenroll."""
    gen = asyncio.run(
        _make_enrolled_generator(
            num_actors=50,
            num_catalog_pathways=10,
            num_enrollments_per_pathway=50,
            first_pathway_enrollment_id=7,
        )
    )
    enrollments = gen.pathway_enrollments

    assert enrollments[0]["id"] == 7
    # 500 enrollments at a 10% unenroll ratio; both kinds are all but certain.
    assert {e["is_active"] for e in enrollments} == {True, False}


def test_pathway_enrollments_none_without_pathways():
    """num_catalog_pathways: 0 also means no enrollments."""
    gen = asyncio.run(_make_enrolled_generator(num_catalog_pathways=0))

    assert not gen.pathway_enrollments


def _competency_generator(**overrides) -> EventGenerator:
    """A whole setup whose fake taxonomy is a competency taxonomy, with enough learners."""
    config = {
        "taxonomy_export_id": "competency-music",
        "num_actors": 40,
        "num_course_sizes": {"small": 3},
        "course_size_makeup": {
            "small": {
                "actors": 30,
                "problems": 10,
                "videos": 5,
                "chapters": 2,
                "sequences": 6,
                "verticals": 8,
                "forum_posts": 1,
            }
        },
    }
    config.update(overrides)
    return asyncio.run(_make_enrolled_generator(**config))


def test_org_prefix():
    """org_prefix names the fake organizations, so a load can be told apart in the org filters."""
    gen = _make_generator(_base_config(num_organizations=2, org_prefix="Competency"))
    gen.setup_orgs()

    assert gen.orgs == ["Competency0", "Competency1"]


def test_first_taxonomy_tag_and_object_tag_ids():
    """
    The first_* ids move the fake taxonomy, tag and object tag ids, tag ids are
    unique across taxonomies, and object tag ids count up across courses.
    """
    gen = _competency_generator(first_taxonomy_id=500, first_tag_id=1000, first_object_tag_id=7000)

    assert list(gen.taxonomy_ids.values()) == [500]
    assert [t["tag_id"] for t in gen.tags] == list(range(1000, 1000 + len(gen.tags)))
    object_tag_ids = [
        o["object_tag_id"]
        for c in gen.courses
        for o in c.serialize_object_tag_data_for_event_sink()
    ]
    assert object_tag_ids == list(range(7000, 7000 + len(object_tag_ids)))
    assert {
        o["export_id"]
        for c in gen.courses
        for o in c.serialize_object_tag_data_for_event_sink()
    } == {"competency-music"}


def test_object_tags_are_stable():
    """Every call returns the same object tags, so all backends write the same rows."""
    gen = _competency_generator()
    course = gen.courses[0]

    assert course.serialize_object_tag_data_for_event_sink() is course.serialize_object_tag_data_for_event_sink()


def test_no_competency_statuses_without_a_competency_taxonomy():
    """The default export id isn't a competency taxonomy, so no statuses are generated."""
    gen = _competency_generator(taxonomy_export_id="fake export id")

    assert not gen.student_competency_statuses


def test_competency_statuses_follow_core_rules():
    """
    One row per learner and competency, only PartiallyAttempted or Demonstrated,
    created before modified, a Partially row unchanged since created, ids from the
    default first id, and every learner a known actor.
    """
    gen = _competency_generator()
    statuses = gen.student_competency_statuses
    actor_ids = {a.id for a in gen.actors}

    assert statuses
    assert [s["id"] for s in statuses] == list(
        range(
            DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
            DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID + len(statuses),
        )
    )
    assert len({(s["actor_id"], s["tag_id"]) for s in statuses}) == len(statuses)
    assert {(s["status_id"], s["status"]) for s in statuses} == {
        (20, "PartiallyAttempted"),
        (30, "Demonstrated"),
    }
    for s in statuses:
        assert s["actor_id"] in actor_ids
        assert SINK_DATETIME_RE.match(s["created"]), s["created"]
        assert SINK_DATETIME_RE.match(s["modified"]), s["modified"]
        created = datetime.datetime.fromisoformat(s["created"])
        modified = datetime.datetime.fromisoformat(s["modified"])
        assert created <= modified
        assert modified.date() <= gen.end_date
        if s["status_id"] == 20:
            assert created == modified


def test_competency_statuses_only_for_assessed_competencies_and_their_parents():
    """
    A learner's statuses cover competencies tagged on their courses' blocks, plus
    ancestors, and a parent is Demonstrated only when every child the learner has a
    status on is.
    """
    gen = _competency_generator()
    tags = {t["tag_id"]: t for t in gen.tags}
    assessed_by_actor: dict = {}
    for course in gen.courses:
        tagged = {o["tag_id"] for o in course.serialize_object_tag_data_for_event_sink()}
        for enrolled in course.actors:
            assessed_by_actor.setdefault(enrolled.actor.id, set()).update(tagged)

    def ancestors(tag_id):
        parent = tags[tag_id]["parent_int_id"]
        return set() if not parent else {parent} | ancestors(parent)

    by_actor: dict = {}
    for s in gen.student_competency_statuses:
        by_actor.setdefault(s["actor_id"], {})[s["tag_id"]] = s
    for actor_id, rows in by_actor.items():
        allowed = set(assessed_by_actor[actor_id])
        for tag_id in assessed_by_actor[actor_id]:
            allowed |= ancestors(tag_id)
        assert set(rows) <= allowed
        for tag_id, row in rows.items():
            children = [r for t, r in rows.items() if tags[t]["parent_int_id"] == tag_id]
            if children and tag_id not in assessed_by_actor[actor_id]:
                all_done = all(c["status_id"] == 30 for c in children)
                assert (row["status_id"] == 30) == all_done


def test_competency_statuses_spread_time_to_mastery():
    """
    Competencies differ in difficulty, from hours to weeks, so their time-to-mastery
    boxes sit at different heights; within one competency learners still vary.
    """
    gen = _competency_generator(num_actors=200, course_size_makeup={
        "small": {
            "actors": 150, "problems": 10, "videos": 5, "chapters": 2,
            "sequences": 6, "verticals": 8, "forum_posts": 1,
        }
    })
    medians = gen.competency_mastery_median_days
    low, high = COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE
    assert all(low <= m <= high for m in medians.values())
    assert max(medians.values()) / min(medians.values()) > 10

    tags = {t["tag_id"]: t for t in gen.tags}
    leaves = {t for t in tags if not any(o["parent_int_id"] == t for o in tags.values())}
    by_tag: dict = {}
    for s in gen.student_competency_statuses:
        if s["status_id"] == 30 and s["tag_id"] in leaves:
            days = (
                datetime.datetime.fromisoformat(s["modified"])
                - datetime.datetime.fromisoformat(s["created"])
            ).total_seconds() / 86400
            by_tag.setdefault(s["tag_id"], []).append(days)

    observed = {t: statistics.median(d) for t, d in by_tag.items() if len(d) >= 20}
    assert len(observed) >= 3
    fastest, slowest = min(observed, key=observed.get), max(observed, key=observed.get)
    assert observed[slowest] > 5 * observed[fastest]
    assert medians[slowest] > medians[fastest]
    for days in by_tag.values():
        if len(days) >= 20:
            assert max(days) > 1.5 * min(days)


def test_first_competency_status_lands_soon_after_enrollment():
    """A learner's first status comes soon after they enroll, not anywhere in the course."""
    gen = _competency_generator()
    enrolled = {}
    for course in gen.courses:
        for e in course.actors:
            when = e.enroll_datetime
            if not isinstance(when, datetime.datetime):
                when = datetime.datetime.combine(when, datetime.time(), tzinfo=datetime.UTC)
            enrolled[e.actor.id] = min(enrolled.get(e.actor.id, when), when)
    gaps = [
        (datetime.datetime.fromisoformat(s["created"]) - enrolled[s["actor_id"]]).total_seconds() / 3600
        for s in gen.student_competency_statuses
    ]
    assert min(gaps) >= 0
    assert statistics.median(gaps) < 48


def test_course_group_statuses_include_attempted_not_demonstrated():
    """
    Course-level group statuses use all three stored values, including the
    AttemptedNotDemonstrated that the competency level never holds, one row per
    learner, competency and course, with ids from the default first id.
    """
    gen = _competency_generator()
    rows = gen.student_competency_criteria_group_statuses

    assert rows
    assert {r["status_id"] for r in rows} == {10, 20, 30}
    assert {r["group_level"] for r in rows} == {"course"}
    assert [r["id"] for r in rows] == list(
        range(
            DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID,
            DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID + len(rows),
        )
    )
    assert len({(r["actor_id"], r["tag_id"], r["course_key"]) for r in rows}) == len(rows)
    course_keys = {c.course_id for c in gen.courses}
    assert {r["course_key"] for r in rows} <= course_keys
    # One course group per competency and course, all under the competency's root group.
    groups = {(r["tag_id"], r["course_key"]): r["group_id"] for r in rows}
    assert len(set(groups.values())) == len(groups)
    roots = {r["tag_id"]: r["parent_group_id"] for r in rows}
    assert all(r["parent_group_id"] == roots[r["tag_id"]] for r in rows)
    assert not set(roots.values()) & set(groups.values())


def test_competency_status_agrees_with_course_group_statuses():
    """
    The competency-level row is Demonstrated exactly when some course group is,
    and otherwise PartiallyAttempted, including when every course is
    AttemptedNotDemonstrated, as core's rollup records it.
    """
    gen = _competency_generator()
    by_pair: dict = {}
    for r in gen.student_competency_criteria_group_statuses:
        by_pair.setdefault((r["actor_id"], r["tag_id"]), []).append(r["status_id"])
    competency = {(s["actor_id"], s["tag_id"]): s["status_id"] for s in gen.student_competency_statuses}

    assert by_pair
    for pair, course_statuses in by_pair.items():
        expected = 30 if 30 in course_statuses else 20
        assert competency[pair] == expected
