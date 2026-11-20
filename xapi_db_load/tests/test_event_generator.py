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
    DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
    DEFAULT_NUM_CATALOG_PATHWAYS,
    DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY,
    PATHWAY_CATEGORIES,
)
from xapi_db_load.course_configs import Actor
from xapi_db_load.generate_load_async import (
    EventGenerator,
    StudentCompetencyStatusRow,
    _as_utc_datetime,
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
        pytest.param({"first_tag_id": 0}, "first_tag_id must be a positive integer", id="tag_id_zero"),
        pytest.param(
            {"first_taxonomy_id": "1000"},
            "first_taxonomy_id must be a positive integer",
            id="taxonomy_id_string",
        ),
        pytest.param(
            {"first_student_competency_course_status_id": True},
            "first_student_competency_course_status_id must be a positive integer",
            id="course_status_id_bool",
        ),
        pytest.param(
            {"taxonomy_type": "competencies"},
            "taxonomy_type must be one of tags, competency",
            id="taxonomy_type_misspelled",
        ),
        pytest.param({"org_prefix": "Org'"}, "org_prefix must be letters and digits", id="org_prefix_quote"),
        pytest.param(
            {"taxonomy_export_id": "it's"},
            "taxonomy_export_id must be a string without quotes",
            id="export_id_quote",
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
        "taxonomy_type": "competency",
        "taxonomy_export_id": "music",
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
        for o in c.get_object_tags()
    ]
    assert object_tag_ids == list(range(7000, 7000 + len(object_tag_ids)))
    assert {
        o["export_id"]
        for c in gen.courses
        for o in c.get_object_tags()
    } == {"music"}


def test_object_tags_are_stable():
    """Every call returns the same object tags, so all backends write the same rows."""
    gen = _competency_generator()
    course = gen.courses[0]

    assert course.get_object_tags() is course.get_object_tags()


def test_no_competency_statuses_without_a_competency_taxonomy():
    """A "tags" taxonomy isn't a competency taxonomy, so no statuses are generated."""
    gen = _competency_generator(taxonomy_type="tags")

    assert gen.taxonomy_type == "tags"
    assert not gen.student_competency_statuses
    assert not gen.student_competency_course_statuses


def test_taxonomy_type_defaults_to_tags():
    """Without taxonomy_type in the config, the fake taxonomy is a plain "tags" one."""
    assert "taxonomy_type" not in _base_config()
    assert _make_generator(_base_config()).taxonomy_type == "tags"


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
    assert {(s["status_id"], s["status_name"]) for s in statuses} == {
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
    ancestors. A parent is Demonstrated once every child the learner has a status
    on is, and, unless it is tagged on a block itself, only then.
    """
    gen = _competency_generator()
    tags = {t["tag_id"]: t for t in gen.tags}
    assessed_by_actor: dict = {}
    for course in gen.courses:
        tagged = {o["tag_id"] for o in course.get_object_tags()}
        for enrolled in course.actors:
            assessed_by_actor.setdefault(enrolled.actor.id, set()).update(tagged)

    def ancestors(tag_id):
        parent = tags[tag_id]["parent_int_id"]
        return set() if parent is None else {parent} | ancestors(parent)

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
            if not children:
                continue
            all_done = all(c["status_id"] == 30 for c in children)
            if tag_id in assessed_by_actor[actor_id]:
                # Tagged on a block too: rolling up only ever raises it.
                if all_done:
                    assert row["status_id"] == 30
            else:
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


def test_course_statuses_include_attempted_not_demonstrated():
    """
    Course-level group statuses use all three stored values, including the
    AttemptedNotDemonstrated that the competency level never holds, one row per
    learner, competency and course, with ids from the default first id.
    """
    gen = _competency_generator()
    rows = gen.student_competency_course_statuses

    assert rows
    assert {r["status_id"] for r in rows} == {10, 20, 30}
    assert [r["id"] for r in rows] == list(
        range(
            DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID,
            DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID + len(rows),
        )
    )
    assert len({(r["actor_id"], r["tag_id"], r["course_key"]) for r in rows}) == len(rows)
    course_keys = {c.course_id for c in gen.courses}
    assert {r["course_key"] for r in rows} <= course_keys
    # One course group per competency and course; no group tree columns.
    groups = {(r["tag_id"], r["course_key"]): r["group_id"] for r in rows}
    assert len(set(groups.values())) == len(groups)
    assert not {"parent_group_id", "group_level"} & set(rows[0])


def test_competency_status_agrees_with_course_statuses():
    """
    A leaf competency's row is Demonstrated exactly when some course group is,
    and otherwise PartiallyAttempted, including when every course is
    AttemptedNotDemonstrated, as core's rollup records it. A parent's row is at
    least that, as its children may raise it.
    """
    gen = _competency_generator()
    by_pair: dict = {}
    for r in gen.student_competency_course_statuses:
        by_pair.setdefault((r["actor_id"], r["tag_id"]), []).append(r["status_id"])
    competency = {(s["actor_id"], s["tag_id"]): s["status_id"] for s in gen.student_competency_statuses}

    parents = {t["parent_int_id"] for t in gen.tags}

    assert by_pair
    for (actor_id, tag_id), course_statuses in by_pair.items():
        from_courses = 30 if 30 in course_statuses else 20
        if tag_id in parents:
            # Its children can raise it further, never lower it.
            assert competency[(actor_id, tag_id)] >= from_courses
        else:
            assert competency[(actor_id, tag_id)] == from_courses


def test_as_utc_datetime():
    """Plain dates and naive datetimes become UTC datetimes; aware ones are kept."""
    utc = datetime.timezone.utc
    assert _as_utc_datetime(datetime.date(2024, 1, 2)) == datetime.datetime(2024, 1, 2, tzinfo=utc)
    assert _as_utc_datetime(datetime.datetime(2024, 1, 2, 3)) == datetime.datetime(2024, 1, 2, 3, tzinfo=utc)
    aware = datetime.datetime(2024, 1, 2, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
    assert _as_utc_datetime(aware) is aware


def _status(actor, status_id, created_day, modified_day=None):
    """A competency-level row created and modified on days of January 2023."""
    def day(d):
        return datetime.datetime(2023, 1, d, tzinfo=datetime.UTC)
    return StudentCompetencyStatusRow(
        actor, status_id, day(created_day), day(modified_day or created_day)
    )


def _tags(parents: dict) -> dict:
    """tags_by_id from tag_id -> parent tag id."""
    return {t: {"tag_id": t, "parent_int_id": p} for t, p in parents.items()}


def _actor():
    return Actor(1)


def test_roll_up_raises_a_parent_with_its_own_status():
    """
    A parent tagged on a block has its own status; once every child is
    Demonstrated, rolling up raises it to Demonstrated instead of skipping it.
    """
    actor = _actor()
    statuses = {
        (1, 1): _status(actor, 20, 2),
        (1, 2): _status(actor, 30, 3, 9),
    }
    EventGenerator.roll_up_statuses(statuses, _tags({1: None, 2: 1}))

    parent = statuses[(1, 1)]
    assert parent.status_id == 30
    assert parent.created == datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC)
    assert parent.modified == datetime.datetime(2023, 1, 9, tzinfo=datetime.UTC)


def test_roll_up_never_lowers_a_parent():
    """A Demonstrated parent stays Demonstrated when a child is only PartiallyAttempted."""
    actor = _actor()
    statuses = {
        (1, 1): _status(actor, 30, 2, 4),
        (1, 2): _status(actor, 20, 1),
    }
    EventGenerator.roll_up_statuses(statuses, _tags({1: None, 2: 1}))

    parent = statuses[(1, 1)]
    assert parent.status_id == 30
    assert parent.modified == datetime.datetime(2023, 1, 4, tzinfo=datetime.UTC)


def test_roll_up_walks_every_level():
    """Grandparents are computed from parents that were themselves rolled up."""
    actor = _actor()
    statuses = {
        (1, 3): _status(actor, 30, 1, 5),
        (1, 4): _status(actor, 30, 2, 6),
    }
    EventGenerator.roll_up_statuses(statuses, _tags({1: None, 2: 1, 3: 2, 4: 2}))

    assert statuses[(1, 2)].status_id == 30
    assert statuses[(1, 1)].status_id == 30
    assert statuses[(1, 1)].modified == datetime.datetime(2023, 1, 6, tzinfo=datetime.UTC)


def test_roll_up_handles_a_root_tag_with_id_zero():
    """A parent id of 0 is still a parent: only None means a root tag."""
    actor = _actor()
    statuses = {(1, 1): _status(actor, 30, 1, 3)}
    EventGenerator.roll_up_statuses(statuses, _tags({0: None, 1: 0}))

    assert statuses[(1, 0)].status_id == 30


def test_fold_in_keeps_first_mastery_and_earliest_creation():
    """A second Demonstrated keeps the first mastery time; created is the earliest."""
    row = _status(_actor(), 30, 3, 5)
    row.fold_in(30, datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
                datetime.datetime(2023, 1, 8, tzinfo=datetime.UTC))

    assert row.created == datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC)
    assert row.modified == datetime.datetime(2023, 1, 5, tzinfo=datetime.UTC)


def test_fold_in_partially_attempted_keeps_modified_at_created():
    """A PartiallyAttempted row was never raised, so modified follows the earliest created."""
    row = _status(_actor(), 20, 5)
    row.fold_in(20, datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC),
                datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC))

    assert row.created == row.modified == datetime.datetime(2023, 1, 2, tzinfo=datetime.UTC)


def test_object_tags_drop_the_fixture_id():
    """Object tags carry object_tag_id, not the source tag's fixture id."""
    gen = _competency_generator()

    for course in gen.courses:
        for object_tag in course.get_object_tags():
            assert "id" not in object_tag
            assert "object_tag_id" in object_tag
