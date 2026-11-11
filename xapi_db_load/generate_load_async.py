"""
Generates batches of random xAPI events.
"""

import asyncio
import datetime
import json
import math
import os
import random
import uuid
from dataclasses import dataclass
from logging import Logger
from random import (
    choice,
    choices,
)
from typing import (
    Dict,
    Generator,
    List,
    NamedTuple,
)

from xapi_db_load.constants import (
    COMPETENCY_COURSE_STATUS_WEIGHTS,
    COMPETENCY_FIRST_STATUS_MEDIAN_HOURS,
    COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE,
    COMPETENCY_MASTERY_SIGMA,
    COURSE_ID_SHORT_LENGTH,
    DEFAULT_FIRST_CATALOG_PATHWAY_ID,
    DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID,
    DEFAULT_FIRST_OBJECT_TAG_ID,
    DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
    DEFAULT_FIRST_TAG_ID,
    DEFAULT_FIRST_TAXONOMY_ID,
    DEFAULT_LMS_URL,
    DEFAULT_NUM_CATALOG_PATHWAYS,
    DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY,
    DEFAULT_ORG_PREFIX,
    DEFAULT_TAXONOMY_EXPORT_ID,
    DEFAULT_TAXONOMY_TYPE,
    MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED,
    MASTERY_STATUS_DEMONSTRATED,
    MASTERY_STATUS_NAMES,
    MASTERY_STATUS_PARTIALLY_ATTEMPTED,
    MAX_COURSE_RUNS,
    MIN_COURSE_RUNS,
    PATHWAY_CATEGORIES,
    PATHWAY_UNENROLLED_RATIO,
    TAXONOMY_TYPE_COMPETENCY,
    TAXONOMY_TYPES,
)
from xapi_db_load.course_configs import (
    Actor,
    RandomCourse,
)
from xapi_db_load.fixtures.music_tags import MUSIC_TAGS
from xapi_db_load.waiter import Waiter
from xapi_db_load.xapi.xapi_forum import PostCreated
from xapi_db_load.xapi.xapi_grade import (
    CourseGradeCalculated,
    FirstTimePassed,
)
from xapi_db_load.xapi.xapi_hint_answer import (
    ShowAnswer,
    ShowHint,
)
from xapi_db_load.xapi.xapi_navigation import (
    LinkClicked,
    NextNavigation,
    PreviousNavigation,
    TabSelectedNavigation,
)
from xapi_db_load.xapi.xapi_problem import (
    BrowserProblemCheck,
    ServerProblemCheck,
)
from xapi_db_load.xapi.xapi_registration import (
    Registered,
    Unregistered,
)
from xapi_db_load.xapi.xapi_video import (
    CompletedVideo,
    LoadedVideo,
    PausedVideo,
    PlayedVideo,
    PositionChangedVideo,
    StoppedVideo,
    TranscriptDisabled,
    TranscriptEnabled,
)

# This is the list of event types to generate, and the proportion of total xapi
# events that should be generated for each. Should total roughly 100 to keep
# percentages simple.
EVENT_LOAD = (
    (CourseGradeCalculated, 20.0),
    (PlayedVideo, 14.019),
    (NextNavigation, 12.467),
    (BrowserProblemCheck, 9.9),
    (ServerProblemCheck, 9.5),
    (PausedVideo, 8.912),
    (LoadedVideo, 7.125),
    (CompletedVideo, 5.124),
    (PositionChangedVideo, 5.105),
    (StoppedVideo, 3.671),
    (ShowAnswer, 1.373),
    (Registered, 1.138),
    (PreviousNavigation, 0.811),
    (PostCreated, 0.5),
    (Unregistered, 0.146),
    (ShowHint, 0.076),
    (TranscriptEnabled, 0.05),
    (TranscriptDisabled, 0.05),
    (FirstTimePassed, 0.031),
    (TabSelectedNavigation, 0.001),
    (LinkClicked, 0.001),
)

EVENTS = [i[0] for i in EVENT_LOAD]
EVENT_WEIGHTS = [i[1] for i in EVENT_LOAD]
FILE_DIR = os.path.dirname(os.path.abspath(__file__))


def _get_uuid() -> str:
    return str(uuid.uuid4())


def _random_datetime(
    start: datetime.datetime, end: datetime.datetime
) -> datetime.datetime:
    """
    Return a random UTC datetime between start and end, with microseconds like
    the timestamps Django stores.
    """
    span = int((end - start).total_seconds() * 1_000_000)
    return start + datetime.timedelta(microseconds=random.randrange(max(span, 1)))


def _as_utc_datetime(value: datetime.date) -> datetime.datetime:
    """
    Return a date or datetime as a UTC datetime; course dates are plain dates
    when the configured start and end dates are.
    """
    if not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.time(), tzinfo=datetime.UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value


def to_sink_datetime(value: datetime.datetime) -> str:
    """
    Format a UTC datetime the way the platform-plugin-aspects model sinks write
    created/modified: DRF's DateTimeField, which is isoformat() with "+00:00"
    turned into "Z", e.g. 2026-10-29T21:18:23.286693Z.
    """
    formatted = value.astimezone(datetime.UTC).isoformat()
    if formatted.endswith("+00:00"):
        formatted = formatted[:-6] + "Z"
    return formatted


class StudentCompetencyCourseStatusRow(NamedTuple):
    """
    How one learner stands on one competency in one course: the course-level
    criteria group status, and when it was first set and last raised.
    """

    course: RandomCourse
    tag_id: int
    actor: Actor
    status_id: int
    created: datetime.datetime
    modified: datetime.datetime


@dataclass
class StudentCompetencyStatusRow:
    """
    A learner's single competency-level status row, raised as course statuses and
    child competencies are folded into it.
    """

    actor: Actor
    status_id: int
    created: datetime.datetime
    modified: datetime.datetime

    def fold_in(
        self,
        status_id: int,
        created: datetime.datetime,
        modified: datetime.datetime,
    ) -> None:
        """
        Fold another status into this row, the way openedx-core's raise-only
        writes would: the higher status wins, ``created`` is the earliest, and a
        Demonstrated row keeps its first mastery time. A PartiallyAttempted row
        was never raised, so its ``modified`` stays equal to its ``created``.
        """
        self.created = min(self.created, created)
        if status_id > self.status_id:
            self.status_id, self.modified = status_id, modified
        elif status_id == self.status_id == MASTERY_STATUS_DEMONSTRATED:
            self.modified = min(self.modified, modified)
        if self.status_id != MASTERY_STATUS_DEMONSTRATED:
            self.modified = self.created


# (actor user_id, tag_id) -> that learner's competency-level status row
StudentCompetencyStatusRows = Dict[tuple[int, int], StudentCompetencyStatusRow]

# Config keys holding ids, each of which must be a positive integer.
POSITIVE_ID_CONFIG_KEYS = (
    "first_taxonomy_id",
    "first_tag_id",
    "first_object_tag_id",
    "first_student_competency_status_id",
    "first_student_competency_course_status_id",
    "first_competency_criteria_group_id",
)


class EventGenerator(Waiter):  # pylint: disable=abstract-method
    """
    Generates a batch of random xAPI events based on the EVENT_WEIGHTS proportions.

    Intentionally partial implementation: overrides run_task/run_db_load_task at the top level
    instead of the _run_task/_run_db_load_task hooks used by other Waiter subclasses.
    """

    setup_complete: bool = False
    task_name: str = "Setup"

    def __init__(
        self, config: Dict, logger: Logger, event_generator: "EventGenerator|None"
    ):
        super().__init__(config, logger, self)
        self.actors: List[Actor] = []
        self.courses: List[RandomCourse] = []
        self.orgs: List[str] = []
        self.catalog_pathways: List[Dict] = []
        self.pathway_enrollments: List[Dict] = []
        self.taxonomies: Dict = {}
        self.taxonomy_ids: Dict[str, int] = {}
        self.tags: List = []
        self.student_competency_statuses: List[Dict] = []
        self.student_competency_course_statuses: List[Dict] = []
        self.competency_mastery_median_days: Dict[int, float] = {}
        self.setup_complete = False
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self._validate_config()

    def _validate_config(self) -> None:
        """
        Make sure the given values make sense.
        """
        if self.start_date >= self.end_date:
            raise ValueError("Start date must be before end date.")

        if (self.end_date - self.start_date).days < self.config["course_length_days"]:
            raise ValueError(
                "The time between start and end dates must be longer than course_length_days."
            )

        for s in self.config["num_course_sizes"]:
            if (
                self.config["course_size_makeup"][s]["actors"]
                > self.config["num_actors"]
            ):
                raise ValueError(
                    f"Course size {s} wants more actors than are configured in num_actors."
                )

        for key in POSITIVE_ID_CONFIG_KEYS:
            value = self.config.get(key, 1)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{key} must be a positive integer, got {value!r}.")

        # These end up inside quoted SQL literals in the CHDB backend.
        prefix = self.config.get("org_prefix", DEFAULT_ORG_PREFIX)
        if not isinstance(prefix, str) or not prefix.isalnum():
            raise ValueError(f"org_prefix must be letters and digits, got {prefix!r}.")
        export_id = self.config.get("taxonomy_export_id", DEFAULT_TAXONOMY_EXPORT_ID)
        if not isinstance(export_id, str) or "'" in export_id or "\\" in export_id:
            raise ValueError(
                f"taxonomy_export_id must be a string without quotes or backslashes, got {export_id!r}."
            )
        if self.taxonomy_type not in TAXONOMY_TYPES:
            raise ValueError(
                f"taxonomy_type must be one of {', '.join(TAXONOMY_TYPES)}, got {self.taxonomy_type!r}."
            )

    async def run_task(self) -> None:
        """
        We override run_task here instead of _run_task because this is the setup task
        everyone else is waiting for!
        """
        self.complete_pct = 0.0
        self.setup_orgs()
        self.setup_catalog_pathways()
        self.complete_pct = 0.1
        self.setup_taxonomies_tags()
        self.complete_pct = 0.2
        self.setup_actors()
        self.complete_pct = 0.3
        await self.setup_courses()
        # Needs the courses, to enroll learners of the pathway's org.
        self.setup_pathway_enrollments()
        # Needs the courses, whose blocks it tags.
        self.setup_object_tags()
        # Needs the object tags, to know which competencies each course assesses.
        self.setup_student_competency_statuses()

        # Force this in case of a rounding error
        self.complete_pct = 1.0
        self.setup_complete = True
        self.finished = True

    async def run_db_load_task(self) -> None:
        """
        When we are just loading the database with existing data there is nothing to do.
        """
        self.complete_pct = 1.0
        self.setup_complete = True
        self.finished = True

    def setup_orgs(self) -> None:
        """
        Create some random organizations based on the config.
        """
        prefix = self.config.get("org_prefix", DEFAULT_ORG_PREFIX)
        for i in range(self.config["num_organizations"]):
            self.orgs.append(f"{prefix}{i}")

    def setup_catalog_pathways(self) -> None:
        """
        Create the Catalog Pathways, rotating through the orgs and categories so
        that each org gets pathways of different categories.

        Rows match the ``event_sink.catalog_pathway`` table written by the
        platform-plugin-aspects ``catalog_pathway`` sink, including the
        created/modified string format (see ``to_sink_datetime``).
        """
        num_pathways = self.config.get(
            "num_catalog_pathways", DEFAULT_NUM_CATALOG_PATHWAYS
        )
        first_id = self.config.get(
            "first_catalog_pathway_id", DEFAULT_FIRST_CATALOG_PATHWAY_ID
        )
        num_orgs = len(self.orgs)
        start, end = self._run_window()

        for i in range(num_pathways):
            # Shift each org's category by one on every full pass over the orgs.
            category_code, category_title = PATHWAY_CATEGORIES[
                (i % num_orgs + i // num_orgs) % len(PATHWAY_CATEGORIES)
            ]
            created = _random_datetime(start, end)
            modified = _random_datetime(
                created, min(created + datetime.timedelta(days=30), end)
            )
            self.catalog_pathways.append(
                {
                    "id": first_id + i,
                    "org": self.orgs[i % num_orgs],
                    "pathway_code": f"Pathway{i}",
                    "title": f"{category_title} {i}",
                    "category_code": category_code,
                    "category_title": category_title,
                    "created": to_sink_datetime(created),
                    "modified": to_sink_datetime(modified),
                }
            )

    def setup_pathway_enrollments(self) -> None:
        """
        Enroll learners in each Catalog Pathway.

        Each pathway gets ``num_enrollments_per_pathway`` distinct learners (at
        most ``num_actors``), taken first from learners of the pathway org's
        courses so that their xAPI activity is in the same org, then from the
        remaining actors. ``actor_id`` is the actor's xAPI id, as written by the
        platform-plugin-aspects ``pathway_enrollment`` sink, so it joins the
        xAPI tables. About ``PATHWAY_UNENROLLED_RATIO`` of the enrollments end
        up unenrolled (``is_active`` False, modified after created); the rest
        keep ``modified`` equal to ``created``.

        Rows match the ``event_sink.pathway_enrollment`` table.
        """
        per_pathway = min(
            self.config.get(
                "num_enrollments_per_pathway", DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY
            ),
            len(self.actors),
        )
        next_id = self.config.get(
            "first_pathway_enrollment_id", DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID
        )
        _, end = self._run_window()

        org_learners: Dict[str, Dict[int, Actor]] = {}
        for course in self.courses:
            learners = org_learners.setdefault(course.org, {})
            for enrolled in course.actors:
                learners[enrolled.actor.user_id] = enrolled.actor

        for pathway in self.catalog_pathways:
            in_org = list(org_learners.get(pathway["org"], {}).values())
            random.shuffle(in_org)
            chosen = in_org[:per_pathway]
            if len(chosen) < per_pathway:
                chosen_ids = {a.user_id for a in chosen}
                others = [a for a in self.actors if a.user_id not in chosen_ids]
                chosen += random.sample(others, per_pathway - len(chosen))

            pathway_created = datetime.datetime.fromisoformat(pathway["created"])
            for actor in chosen:
                created = _random_datetime(pathway_created, end)
                is_active = random.random() >= PATHWAY_UNENROLLED_RATIO
                modified = created if is_active else _random_datetime(created, end)
                self.pathway_enrollments.append(
                    {
                        "id": next_id,
                        "catalog_pathway_id": pathway["id"],
                        "actor_id": actor.id,
                        "created": to_sink_datetime(created),
                        "modified": to_sink_datetime(modified),
                        "is_active": is_active,
                    }
                )
                next_id += 1

    def setup_object_tags(self) -> None:
        """
        Tag each course's blocks once, so every backend writes the same object
        tags and the competency statuses match them.

        Object tag ids count up from ``first_object_tag_id`` across all courses,
        and every row carries ``taxonomy_export_id`` as its taxonomy's export id.
        Both are added in place to the dicts ``RandomCourse.get_object_tags``
        keeps and returns.
        """
        next_id = self.config.get("first_object_tag_id", DEFAULT_FIRST_OBJECT_TAG_ID)
        export_id = self.config.get("taxonomy_export_id", DEFAULT_TAXONOMY_EXPORT_ID)
        for course in self.courses:
            for object_tag in course.get_object_tags():
                object_tag["object_tag_id"] = next_id
                object_tag["export_id"] = export_id
                next_id += 1

    @property
    def taxonomy_type(self) -> str:
        """
        The type written on every fake taxonomy.
        """
        return self.config.get("taxonomy_type", DEFAULT_TAXONOMY_TYPE)

    @property
    def is_competency_load(self) -> bool:
        """
        Whether the fake taxonomies are competency taxonomies.
        """
        return self.taxonomy_type == TAXONOMY_TYPE_COMPETENCY

    def setup_student_competency_statuses(self) -> None:
        """
        Give learners a mastery status on the competencies their courses assess.

        Only for a competency load (see ``is_competency_load``). A course assesses
        every competency tagged on one of its blocks. Each enrolled learner gets,
        per competency and course, one of four course statuses: no status,
        AttemptedNotDemonstrated, PartiallyAttempted or Demonstrated, weighted by
        ``COMPETENCY_COURSE_STATUS_WEIGHTS`` (see ``_time_course_status`` for
        the timing). A Demonstrated draw whose mastery time falls after
        ``end_date`` stays PartiallyAttempted, so the real Demonstrated share is
        below its weight, and lowest for the hardest competencies.

        Competency-level rows follow openedx-core's rules for
        ``StudentCompetencyStatus``:

        - one row per learner and competency: across courses the higher status
          wins, ``created`` is the earliest, and ``modified`` of a Demonstrated
          row is when it was first demonstrated
        - only PartiallyAttempted (20) and Demonstrated (30): AttemptedNotDemonstrated
          is recorded as PartiallyAttempted, as core's rollup does
        - parent competencies roll up (see ``roll_up_statuses``)

        The course statuses are also written as they are, as course-level
        criteria group statuses: see ``_add_student_competency_course_statuses``.

        Rows match the ``event_sink.student_competency_status`` and
        ``event_sink.student_competency_course_status`` tables written by the
        platform-plugin-aspects sinks of the same names.
        """
        if not self.is_competency_load:
            return

        tags_by_id = {t["tag_id"]: t for t in self.tags}
        low, high = COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE
        self.competency_mastery_median_days = {
            tag_id: math.exp(random.uniform(math.log(low), math.log(high)))
            for tag_id in tags_by_id
        }

        course_statuses = self._draw_course_statuses()
        statuses = self._fold_competency_statuses(course_statuses)
        self.roll_up_statuses(statuses, tags_by_id)
        self._add_student_competency_statuses(statuses, tags_by_id)
        self._add_student_competency_course_statuses(course_statuses, tags_by_id)

    def _draw_course_statuses(self) -> List[StudentCompetencyCourseStatusRow]:
        """
        Draw every enrolled learner's course status on every competency their course
        assesses, leaving out the "no status" draws.
        """
        _, run_end = self._run_window()
        drawn_status_ids = (
            None,
            MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED,
            MASTERY_STATUS_PARTIALLY_ATTEMPTED,
            MASTERY_STATUS_DEMONSTRATED,
        )
        course_statuses = []
        for course in self.courses:
            assessed_tag_ids = {o["tag_id"] for o in course.get_object_tags()}
            course_end = min(_as_utc_datetime(course.end_date), run_end)
            # course.actors is drawn with replacement, so a learner can be enrolled
            # twice; keep one enrollment each, or they get two statuses per
            # competency in the same course.
            learners = {e.actor.user_id: e for e in course.actors}
            for enrolled in learners.values():
                enrolled_at = _as_utc_datetime(enrolled.enroll_datetime)
                for tag_id in assessed_tag_ids:
                    drawn_status_id = random.choices(
                        drawn_status_ids, COMPETENCY_COURSE_STATUS_WEIGHTS
                    )[0]
                    if drawn_status_id is None:
                        continue
                    status_id, created, modified = self._time_course_status(
                        drawn_status_id, tag_id, enrolled_at, course_end, run_end
                    )
                    course_statuses.append(
                        StudentCompetencyCourseStatusRow(
                            course, tag_id, enrolled.actor, status_id, created, modified
                        )
                    )
        return course_statuses

    def _time_course_status(
        self,
        drawn_status_id: int,
        tag_id: int,
        enrolled_at: datetime.datetime,
        course_end: datetime.datetime,
        run_end: datetime.datetime,
    ) -> tuple[int, datetime.datetime, datetime.datetime]:
        """
        Return the final status, created and modified times of one drawn status.

        The first status lands an exponential delay after enrollment
        (``COMPETENCY_FIRST_STATUS_MEDIAN_HOURS``), by the course end at the
        latest. A Demonstrated draw is mastered a log-normal time around the
        competency's difficulty later (``competency_mastery_median_days``), and
        stays PartiallyAttempted when that falls after the end of the run.
        """
        # An exponential delay with median m has rate ln(2) / m.
        first_status_rate = math.log(2) / (COMPETENCY_FIRST_STATUS_MEDIAN_HOURS * 3600)
        first_status_delay = datetime.timedelta(
            seconds=random.expovariate(first_status_rate)
        )
        latest_first_status = max(course_end, enrolled_at)
        created = min(enrolled_at + first_status_delay, latest_first_status)

        if drawn_status_id != MASTERY_STATUS_DEMONSTRATED:
            return drawn_status_id, created, created

        mastered_at = created + datetime.timedelta(
            days=random.lognormvariate(0, COMPETENCY_MASTERY_SIGMA)
            * self.competency_mastery_median_days[tag_id]
        )
        if mastered_at > run_end:
            return MASTERY_STATUS_PARTIALLY_ATTEMPTED, created, created
        return MASTERY_STATUS_DEMONSTRATED, created, mastered_at

    @staticmethod
    def _fold_competency_statuses(
        course_statuses: List[StudentCompetencyCourseStatusRow],
    ) -> StudentCompetencyStatusRows:
        """
        Fold the course statuses into one competency-level row per learner and
        competency, recording AttemptedNotDemonstrated as PartiallyAttempted.
        """
        statuses: StudentCompetencyStatusRows = {}
        for o in course_statuses:
            status_id = max(o.status_id, MASTERY_STATUS_PARTIALLY_ATTEMPTED)
            key = (o.actor.user_id, o.tag_id)
            if key in statuses:
                statuses[key].fold_in(status_id, o.created, o.modified)
            else:
                statuses[key] = StudentCompetencyStatusRow(
                    o.actor, status_id, o.created, o.modified
                )
        return statuses

    def _add_student_competency_statuses(
        self, statuses: StudentCompetencyStatusRows, tags_by_id: Dict[int, Dict]
    ) -> None:
        """
        Write the competency-level rows in ``created`` order, with ids counting up
        from ``first_student_competency_status_id``.
        """
        next_id = self.config.get(
            "first_student_competency_status_id",
            DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
        )
        for (_, tag_id), status in sorted(
            statuses.items(), key=lambda item: item[1].created
        ):
            self.student_competency_statuses.append(
                {
                    "id": next_id,
                    "actor_id": status.actor.id,
                    "tag_id": tag_id,
                    "taxonomy_id": tags_by_id[tag_id]["taxonomy_id"],
                    "status_id": status.status_id,
                    "status_name": MASTERY_STATUS_NAMES[status.status_id],
                    "created": to_sink_datetime(status.created),
                    "modified": to_sink_datetime(status.modified),
                }
            )
            next_id += 1

    def _add_student_competency_course_statuses(
        self, course_statuses: List[StudentCompetencyCourseStatusRow], tags_by_id: Dict[int, Dict]
    ) -> None:
        """
        Write one course-level criteria group status per learner, competency and
        course they have a status in: AttemptedNotDemonstrated (10),
        PartiallyAttempted (20) or Demonstrated (30). Ids count up from
        ``first_student_competency_course_status_id``.

        The fake criteria groups exist only as ids: one course group per
        competency and course run, counting up from
        ``first_competency_criteria_group_id``. The sink writes course-level
        groups only, so root and leaf groups have no rows here either.
        """
        next_group_id = self.config.get(
            "first_competency_criteria_group_id",
            DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID,
        )
        group_id_by_tag_and_course: Dict[tuple, int] = {}
        next_id = self.config.get(
            "first_student_competency_course_status_id",
            DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID,
        )
        for o in sorted(course_statuses, key=lambda o: o.created):
            group_key = (o.tag_id, o.course.course_id)
            if group_key not in group_id_by_tag_and_course:
                group_id_by_tag_and_course[group_key] = next_group_id
                next_group_id += 1
            self.student_competency_course_statuses.append(
                {
                    "id": next_id,
                    "actor_id": o.actor.id,
                    "group_id": group_id_by_tag_and_course[group_key],
                    "tag_id": o.tag_id,
                    "taxonomy_id": tags_by_id[o.tag_id]["taxonomy_id"],
                    "course_key": o.course.course_id,
                    "status_id": o.status_id,
                    "status_name": MASTERY_STATUS_NAMES[o.status_id],
                    "created": to_sink_datetime(o.created),
                    "modified": to_sink_datetime(o.modified),
                }
            )
            next_id += 1

    @staticmethod
    def roll_up_statuses(statuses: StudentCompetencyStatusRows, tags_by_id: Dict[int, Dict]) -> None:
        """
        Raise each learner's parent competencies from their children, deepest
        level first, so a parent's children are final before it is computed.

        A parent is Demonstrated once every child the learner has a status on
        is (mastered when the last one was), otherwise PartiallyAttempted from
        the first child status. A parent the learner already has a direct status
        on (it is tagged on a block too) is only ever raised by this, never
        lowered, as with core's raise-only writes.
        """
        depths: Dict[int, int] = {}

        def depth(tag_id: int) -> int:
            if tag_id not in depths:
                parent_tag_id = tags_by_id[tag_id]["parent_int_id"]
                depths[tag_id] = 0 if parent_tag_id is None else 1 + depth(parent_tag_id)
            return depths[tag_id]

        # Each learner's view of ``statuses``, by tag id. Both hold the same row
        # objects, so a raised row is raised in both, but a new parent row has
        # to be added to both.
        statuses_by_user_id: Dict[int, Dict[int, StudentCompetencyStatusRow]] = {}
        for (user_id, tag_id), status in statuses.items():
            statuses_by_user_id.setdefault(user_id, {})[tag_id] = status

        for user_id, user_statuses in statuses_by_user_id.items():
            deepest = max(depth(t) for t in user_statuses)
            for level in range(deepest, 0, -1):
                children_by_parent: Dict[int, List[StudentCompetencyStatusRow]] = {}
                for tag_id, status in user_statuses.items():
                    if depth(tag_id) == level:
                        parent_tag_id = tags_by_id[tag_id]["parent_int_id"]
                        children_by_parent.setdefault(parent_tag_id, []).append(status)
                for parent_tag_id, children in children_by_parent.items():
                    rolled = EventGenerator._roll_up_children(children)
                    if parent_tag_id in user_statuses:
                        user_statuses[parent_tag_id].fold_in(
                            rolled.status_id, rolled.created, rolled.modified
                        )
                    else:
                        user_statuses[parent_tag_id] = rolled
                        statuses[(user_id, parent_tag_id)] = rolled

    @staticmethod
    def _roll_up_children(
        children: List[StudentCompetencyStatusRow],
    ) -> StudentCompetencyStatusRow:
        """
        Return the parent status a learner's child statuses roll up to:
        Demonstrated when every child is (mastered when the last one was),
        otherwise PartiallyAttempted from the first child status.
        """
        created = min(c.created for c in children)
        if all(c.status_id == MASTERY_STATUS_DEMONSTRATED for c in children):
            return StudentCompetencyStatusRow(
                children[0].actor,
                MASTERY_STATUS_DEMONSTRATED,
                created,
                max(c.modified for c in children),
            )
        return StudentCompetencyStatusRow(
            children[0].actor, MASTERY_STATUS_PARTIALLY_ATTEMPTED, created, created
        )

    def _run_window(self) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Return the configured start and end dates as UTC datetimes.
        """
        start = datetime.datetime.combine(
            self.start_date, datetime.time(), tzinfo=datetime.UTC
        )
        end = datetime.datetime.combine(
            self.end_date, datetime.time(), tzinfo=datetime.UTC
        )
        return start, end

    async def setup_courses(self) -> None:
        """
        Pre-create a number of courses based on the config.
        """
        # Bookkeeping for how many courses we have left to create
        ttl_courses = 0
        for _, num_courses in self.config["num_course_sizes"].items():
            ttl_courses += num_courses

        self.logger.info(f"Setting up {ttl_courses} courses")

        remaining_pct = 1.0 - self.complete_pct
        course_pct_value = remaining_pct / ttl_courses

        for course_config_name, num_courses in self.config["num_course_sizes"].items():
            self.logger.info(f"Setting up {num_courses} {course_config_name} courses")

            curr_num = 0
            while curr_num < num_courses:
                self.complete_pct += course_pct_value
                self.logger.debug(
                    f"Creating {course_config_name} courses: {curr_num + 1}/{num_courses}"
                )
                await asyncio.sleep(0.01)
                course_config_makeup = self.config["course_size_makeup"][
                    course_config_name
                ]
                org = choice(self.orgs)
                actors = choices(self.actors, k=course_config_makeup["actors"])
                runs = random.randrange(MIN_COURSE_RUNS, MAX_COURSE_RUNS)
                course_id = str(uuid.uuid4())[:COURSE_ID_SHORT_LENGTH]

                # Create 1-5 of the same course size / makeup / name
                # but different course runs.
                for run_id in range(runs):
                    course = await RandomCourse().populate(
                        org,
                        course_id,
                        run_id,
                        self.start_date,
                        self.end_date,
                        self.config["course_length_days"],
                        actors,
                        course_config_name,
                        course_config_makeup,
                        self.tags,
                        lms_url=self.config.get("lms_url", DEFAULT_LMS_URL),
                    )

                    self.courses.append(course)

                    curr_num += 1

                    # Don't let our number of runs overrun the total number
                    # of this type of course
                    if curr_num == num_courses:
                        break

    def setup_actors(self) -> None:
        """
        Create all known actors.

        Random samplings of these will be passed into courses.
        """
        self.actors = [Actor(i) for i in range(self.config["num_actors"])]

    @staticmethod
    def _get_hierarchy(tag_hierarchy, start_parent_id):
        """
        Return a list of all the parent values of the given parent_id.

        tag_hierarchy is a tuple of ("Tag name", "parent_id")
        """
        if not start_parent_id or start_parent_id not in tag_hierarchy:
            return []

        hierarchy = []
        parent_id = start_parent_id
        while parent_id:
            hierarchy.append(tag_hierarchy[parent_id][0])
            parent_id = tag_hierarchy[parent_id][1]

        # Reverse the list to get the highest parent first, which is how Studio
        # sends it
        hierarchy.reverse()
        return hierarchy

    def setup_taxonomies_tags(self) -> None:
        """
        Load a sample set of tags and format them for use.
        """
        self.taxonomies["Music"] = list(MUSIC_TAGS)

        # tag_hierarchy holds all of the known tags and their parents. This
        # works because the incoming CSV is sorted in a parent-first way. So
        # it should be guaranteed that all parents already exist when we get to
        # the child.
        tag_hierarchy = {}
        taxonomy_id = self.config.get("first_taxonomy_id", DEFAULT_FIRST_TAXONOMY_ID) - 1
        # Tag ids are unique across taxonomies, as in the LMS.
        tag_id = self.config.get("first_tag_id", DEFAULT_FIRST_TAG_ID) - 1
        for taxonomy in self.taxonomies:  # pylint: disable=consider-using-dict-items
            taxonomy_id += 1
            self.taxonomy_ids[taxonomy] = taxonomy_id
            for tag in self.taxonomies[taxonomy]:
                tag_id += 1
                tag["tag_id"] = tag_id
                tag["taxonomy_id"] = taxonomy_id
                tag["parent_int_id"] = (
                    tag_hierarchy[tag["parent_id"]][2]
                    if tag["parent_id"] in tag_hierarchy
                    else None
                )
                tag["hierarchy"] = json.dumps(
                    self._get_hierarchy(tag_hierarchy, tag["parent_id"])
                )

                tag_hierarchy[tag["id"]] = (
                    tag["value"],
                    tag["parent_id"],
                    tag["tag_id"],
                )
                self.tags.append(tag)

    def get_random_event_count(self) -> int:
        """Return the total number of random xAPI events that will be generated."""
        return self.config["batch_size"] * self.config["num_xapi_batches"]

    def get_batch_events_iter(self) -> Generator[str, None, None]:
        """Yield batch events pre-formatted as SQL VALUES tuples."""
        for v in self.get_batch_events():
            yield f"('{v['event_id']}', '{v['emission_time']}', '{v['event']}')"

    def get_batch_events(self) -> Generator[Dict, None, None]:
        """
        Create a batch size list of random events.

        Events are from our EVENTS list, based on the EVENT_WEIGHTS proportions.
        """

        events = choices(EVENTS, EVENT_WEIGHTS, k=self.config["batch_size"])
        for event in events:
            yield event(self).get_data()

    def get_random_event(self) -> Dict:
        """
        Generate one random event.

        Events are from our EVENTS list, based on the EVENT_WEIGHTS proportions.
        """
        event = choices(EVENTS, EVENT_WEIGHTS)[0]
        return event(self).get_data()

    def get_enrollment_event_count(self) -> int:
        """
        Return the number of enrollment events we should generate.
        """
        return sum(len(course.actors) for course in self.courses)

    def get_enrollment_events(self) -> Generator[Dict, None, None]:
        """
        Generate enrollment events for all actors.
        """
        for course in self.courses:
            for actor in course.actors:
                yield Registered(self).get_data(course, actor)

    def get_course(self) -> RandomCourse:
        """
        Return a random course from our pre-built list.
        """
        return choice(self.courses)

    def get_org(self) -> str:
        """
        Return a random org from our pre-built list.
        """
        return choice(self.orgs)
