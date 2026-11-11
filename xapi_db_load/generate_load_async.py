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
from logging import Logger
from random import (
    choice,
    choices,
)
from typing import (
    Dict,
    Generator,
    List,
)

from xapi_db_load.constants import (
    COMPETENCY_EXPORT_ID_PREFIX,
    COMPETENCY_FIRST_STATUS_MEDIAN_HOURS,
    COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE,
    COMPETENCY_MASTERY_SIGMA,
    COMPETENCY_STATUS_WEIGHTS,
    COURSE_ID_SHORT_LENGTH,
    DEFAULT_FIRST_CATALOG_PATHWAY_ID,
    DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID,
    DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID,
    DEFAULT_FIRST_OBJECT_TAG_ID,
    DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID,
    DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
    DEFAULT_FIRST_TAG_ID,
    DEFAULT_FIRST_TAXONOMY_ID,
    DEFAULT_LMS_URL,
    DEFAULT_NUM_CATALOG_PATHWAYS,
    DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY,
    DEFAULT_ORG_PREFIX,
    DEFAULT_TAXONOMY_EXPORT_ID,
    MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED,
    MASTERY_STATUS_DEMONSTRATED,
    MASTERY_STATUS_PARTIALLY_ATTEMPTED,
    MAX_COURSE_RUNS,
    MIN_COURSE_RUNS,
    PATHWAY_CATEGORIES,
    PATHWAY_UNENROLLED_RATIO,
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
        self.student_competency_criteria_group_statuses: List[Dict] = []
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
        """
        next_id = self.config.get("first_object_tag_id", DEFAULT_FIRST_OBJECT_TAG_ID)
        export_id = self.config.get("taxonomy_export_id", DEFAULT_TAXONOMY_EXPORT_ID)
        for course in self.courses:
            for object_tag in course.serialize_object_tag_data_for_event_sink():
                object_tag["object_tag_id"] = next_id
                object_tag["export_id"] = export_id
                next_id += 1

    def is_competency_load(self) -> bool:
        """
        Return whether the fake taxonomy is a competency taxonomy, going by the
        export id prefix Aspects uses to recognize one.
        """
        return self.config.get(
            "taxonomy_export_id", DEFAULT_TAXONOMY_EXPORT_ID
        ).startswith(COMPETENCY_EXPORT_ID_PREFIX)

    def setup_student_competency_statuses(self) -> None:
        """
        Give learners a mastery status on the competencies their courses assess.

        Only for a competency load (see ``is_competency_load``). A course assesses
        every competency tagged on one of its blocks. Each enrolled learner gets,
        per competency, no status, PartiallyAttempted or Demonstrated, weighted by
        ``COMPETENCY_STATUS_WEIGHTS``. The first status lands soon after the
        learner enrolled (``COMPETENCY_FIRST_STATUS_MEDIAN_HOURS``). Each
        competency has its own difficulty (``competency_mastery_median_days``,
        drawn from ``COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE``), and a Demonstrated
        row's ``modified`` comes a log-normal time around it later, so some
        competencies are mastered in hours and others in weeks.

        Rows follow openedx-core's rules for ``StudentCompetencyStatus``:

        - one row per learner and competency: across courses the higher status
          wins, ``created`` is the earliest, and ``modified`` of a Demonstrated
          row is when it was first demonstrated
        - only PartiallyAttempted (20) and Demonstrated (30)
        - parent competencies roll up: Demonstrated once every child competency
          the learner was assessed on is, PartiallyAttempted once any has a status

        Each course outcome is also a course-level criteria group status, which
        can be AttemptedNotDemonstrated (10): see
        ``_add_course_group_statuses``. The competency-level row records 10 as
        PartiallyAttempted, as core's rollup does.

        Rows match the ``event_sink.student_competency_status`` table written by
        the platform-plugin-aspects ``student_competency_status`` sink.
        """
        if not self.is_competency_load():
            return

        _, end = self._run_window()
        tags_by_id = {t["tag_id"]: t for t in self.tags}
        outcomes = (
            None,
            MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED[0],
            MASTERY_STATUS_PARTIALLY_ATTEMPTED[0],
            MASTERY_STATUS_DEMONSTRATED[0],
        )
        low, high = COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE
        self.competency_mastery_median_days = {
            tag_id: math.exp(random.uniform(math.log(low), math.log(high)))
            for tag_id in tags_by_id
        }
        first_status_rate = math.log(2) / (COMPETENCY_FIRST_STATUS_MEDIAN_HOURS * 3600)

        # (user_id, tag_id) -> [actor, status_id, created, modified]
        statuses: Dict[tuple, list] = {}
        # (course, tag_id, actor, status_id, created, modified) per course outcome
        course_outcomes: List[tuple] = []
        for course in self.courses:
            assessed = {o["tag_id"] for o in course.serialize_object_tag_data_for_event_sink()}
            course_end = min(_as_utc_datetime(course.end_date), end)
            learners = {e.actor.user_id: e for e in course.actors}
            for enrolled in learners.values():
                enrolled_at = _as_utc_datetime(enrolled.enroll_datetime)
                for tag_id in assessed:
                    outcome = random.choices(outcomes, COMPETENCY_STATUS_WEIGHTS)[0]
                    if outcome is None:
                        continue
                    created = min(
                        enrolled_at
                        + datetime.timedelta(seconds=random.expovariate(first_status_rate)),
                        max(course_end, enrolled_at),
                    )
                    status_id = min(outcome, MASTERY_STATUS_PARTIALLY_ATTEMPTED[0])
                    modified = created
                    if outcome == MASTERY_STATUS_DEMONSTRATED[0]:
                        mastered = created + datetime.timedelta(
                            days=random.lognormvariate(0, COMPETENCY_MASTERY_SIGMA)
                            * self.competency_mastery_median_days[tag_id]
                        )
                        if mastered <= end:
                            status_id = MASTERY_STATUS_DEMONSTRATED[0]
                            modified = mastered
                        else:
                            status_id = MASTERY_STATUS_PARTIALLY_ATTEMPTED[0]
                    course_outcomes.append(
                        (course, tag_id, enrolled.actor, status_id, created, modified)
                    )
                    self._merge_status(
                        statuses,
                        enrolled.actor,
                        tag_id,
                        max(status_id, MASTERY_STATUS_PARTIALLY_ATTEMPTED[0]),
                        created,
                        modified,
                    )

        self._roll_up_statuses(statuses, tags_by_id)

        next_id = self.config.get(
            "first_student_competency_status_id",
            DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID,
        )
        names = dict((MASTERY_STATUS_PARTIALLY_ATTEMPTED, MASTERY_STATUS_DEMONSTRATED))
        for (_, tag_id), (actor, status_id, created, modified) in sorted(
            statuses.items(), key=lambda item: item[1][2]
        ):
            self.student_competency_statuses.append(
                {
                    "id": next_id,
                    "actor_id": actor.id,
                    "tag_id": tag_id,
                    "taxonomy_id": tags_by_id[tag_id]["taxonomy_id"],
                    "status_id": status_id,
                    "status": names[status_id],
                    "created": to_sink_datetime(created),
                    "modified": to_sink_datetime(modified),
                }
            )
            next_id += 1

        self._add_course_group_statuses(course_outcomes, tags_by_id)

    def _add_course_group_statuses(
        self, course_outcomes: List[tuple], tags_by_id: Dict[int, Dict]
    ) -> None:
        """
        Write one course-level criteria group status per learner, competency and
        course they have a status in: AttemptedNotDemonstrated (10),
        PartiallyAttempted (20) or Demonstrated (30).

        The fake criteria groups exist only as ids: one root group per
        competency and one course group per competency and course run under it,
        counting up from ``first_competency_criteria_group_id``. Only the
        course-level rows are written; core also writes root and leaf group rows.

        Rows match the ``event_sink.student_competency_criteria_group_status``
        table written by the platform-plugin-aspects
        ``student_competency_criteria_group_status`` sink.
        """
        next_group_id = self.config.get(
            "first_competency_criteria_group_id",
            DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID,
        )
        root_groups: Dict[int, int] = {}
        course_groups: Dict[tuple, int] = {}
        names = dict(
            (
                MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED,
                MASTERY_STATUS_PARTIALLY_ATTEMPTED,
                MASTERY_STATUS_DEMONSTRATED,
            )
        )
        next_id = self.config.get(
            "first_competency_group_status_id", DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID
        )
        for course, tag_id, actor, status_id, created, modified in sorted(
            course_outcomes, key=lambda o: o[4]
        ):
            if tag_id not in root_groups:
                root_groups[tag_id] = next_group_id
                next_group_id += 1
            if (tag_id, course.course_id) not in course_groups:
                course_groups[(tag_id, course.course_id)] = next_group_id
                next_group_id += 1
            self.student_competency_criteria_group_statuses.append(
                {
                    "id": next_id,
                    "actor_id": actor.id,
                    "group_id": course_groups[(tag_id, course.course_id)],
                    "parent_group_id": root_groups[tag_id],
                    "tag_id": tag_id,
                    "taxonomy_id": tags_by_id[tag_id]["taxonomy_id"],
                    "course_key": course.course_id,
                    "group_level": "course",
                    "status_id": status_id,
                    "status": names[status_id],
                    "created": to_sink_datetime(created),
                    "modified": to_sink_datetime(modified),
                }
            )
            next_id += 1

    @staticmethod
    def _merge_status(  # pylint: disable=too-many-positional-arguments
        statuses: Dict[tuple, list],
        actor: Actor,
        tag_id: int,
        status_id: int,
        created: datetime.datetime,
        modified: datetime.datetime,
    ) -> None:
        """
        Fold one course's outcome into the learner's single status row, the way
        openedx-core's raise-only writes would: the higher status wins, created
        is the earliest, and a Demonstrated row keeps its first mastery time.
        """
        key = (actor.user_id, tag_id)
        if key not in statuses:
            statuses[key] = [actor, status_id, created, modified]
            return
        row = statuses[key]
        if status_id > row[1]:
            row[1], row[3] = status_id, modified
        elif status_id == row[1] == MASTERY_STATUS_DEMONSTRATED[0]:
            row[3] = min(row[3], modified)
        row[2] = min(row[2], created)
        if row[1] != MASTERY_STATUS_DEMONSTRATED[0]:
            # A PartiallyAttempted row was never raised, so modified is created.
            row[3] = row[2]

    @staticmethod
    def _roll_up_statuses(statuses: Dict[tuple, list], tags_by_id: Dict[int, Dict]) -> None:
        """
        Add each learner's parent competencies, deepest first: Demonstrated once
        every child the learner has a status on is (mastered when the last one
        was), otherwise PartiallyAttempted from the first child status.
        """
        def depth(tag_id: int) -> int:
            parent = tags_by_id[tag_id]["parent_int_id"]
            return 0 if not parent else 1 + depth(parent)

        by_actor: Dict[int, Dict[int, list]] = {}
        for (user_id, tag_id), row in statuses.items():
            by_actor.setdefault(user_id, {})[tag_id] = row

        for user_id, rows in by_actor.items():
            pending = sorted(rows, key=depth, reverse=True)
            while pending:
                parent = tags_by_id[pending.pop(0)]["parent_int_id"]
                if not parent or parent in rows:
                    continue
                children = [
                    rows[t]
                    for t in rows
                    if tags_by_id[t]["parent_int_id"] == parent
                ]
                actor = children[0][0]
                created = min(c[2] for c in children)
                if all(c[1] == MASTERY_STATUS_DEMONSTRATED[0] for c in children):
                    row = [actor, MASTERY_STATUS_DEMONSTRATED[0], created, max(c[3] for c in children)]
                else:
                    row = [actor, MASTERY_STATUS_PARTIALLY_ATTEMPTED[0], created, created]
                rows[parent] = row
                statuses[(user_id, parent)] = row
                pending.append(parent)
                pending.sort(key=depth, reverse=True)

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
