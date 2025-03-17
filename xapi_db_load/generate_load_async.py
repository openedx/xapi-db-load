"""
Generates batches of random xAPI events.
"""

import asyncio
import json
import os
import random
import uuid
from logging import Logger
from random import choice, choices
from typing import Dict, Generator, List

from xapi_db_load.course_configs import Actor, RandomCourse
from xapi_db_load.fixtures.music_tags import MUSIC_TAGS
from xapi_db_load.waiter import Waiter
from xapi_db_load.xapi.xapi_forum import PostCreated
from xapi_db_load.xapi.xapi_grade import CourseGradeCalculated, FirstTimePassed
from xapi_db_load.xapi.xapi_hint_answer import ShowAnswer, ShowHint
from xapi_db_load.xapi.xapi_navigation import (
    LinkClicked,
    NextNavigation,
    PreviousNavigation,
    TabSelectedNavigation,
)
from xapi_db_load.xapi.xapi_problem import BrowserProblemCheck, ServerProblemCheck
from xapi_db_load.xapi.xapi_registration import Registered, Unregistered
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


class EventGenerator(Waiter):
    """
    Generates a batch of random xAPI events based on the EVENT_WEIGHTS proportions.
    """

    actors: List[Actor] = []
    courses: List[RandomCourse] = []
    orgs: List[str] = []
    taxonomies: Dict = {}
    tags: List = []
    setup_complete: bool = False
    task_name: str = "Setup"

    def __init__(
        self, config: Dict, logger: Logger, event_generator: "EventGenerator|None"
    ):
        super().__init__(config, logger, self)
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self._validate_config()

    def _validate_config(self):
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

    async def run_task(self):
        """
        We override run_task here instead of _run_task because this is the setup task
        everyone else is waiting for!
        """
        self.complete_pct = 0.0
        self.setup_orgs()
        self.complete_pct = 0.1
        self.setup_taxonomies_tags()
        self.complete_pct = 0.2
        self.setup_actors()
        self.complete_pct = 0.3
        await self.setup_courses()

        # Force this in case of a rounding error
        self.complete_pct = 1.0
        self.setup_complete = True
        self.finished = True

    async def run_db_load_task(self):
        """
        When we are just loading the database with existing data there is nothing to do.
        """
        self.complete_pct = 1.0
        self.setup_complete = True
        self.finished = True

    def setup_orgs(self):
        """
        Create some random organizations based on the config.
        """
        for i in range(self.config["num_organizations"]):
            self.orgs.append(f"Org{i}")

    async def setup_courses(self):
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
                runs = random.randrange(1, 5)
                course_id = str(uuid.uuid4())[:6]

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
                    )

                    self.courses.append(course)

                    curr_num += 1

                    # Don't let our number of runs overrun the total number
                    # of this type of course
                    if curr_num == num_courses:
                        break

    def setup_actors(self):
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

    def setup_taxonomies_tags(self):
        """
        Load a sample set of tags and format them for use.
        """
        self.taxonomies["Music"] = list(MUSIC_TAGS)

        # tag_hierarchy holds all of the known tags and their parents. This
        # works because the incoming CSV is sorted in a parent-first way. So
        # it should be guaranteed that all parents already exist when we get to
        # the child.
        tag_hierarchy = {}
        taxonomy_id = 0
        for taxonomy in self.taxonomies:  # pylint: disable=consider-using-dict-items
            taxonomy_id += 1
            tag_id = 0
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
        return self.config["batch_size"] * self.config["num_xapi_batches"]

    def get_batch_events_iter(self) -> Generator[str, None, None]:
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
        return sum([len(course.actors) for course in self.courses])

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
