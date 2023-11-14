"""
Generates batches of random xAPI events.
"""
import datetime
import pprint
import uuid
from random import choice, choices

from xapi_db_load.utils import LogTimer, setup_timing

from .course_configs import RandomCourse
from .xapi.xapi_grade import CourseGradeCalculated, FirstTimePassed
from .xapi.xapi_hint_answer import ShowAnswer, ShowHint
from .xapi.xapi_navigation import LinkClicked, NextNavigation, PreviousNavigation, TabSelectedNavigation
from .xapi.xapi_problem import BrowserProblemCheck, ServerProblemCheck
from .xapi.xapi_registration import Registered, Unregistered
from .xapi.xapi_video import (
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
    (Registered, 1.138),
    (Unregistered, 0.146),
    (CompletedVideo, 5.124),
    (LoadedVideo, 7.125),
    (PlayedVideo, 24.519),
    (PausedVideo, 14.912),
    (StoppedVideo, 3.671),
    (PositionChangedVideo, 12.105),
    (BrowserProblemCheck, 8.226),
    (ServerProblemCheck, 8.593),
    (NextNavigation, 6.05),
    (PreviousNavigation, 0.811),
    (TabSelectedNavigation, 0.001),
    (LinkClicked, 0.001),
    (FirstTimePassed, 0.031),
    (ShowHint, 0.076),
    (ShowAnswer, 1.373),
    (TranscriptEnabled, 0.05),
    (TranscriptDisabled, 0.05),
    (CourseGradeCalculated, 1.5),
)

EVENTS = [i[0] for i in EVENT_LOAD]
EVENT_WEIGHTS = [i[1] for i in EVENT_LOAD]


def _get_uuid():
    return str(uuid.uuid4())


class EventGenerator:
    """
    Generates a batch of random xAPI events based on the EVENT_WEIGHTS proportions.
    """

    known_actors = []
    known_courses = []
    known_orgs = []
    course_config_names = []
    course_config_weights = []

    def __init__(self, config):
        self.config = config
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self._validate_config()
        self.setup_course_config_weights()
        self.setup_orgs()
        self.setup_actors()
        self.setup_courses()

    def _validate_config(self):
        if self.start_date >= self.end_date:
            raise ValueError("Start date must be before end date.")

        if (self.end_date - self.start_date).days < self.config["course_length_days"]:
            raise ValueError("The time between start and end dates must be longer than course_length_days.")

        course_size_makeup_keys = set(self.config["course_size_makeup"].keys())
        course_size_pct_keys = set(self.config["course_size_pct"].keys())

        if course_size_makeup_keys != course_size_pct_keys:
            raise ValueError("course_size_makeup and course_size_pct must contain the same keys.")

        for s in self.config["course_size_makeup"]:
            if self.config["course_size_makeup"][s]["actors"] > self.config["num_actors"]:
                raise ValueError(f"Course size {s} wants more actors than are configured in num_actors.")

    def setup_course_config_weights(self):
        for course_config_name, course_config_weight in self.config["course_size_pct"].items():
            self.course_config_names.append(course_config_name)
            self.course_config_weights.append(course_config_weight)

    def setup_orgs(self):
        for i in range(self.config["num_organizations"]):
            self.known_orgs.append(f"Org{i}")

    def setup_courses(self):
        for i in range(self.config["num_courses"]):
            course_config_name = self.get_weighted_course_config()
            course_config_makeup = self.config["course_size_makeup"][course_config_name]
            org = choice(self.known_orgs)
            actors = choices(self.known_actors, k=course_config_makeup["actors"])

            self.known_courses.append(RandomCourse(
                org,
                self.start_date,
                self.end_date,
                self.config["course_length_days"],
                actors,
                course_config_name,
                course_config_makeup
            ))

    def setup_actors(self):
        """
        Create all known actors. Random samplings of these will be passed
        into courses.
        """
        for i in range(self.config["num_actors"]):
            self.known_actors.append(_get_uuid())

    def get_weighted_course_config(self):
        return choices(self.course_config_names, self.course_config_weights)[0]

    def get_batch_events(self):
        """
        Create a batch size list of random events.

        Events are from our EVENTS list, based on the EVENT_WEIGHTS proportions.
        """
        events = choices(EVENTS, EVENT_WEIGHTS, k=self.config["batch_size"])
        return [e(self).get_data() for e in events]

    def get_enrollment_events(self):
        """
        Generate enrollment events for all actors.
        """
        enrollments = []
        for course in self.known_courses:
            for actor in course.known_actors:
                enrollments.append(Registered(self).get_data(course, actor))
        return enrollments

    def get_course(self):
        return choice(self.known_courses)

    def get_org(self):
        return choice(self.known_orgs)

    def dump_courses(self):
        """
        Prettyprint all known courses.
        """
        for c in self.known_courses:
            pprint.pprint(c)


def generate_events(config, backend):
    """
    Generate the actual events in the backend, using the given config.
    """
    setup_timing(config["log_dir"])

    print("Checking table existence and current row count in backend...")
    backend.print_row_counts()
    start = datetime.datetime.utcnow()

    with LogTimer("setup", "full_setup"):
        with LogTimer("setup", "event_generator"):
            event_generator = EventGenerator(config)
            event_generator.dump_courses()

    insert_registrations(event_generator, backend)
    insert_batches(event_generator, config["num_batches"], backend)

    print("Inserting course metadata...")
    with LogTimer("insert_metadata", "course"):
        backend.insert_event_sink_course_data(event_generator.known_courses)
    print("Inserting block metadata...")
    with LogTimer("insert_metadata", "blocks"):
        backend.insert_event_sink_block_data(event_generator.known_courses)

    with LogTimer("batches", "total"):
        print(f"Done! Added {config['num_batches'] * config['batch_size']:,} rows!")

    end = datetime.datetime.utcnow()
    print("Batch insert time: " + str(end - start))

    backend.print_db_time()
    backend.print_row_counts()

    if config["run_distribution_queries"]:
        backend.do_distributions()

    end = datetime.datetime.utcnow()
    print("Total run time: " + str(end - start))


def insert_registrations(event_generator, lake):
    """
    Insert all the registration events
    """
    with LogTimer("enrollment", "get_enrollment_events"):
        events = event_generator.get_enrollment_events()

    with LogTimer("enrollment", "insert_events"):
        lake.batch_insert(events)

    print(f"{len(events)} enrollment events inserted.")


def insert_batches(event_generator, num_batches, lake):
    """
    Generate and insert num_batches of events.
    """
    for x in range(num_batches):
        if x % 100 == 0:
            print(f"{x} of {num_batches}")
            lake.print_db_time()

        with LogTimer("batch", "get_events"):
            events = event_generator.get_batch_events()

        with LogTimer("batch", "insert_events"):
            lake.batch_insert(events)

        if x % 1000 == 0:
            with LogTimer("batch", "all_queries"):
                lake.do_queries(event_generator)
            lake.print_db_time()
            lake.print_row_counts()
