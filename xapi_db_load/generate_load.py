"""
Generates batches of random xAPI events.
"""
import datetime
import pprint
import uuid
from random import choice, choices, randrange

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

BATCH_SIZE = 100


def _get_uuid():
    return str(uuid.uuid4())


def _get_random_thing(
    thing,
    func_for_new_thing=_get_uuid,
    one_in_range=1000,
    max_thing_length=1000000
):
    """
    Return a random instantiated object of the type requested.

    A new object will be created approximately one out of every "one_in_range"
    calls to this function. Otherwise, an existing object will be returned.
    """
    if (not thing or randrange(one_in_range) == 5) \
            and len(thing) < max_thing_length:
        new_thing = func_for_new_thing()
        thing.append(new_thing)
        return new_thing

    return choice(thing)


class EventGenerator:
    """
    Generates a batch of random xAPI events based on the EVENT_WEIGHTS proportions.
    """

    known_actor_uuids = []
    known_courses = []
    known_orgs = ["openedX", "burritoX", "tacoX", "chipX", "salsaX", "guacX"]

    def __init__(self, batch_size, start_date, end_date):
        self.batch_size = batch_size
        self.start_date = start_date
        self.end_date = end_date

    def get_batch_events(self):
        """
        Create a batch size list of random events.

        Events are from our EVENTS list, based on the EVENT_WEIGHTS proportions.
        """
        events = choices(EVENTS, EVENT_WEIGHTS, k=self.batch_size)
        return [e(self).get_data() for e in events]

    def get_actor(self):
        """
        Return a random actor.
        """
        return _get_random_thing(self.known_actor_uuids)

    def _generate_random_course(self):
        org = choice(self.known_orgs)
        return RandomCourse(org, self.start_date, self.end_date)

    def get_course(self):
        """
        Return a random course.
        """
        return _get_random_thing(
            self.known_courses, self._generate_random_course, one_in_range=10000
        )

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

    if config["start_date"] >= config["end_date"]:
        raise ValueError("Start date must be before end date.")

    start = datetime.datetime.utcnow()

    with LogTimer("setup", "full_setup"):
        if config["drop_tables_first"]:
            with LogTimer("setup", "drop_tables"):
                backend.drop_tables()

        with LogTimer("setup", "create_tables"):
            backend.create_tables()

        with LogTimer("setup", "event_generator"):
            event_generator = EventGenerator(
                batch_size=config.get("batch_size", BATCH_SIZE),
                start_date=config["start_date"],
                end_date=config["end_date"]
            )

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
    backend.do_distributions()

    end = datetime.datetime.utcnow()
    print("Total run time: " + str(end - start))


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
