"""
Generates a semi-realistic set of random xAPI statements.
"""

import datetime
import pprint
import uuid
from random import choice, choices, randrange

from .course_configs import CourseConfigLarge, CourseConfigMedium, CourseConfigSmall, CourseConfigWhopper
from .xapi.xapi_grade import FirstTimePassed
from .xapi.xapi_hint_answer import ShowAnswer, ShowHint
from .xapi.xapi_navigation import LinkClicked, NextNavigation, PreviousNavigation, TabSelectedNavigation
from .xapi.xapi_problem import BrowserProblemCheck, ServerProblemCheck
from .xapi.xapi_registration import Registered, Unregistered
from .xapi.xapi_video import LoadedVideo, PausedVideo, PlayedVideo, PositionChangedVideo, StoppedVideo

# This is the list of event types to generate, and the proportion of total xapi events
# that should be generated for each. Should total roughly 100 to keep percentages simple.
# The proportions are taken from the relative frequency of each type of event seen on edx.org
# over 30 days in 2022.
EVENT_LOAD = (
    (Registered, 1.138),
    (Unregistered, 0.146),
    (LoadedVideo, 7.125),
    (PlayedVideo, 27.519),
    (PausedVideo, 17.038),
    (StoppedVideo, 3.671),
    (PositionChangedVideo, 13.105),
    (BrowserProblemCheck, 8.776),
    (ServerProblemCheck, 8.643),
    (NextNavigation, 6.05),
    (PreviousNavigation, 0.811),
    (TabSelectedNavigation, 0.001),
    (LinkClicked, 0.001),
    (FirstTimePassed, 0.031),
    (ShowHint, 0.076),
    (ShowAnswer, 1.373),
)

EVENTS = [i[0] for i in EVENT_LOAD]
EVENT_WEIGHTS = [i[1] for i in EVENT_LOAD]

# These determine the proportions of each size of course created,
# these proportions are entirely made up.
COURSE_CONFIG_WEIGHTS = (
    (CourseConfigSmall, 10),
    (CourseConfigMedium, 50),
    (CourseConfigLarge, 30),
    (CourseConfigWhopper, 10),
)

COURSE_CONFIGS = [i[0] for i in COURSE_CONFIG_WEIGHTS]
COURSE_CONFIG_WEIGHTS = [i[1] for i in COURSE_CONFIG_WEIGHTS]

BATCH_SIZE = 100


def _get_uuid():
    """
    Return a string representation of a random uuid.
    """
    return str(uuid.uuid4())


def _get_random_thing(
    thing, func_for_new_thing=_get_uuid, one_in_range=1000, max_thing_length=1000000
):
    """
    Return a random item from a cached list or creates a new one from the given function.
    """
    # Creates a new "thing" using func_for_new_thing() based on the following criteria:
    # - If there is nothing in the list to choose from
    # - If we randomly hit the one_in_range (by default there is a 1/1000 chance of
    #   creating a new thing
    # - And only if we have not already reached our maximum count of "thing"s
    if (not thing or randrange(one_in_range) == 5) and len(
        thing
    ) < max_thing_length:
        new_thing = func_for_new_thing()
        thing.append(new_thing)
        return new_thing

    # Otherwise just return a random existing "thing" from the list
    return choice(thing)


class RandomCourse:
    """
    Hold data representing a random course.
    """

    items_in_course = 0
    known_problem_ids = []
    known_video_ids = []
    known_sequential_ids = []
    start_date = None
    end_date = None

    def __init__(self, org):
        """
        Create a fake course from weighted random configuration.
        """
        self.course_uuid = str(uuid.uuid4())
        self.org = org
        self.course_id = f"{org}+DemoX+{self.course_uuid}"
        self.course_url = f"http://localhost:18000/course/course-v1:{self.course_id}"

        self.course_config = choices(COURSE_CONFIGS, COURSE_CONFIG_WEIGHTS)[0]
        self.configure()

    def __repr__(self):
        """
        Return a string representation of the class, like every other repr.
        """
        return f"""{self.course_uuid} ({str(self.course_config)}):
        {self.start_date} - {self.end_date}
        Items: {self.items_in_course}
        Videos: {len(self.known_video_ids)}
        Problems: {len(self.known_problem_ids)}
        Sequences: {len(self.known_sequential_ids)}
        """

    def configure(self):
        """
        Set some basic parameters for this randomized course.
        """
        course_length_days = randrange(90, 365)
        # Course starts at least course_length_days ago
        latest_course_date = datetime.datetime.utcnow() - datetime.timedelta(
            days=course_length_days
        )
        self.start_date = self._random_datetime(end_datetime=latest_course_date)
        self.end_date = self.start_date + datetime.timedelta(days=course_length_days)

        assert self.end_date > self.start_date

        self.items_in_course = randrange(
            self.course_config.items[0], self.course_config.items[1]
        )

        self.known_problem_ids = [
            self._generate_random_problem_id()
            for _ in range(
                randrange(
                    self.course_config.problems[0], self.course_config.problems[1]
                )
            )
        ]

        self.known_video_ids = [
            self._generate_random_video_id()
            for _ in range(
                randrange(self.course_config.videos[0], self.course_config.videos[1])
            )
        ]

        self.known_sequential_ids = [
            self._generate_random_sequential_id()
            for _ in range(
                randrange(
                    self.course_config.sequences[0], self.course_config.sequences[1]
                )
            )
        ]

    def get_random_emission_time(self):
        """
        Get a random event timestamp valid for the run dates of this course.
        """
        return self._random_datetime(
            start_datetime=self.start_date, end_datetime=self.end_date
        )

    @staticmethod
    def _random_datetime(start_datetime=None, end_datetime=None):
        """
        Select a random time between two datetimes.

        Defaults to timestamps between "5 years ago" and "now".
        """
        if not end_datetime:
            end_datetime = datetime.datetime.utcnow()
        if not start_datetime:
            start_datetime = end_datetime - datetime.timedelta(days=365 * 5)

        delta = end_datetime - start_datetime
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start_datetime + datetime.timedelta(seconds=random_second)

    def _generate_random_video_id(self):
        """
        Return a properly formatted LMS video identifier.
        """
        video_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@video+block@{video_uuid}"

    def get_video_id(self):
        """
        Return a video identifier from the pre-created list.
        """
        return choice(self.known_video_ids)

    def _generate_random_problem_id(self):
        """
        Return a properly formatted LMS problem identifier.
        """
        problem_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@problem+block@{problem_uuid}"

    def get_problem_id(self):
        """
        Return a problem identifier from the pre-created list.
        """
        return choice(self.known_problem_ids)

    def _generate_random_sequential_id(self):
        """
        Return a properly formatted sequential block identifier.
        """
        sequential_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@sequential+block@{sequential_uuid}"

    def get_random_sequential_id(self):
        """
        Return a sequential block identifier from the pre-created list.
        """
        return choice(self.known_sequential_ids)

    def get_random_nav_location(self):
        """
        Return a random navigation location based in the size of the course.
        """
        return str(randrange(1, self.items_in_course))


class EventGenerator:
    """
    Generate a batch of events using randomized courses and users.
    """

    known_actor_uuids = []
    known_courses = []
    known_orgs = ["openedX", "burritoX", "tacoX", "chipX", "salsaX", "guacX"]

    def __init__(self, batch_size=BATCH_SIZE):
        """
        Init the EventGenerator.
        """
        self.batch_size = batch_size

    def get_batch_events(self):
        """
        Create and return a weighted batch of events.
        """
        events = choices(EVENTS, EVENT_WEIGHTS, k=self.batch_size)
        return [e(self).get_data() for e in events]

    def get_actor(self):
        """
        Return a random actor from the existing set, or create and return one.
        """
        return _get_random_thing(self.known_actor_uuids)

    def _generate_random_course(self):
        org = choice(self.known_orgs)
        return RandomCourse(org)

    def get_course(self):
        """
        Return a random course from the existing set, or create and return one.
        """
        return _get_random_thing(
            self.known_courses, self._generate_random_course, one_in_range=10000
        )

    def dump_courses(self):
        """
        Print a human readable representation of all known courses in this batch.
        """
        for c in self.known_courses:
            pprint.pprint(c)
