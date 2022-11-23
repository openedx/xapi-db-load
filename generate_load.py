import uuid
from random import choice, choices, randrange

from xapi.xapi_hint_answer import ShowAnswer, ShowHint
from xapi.xapi_grade import FirstTimePassed
from xapi.xapi_navigation import NextNavigation, PreviousNavigation, TabSelectedNavigation, LinkClicked
from xapi.xapi_registration import Registered, Unregistered
from xapi.xapi_problem import BrowserProblemCheck, ServerProblemCheck
from xapi.xapi_video import (
    LoadedVideo,
    PlayedVideo,
    PausedVideo,
    StoppedVideo,
    PositionChangedVideo,
)

# This is the list of event types to generate, and the proportion of total xapi events
# that should be generated for each. Should total roughly 100 to keep percentages simple.
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
WEIGHTS = [i[1] for i in EVENT_LOAD]

BATCH_SIZE = 100
MAX_THING_LENGTH = 1000000


def _get_uuid():
    return str(uuid.uuid4())


def _get_random_thing(thing, func_for_new_thing=_get_uuid, one_in_range=1000):
    if (not len(thing) or randrange(one_in_range) == 5) and len(thing) < MAX_THING_LENGTH:
        new_thing = func_for_new_thing()
        thing.append(new_thing)
        return new_thing

    return choice(thing)


class RandomCourse:
    known_problem_ids = []
    known_video_ids = []
    known_sequential_ids = []
    known_course_points = []

    def __init__(self, org, course_id, course_url, items_in_course):
        self.org = org
        self.course_id = course_id
        self.course_url = course_url
        self.items_in_course = items_in_course

    def _generate_random_video_id(self):
        video_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@video+block@{video_uuid}"

    def get_video_id(self):
        return _get_random_thing(self.known_video_ids, self._generate_random_video_id, one_in_range=100)

    def _generate_random_problem_id(self):
        problem_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@problem+block@{problem_uuid}"

    def get_problem_id(self):
        return _get_random_thing(self.known_problem_ids, self._generate_random_problem_id, one_in_range=1000000)

    def _generate_random_sequential_id(self):
        sequential_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@sequential+block@{sequential_uuid}"

    def get_random_sequential_id(self):
        return _get_random_thing(self.known_sequential_ids, self._generate_random_sequential_id, one_in_range=100)

    def get_random_nav_location(self):
        return str(randrange(1, self.items_in_course))


class EventGenerator:
    known_actor_uuids = []
    known_courses = []
    known_orgs = ['openedX', 'burritoX', 'tacoX', 'chipX', 'salsaX', 'guacX']

    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size

    def get_batch_events(self):
        events = choices(EVENTS, WEIGHTS, k=self.batch_size)
        # print(events)

        return [e(self).get_data() for e in events]

    def get_actor(self):
        return _get_random_thing(self.known_actor_uuids)

    def _generate_random_course(self):
        org = choice(self.known_orgs)
        course_uuid = str(uuid.uuid4())
        course_id = f"{org}+DemoX+{course_uuid}"
        course_url = f"http://localhost:18000/course/course-v1:{course_id}"
        items_in_course = randrange(10, 100)

        return RandomCourse(org, course_id, course_url, items_in_course)

    def get_course(self):
        return _get_random_thing(self.known_courses, self._generate_random_course, one_in_range=10000)
