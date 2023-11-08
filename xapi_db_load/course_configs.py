"""
Configuration values for emulating courses of various sizes.
"""
import datetime
import uuid
from random import choice, choices, randrange


class CourseConfigSmall:
    items = (10, 20)
    problems = (10, 20)
    videos = (5, 10)
    sequences = (5, 10)


class CourseConfigMedium:
    items = (20, 40)
    problems = (20, 40)
    videos = (10, 20)
    sequences = (10, 20)


class CourseConfigLarge:
    items = (40, 80)
    problems = (40, 80)
    videos = (10, 30)
    sequences = (20, 40)


class CourseConfigWhopper:
    items = (80, 200)
    problems = (80, 160)
    videos = (10, 40)
    sequences = (40, 80)


# These determine the proportions of each size of course created
COURSE_CONFIG_WEIGHTS = (
    (CourseConfigSmall, 10),
    (CourseConfigMedium, 50),
    (CourseConfigLarge, 30),
    (CourseConfigWhopper, 10),
)

COURSE_CONFIGS = [i[0] for i in COURSE_CONFIG_WEIGHTS]
COURSE_CONFIG_WEIGHTS = [i[1] for i in COURSE_CONFIG_WEIGHTS]


class RandomCourse:
    """
    Holds "known objects" and configuration values for a fake course.
    """

    items_in_course = 0
    known_problem_ids = []
    known_video_ids = []
    known_sequential_ids = []
    start_date = None
    end_date = None

    def __init__(self, org, start_date, end_date):
        self.course_uuid = str(uuid.uuid4())
        self.org = org
        self.course_id = f"course-v1:{org}+DemoX+{self.course_uuid}"
        self.course_url = f"http://localhost:18000/course/{self.course_id}"

        self.start_date = start_date
        self.end_date = end_date
        self.course_config = choices(COURSE_CONFIGS, COURSE_CONFIG_WEIGHTS)[0]
        self.configure()

    def __repr__(self):
        return f"""{self.course_uuid} ({str(self.course_config)}):
        {self.start_date} - {self.end_date}
        Items: {self.items_in_course}
        Videos: {len(self.known_video_ids)}
        Problems: {len(self.known_problem_ids)}
        Sequences: {len(self.known_sequential_ids)}
        """

    def configure(self):
        """
        Set up the fake course configuration such as course length, start and end dates, and size.
        """
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
        Randomizes an emission time for events that falls within the course start and end dates.
        """
        return self._random_datetime(
            start_datetime=self.start_date, end_datetime=self.end_date
        )

    @staticmethod
    def _random_datetime(start_datetime=None, end_datetime=None):
        """
        Create a random datetime within the given boundaries.

        If no start date is given, we start 5 years ago.
        If no end date is given, we end now.
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
        video_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@video+block@{video_uuid}"

    def get_video_id(self):
        """
        Return a video id from our list of known video ids.
        """
        return choice(self.known_video_ids)

    def _generate_random_problem_id(self):
        problem_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@problem+block@{problem_uuid}"

    def get_problem_id(self):
        """
        Return a problem id from our list of known problem ids.
        """
        return choice(self.known_problem_ids)

    def _generate_random_sequential_id(self):
        sequential_uuid = str(uuid.uuid4())
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@sequential+block@{sequential_uuid}"

    def get_random_sequential_id(self):
        """
        Return a sequential id from our list of known sequential ids.
        """
        return choice(self.known_sequential_ids)

    def get_random_nav_location(self):
        """
        Return a navigation location from our list of known ids.
        """
        return str(randrange(1, self.items_in_course))

    def serialize_course_data_for_event_sink(self):
        """
        Return a dict representing the course data from event-sink-clickhouse.
        """
        return {
            "org": self.org,
            "course_key": self.course_id,
            "display_name": f"Course {self.course_uuid[:5]}",
            "course_start": self.start_date,
            "course_end": self.end_date,
            "enrollment_start": self.start_date,
            "enrollment_end": self.end_date,
            "self_paced": choice([True, False]),
            # This is a catchall field, we don't currently use it
            "course_data_json": "{}",
            "created": self.start_date,
            "modified": self.end_date
        }

    def _serialize_block(self, block_type, block_id, cnt):
        return {
            "org": self.org,
            "course_key": self.course_id,
            "location": block_id.split("/xblock/")[-1],
            "display_name": f"{block_type} {cnt}",
            # This is a catchall field, we don't currently use it
            "xblock_data_json": "{}",
            "order": cnt,
            "edited_on": self.end_date
        }

    def _serialize_course_block(self):
        location_course_id = self.course_id.replace("course-v1:", "")
        return {
            "org": self.org,
            "course_key": self.course_id,
            "location": f"block-v1:{location_course_id}+type@course+block@course",
            "display_name": f"Course {self.course_uuid[:5]}",
            # This is a catchall field, we don't currently use it
            "xblock_data_json": "{}",
            "order": 1,
            "edited_on": self.end_date
        }

    def serialize_block_data_for_event_sink(self):
        """
        Return a list of dicts representing all blocks in this course.

        The data format mirrors what is created by event-sink-clickhouse.
        """
        blocks = []
        cnt = 1
        for v in self.known_video_ids:
            blocks.append(self._serialize_block("Video", v, cnt))
            cnt += 1
        for p in self.known_problem_ids:
            blocks.append(self._serialize_block("Problem", p, cnt))
            cnt += 1
        for s in self.known_sequential_ids:
            blocks.append(self._serialize_block("Sequential", s, cnt))
            cnt += 1
        blocks.append(self._serialize_course_block())

        return blocks
