"""
Configuration values for emulating courses of various sizes.
"""
import datetime
import uuid
from random import choice, randrange


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

    def __init__(
        self,
        org,
        overall_start_date,
        overall_end_date,
        course_length,
        course_config_name,
        course_size_makeup
    ):
        self.course_uuid = str(uuid.uuid4())[:6]
        self.course_name = f"{self.course_uuid} ({course_config_name})"
        self.org = org
        self.course_id = f"course-v1:{org}+DemoX+{self.course_uuid}"
        self.course_url = f"http://localhost:18000/course/{self.course_id}"

        delta = datetime.timedelta(days=course_length)
        self.start_date = self._random_datetime(overall_start_date, overall_end_date-delta)
        self.end_date = self.start_date + delta

        self.course_config_name = course_config_name
        self.course_config = course_size_makeup
        self.configure()

    def __repr__(self):
        return f"""{self.course_name}:
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
        self.items_in_course = self.course_config["items"]

        self.known_problem_ids = [
            self._generate_random_problem_id()
            for _ in range(self.course_config["problems"])
        ]

        self.known_video_ids = [
            self._generate_random_video_id()
            for _ in range(self.course_config["videos"])
        ]

        self.known_sequential_ids = [
            self._generate_random_sequential_id()
            for _ in range(self.course_config["sequences"])
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
        video_uuid = str(uuid.uuid4())[:8]
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@video+block@{video_uuid}"

    def get_video_id(self):
        """
        Return a video id from our list of known video ids.
        """
        return choice(self.known_video_ids)

    def _generate_random_problem_id(self):
        problem_uuid = str(uuid.uuid4())[:8]
        return f"http://localhost:18000/xblock/block-v1:{self.course_id}+type@problem+block@{problem_uuid}"

    def get_problem_id(self):
        """
        Return a problem id from our list of known problem ids.
        """
        return choice(self.known_problem_ids)

    def _generate_random_sequential_id(self):
        sequential_uuid = str(uuid.uuid4())[:8]
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
            "display_name": self.course_name,
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

        Block types we care about:
        -- course block
        -- x video block
        -- vertical block
        -- static_tab block
        -- x sequential
        -- x problem block
        -- html block
        -- discussion block
        -- course_info
        -- chapter block
        -- about block
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
