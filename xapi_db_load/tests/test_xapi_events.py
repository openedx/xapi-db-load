"""
Shape and content tests for every concrete xAPI event class.

These tests guard against regressions in the dict layout returned by
``get_data()``, validate that the embedded ``event`` field is valid JSON,
and verify that the configurable ``lms_url`` propagates through to the
generated statements (i.e. nothing is silently hardcoded back to localhost).
"""
# pylint: disable=redefined-outer-name

import asyncio
import datetime
import json
import logging

import pytest

from xapi_db_load.constants import DEFAULT_VIDEO_LENGTH_SECONDS
from xapi_db_load.course_configs import RandomCourse
from xapi_db_load.generate_load_async import EventGenerator
from xapi_db_load.xapi.xapi_forum import PostCreated
from xapi_db_load.xapi.xapi_grade import (
    CourseGradeCalculated,
    FirstTimePassed,
    SubsectionGradeCalculated,
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

# Custom LMS URL so we can assert it propagates through the whole stack
# instead of accidentally falling back to the hardcoded default.
TEST_LMS_URL = "https://lms.test.example.com"

# Every concrete event class the load tool can emit.
ALL_EVENT_CLASSES = [
    # Video
    LoadedVideo,
    PlayedVideo,
    PausedVideo,
    StoppedVideo,
    CompletedVideo,
    PositionChangedVideo,
    TranscriptEnabled,
    TranscriptDisabled,
    # Problem
    BrowserProblemCheck,
    ServerProblemCheck,
    # Navigation
    NextNavigation,
    PreviousNavigation,
    TabSelectedNavigation,
    LinkClicked,
    # Hint / Answer
    ShowHint,
    ShowAnswer,
    # Forum
    PostCreated,
    # Grade
    FirstTimePassed,
    CourseGradeCalculated,
    SubsectionGradeCalculated,
    # Registration
    Registered,
    Unregistered,
]


def _make_config() -> dict:
    """Return a minimal but valid EventGenerator config."""
    return {
        "lms_url": TEST_LMS_URL,
        "start_date": datetime.date(2023, 1, 1),
        "end_date": datetime.date(2023, 12, 31),
        "course_length_days": 30,
        "num_organizations": 2,
        "num_actors": 5,
        "num_course_sizes": {"small": 2},
        "num_course_publishes": 1,
        "course_size_makeup": {
            "small": {
                "actors": 2,
                "problems": 3,
                "videos": 2,
                "chapters": 2,
                "sequences": 2,
                "verticals": 2,
                "forum_posts": 2,
            }
        },
        "batch_size": 10,
        "num_xapi_batches": 1,
        "num_actor_profile_changes": 1,
    }


@pytest.fixture(scope="module")
def event_generator() -> EventGenerator:
    """Build a fully-populated EventGenerator usable by every event class."""
    gen = EventGenerator(_make_config(), logging.getLogger("test"), None)
    asyncio.run(gen.run_task())
    return gen


REQUIRED_KEYS = {"event_id", "verb", "actor_id", "emission_time", "event"}


@pytest.mark.parametrize("event_class", ALL_EVENT_CLASSES, ids=lambda c: c.__name__)
def test_event_get_data_shape(event_generator, event_class):
    """Every event class returns a dict with the expected keys and a JSON event."""
    data = event_class(event_generator).get_data()

    assert REQUIRED_KEYS.issubset(data.keys()), (
        f"{event_class.__name__} missing required keys: "
        f"{REQUIRED_KEYS - set(data.keys())}"
    )
    assert data["verb"] == event_class.verb
    assert isinstance(data["actor_id"], str) and data["actor_id"]
    assert isinstance(data["event_id"], str) and data["event_id"]

    # The "event" field is a JSON-encoded xAPI statement.
    statement = json.loads(data["event"])
    assert statement["id"] == data["event_id"]
    assert statement["verb"]["id"] == event_class.verb


@pytest.mark.parametrize("event_class", ALL_EVENT_CLASSES, ids=lambda c: c.__name__)
def test_event_respects_configured_lms_url(event_generator, event_class):
    """The configured ``lms_url`` must appear in the generated actor.account.homePage."""
    data = event_class(event_generator).get_data()
    statement = json.loads(data["event"])

    home_page = statement["actor"]["account"]["homePage"]
    assert home_page == TEST_LMS_URL, (
        f"{event_class.__name__} generated homePage={home_page!r}, "
        f"expected {TEST_LMS_URL!r}"
    )


VIDEO_TIME_EXT = "https://w3id.org/xapi/video/extensions/time"
VIDEO_LENGTH_EXT = "https://w3id.org/xapi/video/extensions/length"


@pytest.mark.parametrize("clip", [(30.0, 150.0), None], ids=["clipped", "full"])
def test_video_clip_times(event_generator, monkeypatch, clip):
    """
    Clipped videos report the clip length but positions in the source video.

    The course block data for the video carries the clip times.
    """
    course = event_generator.courses[0]
    video_id = course.video_ids[0]
    monkeypatch.setattr(course, "video_clips", {video_id: clip} if clip else {})
    monkeypatch.setattr(event_generator, "get_course", lambda: course)
    monkeypatch.setattr(course, "get_video_id", lambda: video_id)

    start, end = clip or (0.0, DEFAULT_VIDEO_LENGTH_SECONDS)

    for _ in range(20):
        statement = json.loads(PlayedVideo(event_generator).get_data()["event"])
        assert statement["context"]["extensions"][VIDEO_LENGTH_EXT] == end - start
        assert start <= statement["result"]["extensions"][VIDEO_TIME_EXT] < end

    location = video_id.split("/xblock/")[-1]
    block = next(
        b
        for b in course.serialize_block_data_for_event_sink()
        if b["location"] == location
    )
    xblock_data = json.loads(block["xblock_data_json"])
    assert xblock_data["video_start_time"] == (clip[0] if clip else 0.0)
    assert xblock_data["video_end_time"] == (clip[1] if clip else 0.0)


def test_generated_video_clips_are_valid():
    """Generated clips start within bounds and end within the source video."""
    for _ in range(100):
        start, end = RandomCourse._generate_random_video_clip()  # pylint: disable=protected-access
        assert 0 < start < end <= DEFAULT_VIDEO_LENGTH_SECONDS
