"""
Tests for learner-journey generation.

The key check re-derives the expected engagement results from the generated xAPI events alone, so
the simulator's bookkeeping and the events it emits can't drift apart.
"""

import collections
import csv
import datetime
import gzip
import json
import logging
import pathlib
import random

import pytest
import yaml
from click.testing import CliRunner

from xapi_db_load.journeys import statements as st
from xapi_db_load.journeys.generate import JourneyGenerator
from xapi_db_load.journeys.oracle import status
from xapi_db_load.journeys.simulate import (
    Behavior,
    EnrollmentTruth,
    Simulator,
    VideoTruth,
)
from xapi_db_load.journeys.structure import (
    CourseTemplate,
    JourneyCourse,
)
from xapi_db_load.main import journeys

CONFIG = pathlib.Path(__file__).parents[2] / "example_configs" / "journeys_oracle.yaml"
NOW = "2026-10-08 12:00:00"


def _generate(out: pathlib.Path, **overrides) -> dict:
    conf = yaml.safe_load(CONFIG.read_text())["journeys"]
    conf["now"] = NOW
    conf.update(overrides)
    return JourneyGenerator(conf, "http://localhost:18000", logging.getLogger()).run(str(out))


def _rows(path: pathlib.Path):
    with gzip.open(path, "rt", newline="") as f:
        yield from csv.reader(f)


def _dict_rows(path: pathlib.Path):
    with gzip.open(path, "rt", newline="") as f:
        yield from csv.DictReader(f)


@pytest.fixture(name="dataset", scope="module")
def fixture_dataset(tmp_path_factory):
    out = tmp_path_factory.mktemp("journeys")
    manifest = _generate(out)
    return out, manifest


def _actor_id(event: dict) -> str:
    actor = event["actor"]
    return actor["mbox"] if "mbox" in actor else actor["account"]["name"]


def _location(url: str) -> str:
    return url.split("/xblock/")[-1]


def _events(out):
    """
    Unique events (duplicates removed by id), in time order.

    On equal timestamps any other event sorts before a video play, so a pause and the resume at
    the same instant pair up in the order they happened.
    """
    seen = {}
    for event_id, emission_time, raw in _rows(out / "xapi.csv.gz"):
        seen[event_id] = (emission_time, json.loads(raw))
    return sorted(
        seen.values(),
        key=lambda e: (e[0], e[1]["verb"]["id"] == st.VERB_PLAYED, e[1]["id"]),
    )


def _structure(out):
    """Return {course_key: {block location: (type, section, subsection, unit)}}, final publish."""
    latest = {}
    for row in _rows(out / "blocks.csv.gz"):
        _org, course_key, location, _name, data, _order, _edited, _dump, dumped = row
        if location not in latest or dumped > latest[location][0]:
            latest[location] = (dumped, course_key, json.loads(data))
    final_dump = {}
    for dumped, course_key, _ in latest.values():
        final_dump[course_key] = max(final_dump.get(course_key, ""), dumped)
    courses = collections.defaultdict(dict)
    for location, (dumped, course_key, d) in latest.items():
        if dumped == final_dump[course_key]:  # blocks missing from the final publish are deleted
            courses[course_key][location] = (
                d["block_type"], d["section"], d["subsection"], d["unit"]
            )
    return courses


def test_reproducible(tmp_path):
    _generate(tmp_path / "a")
    _generate(tmp_path / "b")
    for name in ("xapi", "blocks", "expected_engagement", "expected_video_seconds"):
        a = gzip.open(tmp_path / "a" / f"{name}.csv.gz").read()
        b = gzip.open(tmp_path / "b" / f"{name}.csv.gz").read()
        assert a == b, name


def test_ttl_guard(tmp_path):
    with pytest.raises(ValueError, match="window_days"):
        _generate(tmp_path, window_days=400)


def test_events_sorted_and_recent(dataset):
    out, manifest = dataset
    times = [t for _, t, _ in _rows(out / "xapi.csv.gz")]
    assert times == sorted(times)
    assert max(times) <= manifest["now"]
    # Some activity falls inside a 1-day refresh lookback window.
    assert any(t >= "2026-10-07 12:00:00" for t in times)


def test_structure_is_nested(dataset):
    out, _ = dataset
    for blocks in _structure(out).values():
        units = {(s, ss, u) for t, s, ss, u in blocks.values() if t == "vertical"}
        subsections = {(s, ss) for t, s, ss, _ in blocks.values() if t == "sequential"}
        for t, s, ss, u in blocks.values():
            if t in ("problem", "video"):
                assert (s, ss, u) in units
            if t == "vertical":
                assert (s, ss) in subsections


def test_edge_cases_present(dataset):
    out, manifest = dataset
    assert manifest["mbox_actors"] > 0
    assert manifest["courses_with_deleted_unit"] > 0
    ids = collections.Counter(r[0] for r in _rows(out / "xapi.csv.gz"))
    assert any(c > 1 for c in ids.values()), "expected duplicated events"
    # Pause and resume at the same instant.
    by_time = collections.defaultdict(set)
    for t, e in _events(out):
        by_time[(t, _actor_id(e), e["object"]["id"])].add(e["verb"]["id"])
    assert any({st.VERB_PAUSED, st.VERB_PLAYED} <= v for v in by_time.values())


def test_expected_engagement_matches_events(dataset):  # pylint: disable=too-many-locals
    """Rebuild every expected subsection/section row from the raw events and structure."""
    out, _ = dataset
    structure = _structure(out)
    pages = collections.defaultdict(set)
    problems = collections.defaultdict(set)
    for _, e in _events(out):
        verb = e["verb"]["id"]
        key = (e["context"]["contextActivities"]["parent"][0]["id"].split("/course/")[-1]
               if "contextActivities" in e.get("context", {}) else None, _actor_id(e))
        if verb == st.VERB_NAVIGATED:
            pages[key].add(_location(e["object"]["id"]))
        elif verb == st.VERB_EVALUATED:
            problems[key].add(_location(e["object"]["id"]))

    checked = 0
    for row in _dict_rows(out / "expected_engagement.csv.gz"):
        if row["metric"] == "videos":
            continue
        blocks = structure[row["course_key"]]
        _, s, ss, _ = blocks[row["block_id"]]
        level = row["content_level"]
        item_type = "vertical" if row["metric"] == "pages" else "problem"
        items = {
            loc for loc, (t, bs, bss, _) in blocks.items()
            if t == item_type and bs == s and (level == "section" or bss == ss)
        }
        seen = (pages if row["metric"] == "pages" else problems)[
            (row["course_key"], row["actor_id"])
        ]
        done = len(items & seen)
        assert (done, len(items)) == (int(row["done"]), int(row["total"])), row
        assert row["status"] == status(row["metric"], done, len(items))
        checked += 1
    assert checked > 1000


def test_observable_video_seconds_match_events(dataset):
    """
    Pair each play with the next event for the same learner and video; the forward progress of
    those pairs is what the events can show, and must equal the expected observable seconds.
    """
    out, _ = dataset
    streams = collections.defaultdict(list)
    for _, e in _events(out):
        if e["verb"]["id"] in st.VIDEO_VERB_DISPLAY and e["verb"]["id"] != st.VERB_INITIALIZED:
            course = e["context"]["contextActivities"]["parent"][0]["id"].split("/course/")[-1]
            streams[(course, _actor_id(e), _location(e["object"]["id"]))].append(e)

    def position(e):
        ext = e["result"]["extensions"]
        return ext.get(st.EXT_VIDEO_TIME, ext.get(st.EXT_VIDEO_TIME_FROM))

    derived = collections.Counter()
    for key, events in streams.items():
        for cur, nxt in zip(events, events[1:]):
            if cur["verb"]["id"] == st.VERB_PLAYED and position(nxt) > position(cur):
                derived[key] += int(position(nxt) - position(cur))

    expected = {
        (r["course_key"], r["actor_id"], r["video_block_id"]): int(r["observable_seconds"])
        for r in _dict_rows(out / "expected_video_seconds.csv.gz")
    }
    mismatches = {k: (derived.get(k, 0), v) for k, v in expected.items() if derived.get(k, 0) != v}
    assert not mismatches, list(mismatches.items())[:5]


def test_unsorted_output_has_same_events(dataset, tmp_path):
    """sort_events only changes the order of xapi.csv, not what is generated."""
    out, _ = dataset
    _generate(tmp_path, sort_events=False)
    for name in ("xapi", "expected_engagement", "expected_video_seconds"):
        assert sorted(_rows(out / f"{name}.csv.gz")) == sorted(_rows(tmp_path / f"{name}.csv.gz"))


def test_status_labels():
    assert status("pages", 0, 3) == "No pages viewed yet"
    assert status("problems", 1, 3) == "At least one problem attempted"
    assert status("videos", 3, 3) == "All videos viewed"


def test_unknown_behavior_setting():
    with pytest.raises(ValueError, match="Unknown behavior setting: typo"):
        Behavior.from_config({"typo": 1})


def _stream(*events):
    """Build a video event stream from (second, verb, position) tuples."""
    t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    return [
        (t0 + datetime.timedelta(seconds=sec), f"id{i}", verb, pos)
        for i, (sec, verb, pos) in enumerate(events)
    ]


def test_observable_segments_pause_and_resume_at_same_time():
    """A pause and a resume with the same timestamp still pair in the order they happened."""
    vt = VideoTruth(
        truth=[(0, 40), (40, 60)],
        # The resume is listed first to show that ordering doesn't depend on input order.
        stream=_stream(
            (40, st.VERB_PLAYED, 40),
            (0, st.VERB_PLAYED, 0),
            (40, st.VERB_PAUSED, 40),
            (60, st.VERB_TERMINATED, 60),
        ),
    )
    assert vt.observable_segments() == [(0, 40), (40, 60)]
    assert vt.summary() == {
        "truth_seconds": 60,
        "truth_distinct_seconds": 60,
        "observable_seconds": 60,
        "observable_distinct_seconds": 60,
    }


def test_observable_segments_seek_back_and_close():
    """Seeking back re-watches seconds; closing the tab leaves the last play unpaired."""
    vt = VideoTruth(
        truth=[(0, 30), (10, 25)],
        stream=_stream(
            (0, st.VERB_PLAYED, 0),
            (30, st.VERB_SEEKED, 30),  # from 30 back to 10
            (31, st.VERB_PLAYED, 10),
        ),
    )
    assert vt.observable_segments() == [(0, 30)]
    assert vt.summary() == {
        "truth_seconds": 45,
        "truth_distinct_seconds": 30,
        "observable_seconds": 30,
        "observable_distinct_seconds": 30,
    }


def test_video_truth_stops_at_now():
    """A video still playing at "now" only counts the seconds played so far, and has no end event."""
    rng = random.Random(1)
    template = CourseTemplate.from_config({
        "chapters": [1, 1], "sequentials_per_chapter": [1, 1], "verticals_per_sequential": [1, 1],
        "problems_per_vertical": [1], "videos_per_vertical": [0, 1], "video_length": [600, 600],
    })
    course = JourneyCourse(rng, "Org", "C", "run", "t", template, "http://lms")
    video = next(b for b in course.all_blocks() if b.block_type == "video")
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    sim = Simulator(rng, Behavior(video_actions={"to_end": 1}), start + datetime.timedelta(seconds=100))
    sim._course = course  # pylint: disable=protected-access
    truth = EnrollmentTruth()
    sim._watch_video(video, start, truth)  # pylint: disable=protected-access

    vt = truth.videos[video.location]
    (begin, end), = vt.truth
    assert begin == 0 and 80 <= end < 100  # play starts 1-10 s after loading
    assert [verb for _, _, verb, _ in vt.stream] == [st.VERB_PLAYED]
    assert vt.observable_segments() == []


def test_cli(tmp_path):
    out = tmp_path / "out"
    result = CliRunner().invoke(
        journeys,
        ["--config_file", str(CONFIG), "--output_dir", str(out), "--now", NOW, "--seed", "7"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert "xAPI events for" in result.output
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["seed"] == 7
    assert manifest["now"].startswith(NOW)
    for name in ("courses", "blocks", "external_ids", "user_profiles", "xapi",
                 "expected_engagement", "expected_video_seconds"):
        assert (out / f"{name}.csv.gz").exists()


@pytest.mark.parametrize(
    "journeys_conf, message",
    [
        (None, "has no 'journeys' section"),
        ({"window_days": 400}, "window_days must be under 365"),
        ({"window_days": 30, "course_length_days": 60}, "course_length_days"),
        ({"output_dir": None}, "Set --output_dir"),
    ],
)
def test_cli_config_errors(tmp_path, journeys_conf, message):
    conf = yaml.safe_load(CONFIG.read_text())
    if journeys_conf is None:
        del conf["journeys"]
    else:
        conf["journeys"].update(journeys_conf)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(conf))
    result = CliRunner().invoke(journeys, ["--config_file", str(config_file)])
    assert result.exit_code == 2
    assert message in result.output
