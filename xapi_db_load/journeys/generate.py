"""
Generate a learner-journey dataset: event-sink and xAPI CSV files plus the expected results.

Output files (in ``output_dir``), using the same CSV layouts as the csv backend so they can be
loaded the same way:

- courses.csv.gz, blocks.csv.gz, external_ids.csv.gz, user_profiles.csv.gz: event sink tables
- xapi.csv.gz: xapi_events_all rows (event_id, emission_time, event)
- expected_engagement.csv.gz, expected_video_seconds.csv.gz: known answers (with header row)
- manifest.json: settings, counts, and learners worth benchmarking the learner dashboard with
"""

import csv
import datetime
import gzip
import json
import logging
import os
import random
from contextlib import ExitStack
from typing import (
    Dict,
    List,
    Optional,
)

from xapi_db_load.journeys import oracle
from xapi_db_load.journeys.simulate import (
    Behavior,
    JourneyActor,
    Simulator,
)
from xapi_db_load.journeys.structure import (
    CourseTemplate,
    JourneyCourse,
    seeded_uuid,
)

TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
FILES = (
    "courses",
    "blocks",
    "external_ids",
    "user_profiles",
    "xapi",
    "expected_engagement",
    "expected_video_seconds",
)


def _fmt(t: datetime.datetime) -> str:
    return t.astimezone(datetime.UTC).strftime(TIME_FORMAT)


class JourneyGenerator:
    """Builds a journey dataset from the ``journeys`` section of a config file."""

    def __init__(self, conf: Dict, lms_url: str, logger: logging.Logger):
        self.conf = conf
        self.lms_url = lms_url
        self.log = logger
        self.rng = random.Random(conf.get("seed", 0))
        now = conf.get("now")
        self.now = (
            datetime.datetime.fromisoformat(str(now)).replace(tzinfo=datetime.UTC)
            if now
            else datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        )
        window_days = conf.get("window_days", 300)
        if window_days >= 365:
            # Aspects' default TTL drops events older than a year.
            raise ValueError("window_days must be under 365 to survive the xAPI data TTL.")
        if conf.get("course_length_days", 120) > window_days:
            raise ValueError("course_length_days must not be more than window_days.")
        self.behavior = Behavior.from_config(conf.get("behavior"))
        self.templates = {
            name: CourseTemplate.from_config(t)
            for name, t in conf["course_templates"].items()
        }
        self.actors: List[JourneyActor] = []
        self.courses: List[JourneyCourse] = []
        self.course_windows: Dict[str, tuple] = {}

    # ---- setup ------------------------------------------------------------------------------

    def setup(self) -> None:
        """Create actors and courses."""
        mbox_fraction = self.conf.get("mbox_actor_fraction", 0.0)
        for i in range(self.conf["num_actors"]):
            username = f"actor_{i}"
            self.actors.append(
                JourneyActor(
                    user_id=i + 1,
                    external_id=seeded_uuid(self.rng),
                    username=username,
                    name=f"Actor {i}",
                    email=f"{username}@aspects.invalid",
                    uses_mbox=self.rng.random() < mbox_fraction,
                )
            )

        orgs = [f"Org{i}" for i in range(self.conf.get("num_organizations", 3))]
        window = datetime.timedelta(days=self.conf.get("window_days", 300))
        length = datetime.timedelta(days=self.conf.get("course_length_days", 120))
        running_fraction = self.conf.get("running_course_fraction", 0.5)
        deleted_fraction = self.conf.get("deleted_unit_course_fraction", 0.0)

        for entry in self.conf["courses"]:
            template = self.templates[entry["template"]]
            for _ in range(entry["count"]):
                org = self.rng.choice(orgs)
                code = seeded_uuid(self.rng)[:6]
                for run in range(self.rng.randint(*entry.get("runs", [1, 1]))):
                    course = JourneyCourse(
                        self.rng, org, code, f"run{run}", entry["template"], template,
                        self.lms_url,
                    )
                    if self.rng.random() < deleted_fraction:
                        course.add_deleted_vertical(self.rng)
                    if self.rng.random() < running_fraction:
                        end = self.now
                    else:
                        end = self.now - self.rng.random() * (window - length)
                    self.course_windows[course.course_key] = (end - length, end)
                    course.learner_count = self.rng.randint(*entry["learners"])
                    self.courses.append(course)

        self.log.info(f"{len(self.actors)} actors, {len(self.courses)} course runs")

    # ---- writing ----------------------------------------------------------------------------

    def run(self, output_dir: str) -> Dict:
        """Generate everything into ``output_dir`` and return the manifest."""
        self.setup()
        os.makedirs(output_dir, exist_ok=True)
        with ExitStack() as stack:
            handles = {
                name: stack.enter_context(
                    gzip.open(os.path.join(output_dir, f"{name}.csv.gz"), "wt", newline="")
                )
                for name in FILES
            }
            writers = {name: csv.writer(h) for name, h in handles.items()}
            writers["expected_engagement"] = csv.DictWriter(
                handles["expected_engagement"], oracle.ENGAGEMENT_COLUMNS
            )
            writers["expected_video_seconds"] = csv.DictWriter(
                handles["expected_video_seconds"], oracle.VIDEO_SECONDS_COLUMNS
            )
            writers["expected_engagement"].writeheader()
            writers["expected_video_seconds"].writeheader()

            self._write_actors(writers)
            self._write_courses(writers)
            manifest = self._write_journeys(writers)

        manifest.update(
            {
                "seed": self.conf.get("seed", 0),
                "now": _fmt(self.now),
                "lms_url": self.lms_url,
                "num_actors": len(self.actors),
                "num_course_runs": len(self.courses),
                "mbox_actors": sum(a.uses_mbox for a in self.actors),
                "courses_with_deleted_unit": sum(bool(c.deleted_blocks) for c in self.courses),
                "config": self.conf,
            }
        )
        with open(os.path.join(output_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2, default=str)
        return manifest

    def _write_actors(self, writers) -> None:
        """Write external ids and user profiles, dumped before any course starts."""
        dump_time = self.now - datetime.timedelta(days=400)
        for a in self.actors:
            writers["external_ids"].writerow(
                (a.external_id, "xapi", a.username, a.user_id, seeded_uuid(self.rng),
                 _fmt(dump_time))
            )
        for change in range(self.conf.get("num_actor_profile_changes", 1)):
            t = dump_time + datetime.timedelta(days=change)
            for a in self.actors:
                writers["user_profiles"].writerow(
                    (a.user_id, a.user_id, a.name, a.username, a.email, "{}", "", "", "",
                     1990, "", "", "", "", "", "", "", "", "", "",
                     seeded_uuid(self.rng), _fmt(t))
                )

    def _write_courses(self, writers) -> None:
        """
        Write every publish of every course; deleted units are only in the earlier publishes.
        """
        publishes = self.conf.get("course_publishes", 1)
        for course in self.courses:
            start, end = self.course_windows[course.course_key]
            for p in range(publishes):
                final = p == publishes - 1
                dump_id = seeded_uuid(self.rng)
                dump_time = _fmt(start - datetime.timedelta(days=publishes - p))
                writers["courses"].writerow(
                    (course.org, course.course_key, course.name, _fmt(start), _fmt(end),
                     _fmt(start), _fmt(end), False, "{}", _fmt(start), dump_time, dump_id,
                     dump_time)
                )
                for b in course.block_rows(include_deleted=not final):
                    xblock_data = json.dumps(
                        {
                            "block_type": b["block_type"],
                            "section": b["section"],
                            "subsection": b["subsection"],
                            "unit": b["unit"],
                            "graded": b["graded"],
                        }
                    )
                    writers["blocks"].writerow(
                        (course.org, course.course_key, b["location"], b["display_name"],
                         xblock_data, b["order"], dump_time, dump_id, dump_time)
                    )

    def _write_journeys(self, writers) -> Dict:  # pylint: disable=too-many-locals
        """Simulate every enrollment, writing its events and expected results."""
        sim = Simulator(self.rng, self.behavior, self.now)
        sort_events = self.conf.get("sort_events", False)
        buffered: List[Dict] = []
        n_events = 0
        n_enrollments = 0
        heavy: List[Dict] = []

        for ci, course in enumerate(self.courses):
            start, end = self.course_windows[course.course_key]
            lengths = {
                b.location: b.length for b in course.all_blocks() if b.block_type == "video"
            }
            total_pages = sum(len(seq.children) for seq in course.sequentials())
            learners = self.rng.sample(self.actors, min(course.learner_count, len(self.actors)))
            best_actor: Optional[JourneyActor] = None
            best_pages = 0
            for actor in learners:
                enroll_time = start + (end - start) * self.rng.random() * 0.6
                events, truth = sim.run_enrollment(course, actor, enroll_time, running=end == self.now)
                n_enrollments += 1
                n_events += len(events)
                if sort_events:
                    buffered.extend(events)
                else:
                    for e in events:
                        writers["xapi"].writerow(
                            (e["event_id"], _fmt(e["emission_time"]), e["event"])
                        )
                for row in oracle.engagement_rows(course, actor.actor_id, truth):
                    writers["expected_engagement"].writerow(row)
                for row in oracle.video_seconds_rows(course, actor.actor_id, truth, lengths):
                    writers["expected_video_seconds"].writerow(row)
                if total_pages and (best_actor is None or len(truth.pages_viewed) > best_pages):
                    best_actor, best_pages = actor, len(truth.pages_viewed)

            if best_actor:
                heavy.append(
                    {
                        "course_key": course.course_key,
                        "actor_id": best_actor.actor_id,
                        "username": best_actor.username,
                        "pages_viewed_fraction": round(best_pages / total_pages, 3),
                    }
                )
            if ci % 50 == 0:
                self.log.info(f"course {ci + 1}/{len(self.courses)}: {n_events} events so far")

        if sort_events:
            buffered.sort(key=lambda e: (e["emission_time"], e["event_id"]))
            for e in buffered:
                writers["xapi"].writerow((e["event_id"], _fmt(e["emission_time"]), e["event"]))

        return {"num_enrollments": n_enrollments, "num_xapi_events": n_events,
                "heavy_learners": heavy}
