"""
Simulate learners moving through journey courses, emitting xAPI events and recording what happened.
"""

import datetime
import json
import math
import random
from dataclasses import (
    dataclass,
    field,
)
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from xapi_db_load.journeys import statements as st
from xapi_db_load.journeys.structure import (
    Block,
    JourneyCourse,
    seeded_uuid,
)


@dataclass
class JourneyActor:
    """A learner and the identifiers Aspects sees for them."""

    user_id: int
    external_id: str
    username: str
    name: str
    email: str
    # When True, events identify the learner by mailto: email instead of account name.
    uses_mbox: bool = False

    @property
    def actor_id(self) -> str:
        """Return the actor id as Aspects parses it from events."""
        return f"mailto:{self.email}" if self.uses_mbox else self.external_id

    def xapi_actor(self, lms_url: str) -> Dict:
        """Return the xAPI actor object for this learner."""
        return st.actor_json(
            lms_url, self.external_id, f"mailto:{self.email}" if self.uses_mbox else None
        )


@dataclass
class Behavior:  # pylint: disable=too-many-instance-attributes
    """Probabilities that shape learner behavior; see example_configs/journeys_*.yaml."""

    start_prob: float = 0.8
    completer_fraction: float = 0.15
    continue_prob: float = 0.9
    unit_skip_prob: float = 0.08
    revisit_prob: float = 0.05
    exit_without_nav_prob: float = 0.7
    problem_attempt_prob: float = 0.8
    max_attempts: int = 3
    video_watch_prob: float = 0.75
    second_session_prob: float = 0.15
    days_between_subsections: float = 2.0
    # Weights for what ends each stretch of video playback.
    video_actions: Dict[str, float] = field(
        default_factory=lambda: {
            "to_end": 0.35,
            "pause": 0.25,
            "seek": 0.15,
            "abandon": 0.1,
            "terminate": 0.15,
        }
    )
    resume_after_pause_prob: float = 0.6
    # Share of learners in still-running courses whose activity is moved so it ends within the
    # last ``recent_hours``, giving refreshable views something inside their lookback window.
    recent_activity_fraction: float = 0.05
    recent_hours: float = 12.0
    # Edge cases, mainly for the known-answer scenario.
    same_time_resume_prob: float = 0.0
    duplicate_event_prob: float = 0.0

    @classmethod
    def from_config(cls, conf: Optional[Dict]) -> "Behavior":
        """Build from the YAML ``behavior`` section; missing keys keep their defaults."""
        b = cls()
        for k, v in (conf or {}).items():
            if not hasattr(b, k):
                raise ValueError(f"Unknown behavior setting: {k}")
            setattr(b, k, v)
        return b


@dataclass
class VideoTruth:
    """
    One learner's viewing of one video.

    ``truth`` is the playback that actually happened, as (from, to) whole-second positions.
    ``stream`` is the video events emitted for it, from which ``observable_segments`` derives what
    the events can show. Second n covers playback from n-1 to n.
    """

    truth: List[Tuple[int, int]] = field(default_factory=list)
    # (emission_time, event_id, verb, position) for every emitted non-initialized video event.
    stream: List[Tuple[datetime.datetime, str, str, float]] = field(default_factory=list)

    def observable_segments(self) -> List[Tuple[int, int]]:
        """
        Pair each play with the next video event and keep the pairs that moved forward.

        Events are ordered by time; on equal times any other event goes before a play, so a pause
        and a resume at the same instant stay in the order they happened. The next event's
        position is ``time`` (or ``time-from`` for a seek); ``completed`` always reports the full
        length, as event-routing-backends sends it.
        """
        ordered = sorted(self.stream, key=lambda e: (e[0], e[2] == st.VERB_PLAYED, e[1]))
        segments = []
        for (_, _, verb, start), (_, _, _, end) in zip(ordered, ordered[1:]):
            if verb == st.VERB_PLAYED and end > start:
                segments.append((int(start), int(end)))
        return segments

    @staticmethod
    def _total(segments) -> int:
        return sum(b - a for a, b in segments)

    @staticmethod
    def _distinct(segments) -> int:
        seconds: Set[int] = set()
        for a, b in segments:
            seconds.update(range(a + 1, b + 1))
        return len(seconds)

    def summary(self) -> Dict[str, int]:
        """Return total and distinct watched seconds, actual and observable from events."""
        observable = self.observable_segments()
        return {
            "truth_seconds": self._total(self.truth),
            "truth_distinct_seconds": self._distinct(self.truth),
            "observable_seconds": self._total(observable),
            "observable_distinct_seconds": self._distinct(observable),
        }


@dataclass
class EnrollmentTruth:
    """Everything a learner did in one course that the engagement reports should reflect."""

    pages_viewed: Set[str] = field(default_factory=set)  # vertical locations with a nav event
    problems_attempted: Set[str] = field(default_factory=set)  # with an evaluated event
    videos: Dict[str, VideoTruth] = field(default_factory=dict)  # by video location


def _retime(raw: str, when: datetime.datetime) -> str:
    """Return the statement JSON with its timestamp set to ``when``."""
    statement = json.loads(raw)
    statement["timestamp"] = when.isoformat()
    return json.dumps(statement)


class Simulator:
    """Generates the events for one enrollment at a time."""

    def __init__(self, rng: random.Random, behavior: Behavior, now: datetime.datetime):
        self.rng = rng
        self.b = behavior
        self.now = now
        self.events: List[Dict] = []
        self._actor: Dict = {}
        self._course: Optional[JourneyCourse] = None

    # ---- event plumbing ---------------------------------------------------------------------

    def _emit(self, when: datetime.datetime, statement_fn, *args, **kwargs) -> Optional[str]:
        """Emit an event unless it would be in the future; return its id if emitted."""
        if when > self.now:
            return None
        event_id = seeded_uuid(self.rng)
        row = {
            "event_id": event_id,
            "emission_time": when,
            "event": statement_fn(event_id, self._actor, *args, when=when, **kwargs),
        }
        self.events.append(row)
        if self.rng.random() < self.b.duplicate_event_prob:
            self.events.append(dict(row))
        return event_id

    def _clamp(self, when: datetime.datetime) -> bool:
        """Return True when ``when`` is still in the past, i.e. activity can happen then."""
        return when <= self.now

    # ---- journeys ---------------------------------------------------------------------------

    def run_enrollment(
        self,
        course: JourneyCourse,
        actor: JourneyActor,
        enroll_time: datetime.datetime,
        running: bool = False,
    ) -> Tuple[List[Dict], EnrollmentTruth]:
        """
        Simulate one learner in one course; return its events and what it did.

        In a ``running`` course some learners are still active: their whole journey is moved
        later so the last event lands within the last ``recent_hours``.
        """
        recent = running and self.rng.random() < self.b.recent_activity_fraction
        if not recent:
            return self._journey(course, actor, enroll_time)
        # Simulate as if "now" were far away so nothing is cut off, then shift into place.
        real_now = self.now
        self.now = datetime.datetime.max.replace(tzinfo=datetime.UTC)
        try:
            events, truth = self._journey(course, actor, enroll_time)
        finally:
            self.now = real_now
        last = max(e["emission_time"] for e in events)
        target = real_now - datetime.timedelta(hours=self.rng.random() * self.b.recent_hours)
        shift = target - last
        for e in events:
            e["emission_time"] += shift
            e["event"] = _retime(e["event"], e["emission_time"])
        return events, truth

    def _journey(
        self,
        course: JourneyCourse,
        actor: JourneyActor,
        enroll_time: datetime.datetime,
    ) -> Tuple[List[Dict], EnrollmentTruth]:
        self.events = []
        self._actor = actor.xapi_actor(course.lms_url)
        self._course = course
        truth = EnrollmentTruth()

        mode = self.rng.choice(("audit", "honor", "verified"))
        self._emit(enroll_time, st.registered, course.url, mode)

        if self.rng.random() >= self.b.start_prob:
            return self.events, truth

        completer = self.rng.random() < self.b.completer_fraction
        t = enroll_time + datetime.timedelta(minutes=self.rng.randint(1, 600))
        sequentials = list(course.sequentials())

        for i, seq in enumerate(sequentials):
            if not self._clamp(t):
                break
            last = i == len(sequentials) - 1
            stopping = last or (not completer and self.rng.random() >= self.b.continue_prob)
            t = self._run_subsection(seq, t, truth, stopping)
            if stopping:
                break
            gap_days = self.rng.expovariate(1 / self.b.days_between_subsections)
            t += datetime.timedelta(days=gap_days)

        return self.events, truth

    def _run_subsection(
        self, seq: Block, t: datetime.datetime, truth: EnrollmentTruth, stopping: bool
    ) -> datetime.datetime:
        """Work through one subsection's units in order, starting at ``t``."""
        units = seq.children
        u = 0
        while u < len(units):
            unit = units[u]
            t = self._work_unit(unit, t, truth)
            final_unit = u == len(units) - 1

            # Leaving the unit emits a navigation event for it, except when the learner just
            # closes the page at the end of their study session.
            if final_unit and stopping and self.rng.random() < self.b.exit_without_nav_prob:
                break

            t += datetime.timedelta(seconds=self.rng.randint(2, 20))
            if not final_unit and self.rng.random() < self.b.revisit_prob and u > 0:
                # Go back one unit and come forward again: two more nav events, one of them
                # for the previous unit.
                self._nav(unit, units, u, "previous unit", t, truth)
                t += datetime.timedelta(seconds=self.rng.randint(10, 120))
                self._nav(units[u - 1], units, u - 1, "next unit", t, truth)
                t += datetime.timedelta(seconds=self.rng.randint(2, 20))
            if not final_unit and self.rng.random() < self.b.unit_skip_prob and u + 2 < len(units):
                # Jump ahead with the unit tabs, skipping the next unit entirely.
                self._nav(unit, units, u, str(u + 3), t, truth)
                u += 2
            else:
                self._nav(unit, units, u, "next unit", t, truth)
                u += 1
        return t

    def _nav(  # pylint: disable=too-many-positional-arguments
        self, unit: Block, units: List[Block], index: int, ending: str, t, truth: EnrollmentTruth
    ) -> None:
        assert self._course
        if not self._clamp(t):
            return
        self._emit(t, st.navigated, self._course.url, unit.url, len(units), index + 1, ending)
        truth.pages_viewed.add(unit.location)

    def _work_unit(self, unit: Block, t: datetime.datetime, truth: EnrollmentTruth):
        """Spend time on a unit: read, watch its videos, attempt its problems."""
        t += datetime.timedelta(seconds=self.rng.randint(15, 240))
        for child in unit.children:
            if not self._clamp(t):
                break
            if child.block_type == "video" and self.rng.random() < self.b.video_watch_prob:
                t = self._watch_video(child, t, truth)
                if self.rng.random() < self.b.second_session_prob:
                    t = self._watch_video(child, t + datetime.timedelta(minutes=5), truth)
            elif child.block_type == "problem" and self.rng.random() < self.b.problem_attempt_prob:
                t = self._attempt_problem(child, t, truth)
        return t

    def _attempt_problem(self, problem: Block, t, truth: EnrollmentTruth):
        assert self._course
        max_score = self.rng.randint(1, 10)
        for attempt in range(1, self.rng.randint(1, self.b.max_attempts) + 1):
            t += datetime.timedelta(seconds=self.rng.randint(20, 300))
            if not self._clamp(t):
                break
            success = self.rng.random() < 0.4 + 0.2 * attempt
            raw = max_score if success else self.rng.randint(0, max_score - 1)
            self._emit(t, st.problem_attempted, self._course.url, problem.url)
            t += datetime.timedelta(seconds=1)
            self._emit(
                t, st.problem_evaluated, self._course.url, problem.url,
                attempt, success, raw, max_score,
            )
            if self._clamp(t):
                truth.problems_attempted.add(problem.location)
            if success:
                break
        return t

    def _watch_video(self, vid: Block, t, truth: EnrollmentTruth):  # pylint: disable=too-many-statements
        """
        One viewing session: load, play, then a series of pauses / seeks until it ends.

        Positions are whole seconds. As in the edX player, ``completed`` fires once per session,
        when playback first reaches 95%, and reports the full length.
        """
        assert self._course
        course_url = self._course.url
        length = vid.length
        threshold = math.ceil(length * 0.95)
        vt = truth.videos.setdefault(vid.location, VideoTruth())
        actions = list(self.b.video_actions)
        weights = list(self.b.video_actions.values())
        completed_sent = False

        def emit(verb, when, position=None, **kw):
            event_id = self._emit(
                when, st.video, course_url, vid.url, float(length), verb, **kw
            )
            if event_id and verb != st.VERB_INITIALIZED:
                vt.stream.append((when, event_id, verb, position))

        if not self._clamp(t):
            return t
        emit(st.VERB_INITIALIZED, t)
        pos = 0
        t += datetime.timedelta(seconds=self.rng.randint(1, 10))
        emit(st.VERB_PLAYED, t, pos, time=float(pos))

        while True:
            action = self.rng.choices(actions, weights)[0]
            if action == "to_end" or length - pos <= 5:
                stop = length
            else:
                stop = self.rng.randint(pos + 1, length - 1)
            start_t = t
            t = start_t + datetime.timedelta(seconds=stop - pos)
            vt.truth.append((pos, stop))
            if not completed_sent and stop >= threshold:
                # Strictly before the event that ends this stretch: two closing events at the
                # same instant would have no defined order.
                done_t = min(
                    start_t + datetime.timedelta(seconds=max(threshold - pos, 0) + 0.25),
                    t - datetime.timedelta(milliseconds=1),
                )
                emit(st.VERB_COMPLETED, done_t, length, time=float(length))
                completed_sent = True
            if not self._clamp(t):
                break  # Still playing right now; nothing marks the end yet.

            if stop == length:
                emit(st.VERB_TERMINATED, t, length, time=float(length))
                break
            if action == "abandon":
                break
            if action == "terminate":
                emit(st.VERB_TERMINATED, t, stop, time=float(stop))
                break
            if action == "pause":
                emit(st.VERB_PAUSED, t, stop, time=float(stop))
                if self.rng.random() >= self.b.resume_after_pause_prob:
                    break
                if self.rng.random() >= self.b.same_time_resume_prob:
                    t += datetime.timedelta(seconds=self.rng.randint(2, 120))
                pos = stop
                emit(st.VERB_PLAYED, t, pos, time=float(pos))
                continue
            # Seek while playing: the player logs the seek, then a play from the new position.
            target = self.rng.randint(0, length - 1)
            emit(st.VERB_SEEKED, t, stop, time_from=float(stop), time_to=float(target))
            t += datetime.timedelta(seconds=1)
            pos = target
            emit(st.VERB_PLAYED, t, pos, time=float(pos))
        return t
