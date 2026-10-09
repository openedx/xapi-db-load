"""
Expected engagement results for simulated learners.

These are computed from what the simulator recorded each learner doing, independently of any SQL,
and follow the definitions behind the Aspects engagement charts:

- pages: units (verticals) in the subsection / section with a navigation event for them
- problems: problems with at least one evaluated (server-side check) event
- videos: videos with any watched seconds. "observable" (``done``) is what the events can show:
  each play paired with the next video event for the same learner and video, counted when the
  position moved forward. "truth" (``done_truth``) is the playback that actually happened,
  including stretches cut off by closing the page.

Section rows add up every subsection in the section, including ones the learner never opened.
Only rows where the section / subsection has at least one item of that type are written.
"""

from typing import (
    Dict,
    Iterator,
    List,
)

from xapi_db_load.journeys.simulate import EnrollmentTruth
from xapi_db_load.journeys.structure import (
    Block,
    JourneyCourse,
)

STATUS_LABELS = {
    "pages": ("No pages viewed yet", "At least one page viewed", "All pages viewed"),
    "problems": (
        "No problems attempted yet",
        "At least one problem attempted",
        "All problems attempted",
    ),
    "videos": ("No videos viewed yet", "At least one video viewed", "All videos viewed"),
}

ENGAGEMENT_COLUMNS = [
    "org",
    "course_key",
    "actor_id",
    "content_level",
    "block_id",
    "metric",
    "done",
    "total",
    "status",
    "done_truth",
    "status_truth",
]

VIDEO_SECONDS_COLUMNS = [
    "org",
    "course_key",
    "actor_id",
    "video_block_id",
    "video_duration",
    "truth_seconds",
    "truth_distinct_seconds",
    "observable_seconds",
    "observable_distinct_seconds",
]


def status(metric: str, done: int, total: int) -> str:
    """Return the Aspects label for done-out-of-total."""
    none_label, some_label, all_label = STATUS_LABELS[metric]
    if done == 0:
        return none_label
    if done == total:
        return all_label
    return some_label


def _items(seq: Block, block_type: str) -> List[Block]:
    if block_type == "vertical":
        return list(seq.children)
    return [c for v in seq.children for c in v.children if c.block_type == block_type]


def _counts(seq: Block, truth: EnrollmentTruth) -> Dict[str, tuple]:
    """Return {metric: (done, total, done_truth)} for one subsection."""
    pages = _items(seq, "vertical")
    problems = _items(seq, "problem")
    videos = _items(seq, "video")

    def watched(v: Block, kind: str) -> bool:
        vt = truth.videos.get(v.location)
        if not vt:
            return False
        segments = vt.truth if kind == "truth" else vt.observable_segments()
        return any(b > a for a, b in segments)

    pages_done = sum(p.location in truth.pages_viewed for p in pages)
    problems_done = sum(p.location in truth.problems_attempted for p in problems)
    return {
        "pages": (pages_done, len(pages), pages_done),
        "problems": (problems_done, len(problems), problems_done),
        "videos": (
            sum(watched(v, "observable") for v in videos),
            len(videos),
            sum(watched(v, "truth") for v in videos),
        ),
    }


def engagement_rows(
    course: JourneyCourse, actor_id: str, truth: EnrollmentTruth
) -> Iterator[Dict]:
    """Yield the expected engagement rows for one enrollment."""
    for chapter in course.chapters:
        section_sums = {m: [0, 0, 0] for m in STATUS_LABELS}
        for seq in chapter.children:
            for metric, (done, total, done_truth) in _counts(seq, truth).items():
                section_sums[metric][0] += done
                section_sums[metric][1] += total
                section_sums[metric][2] += done_truth
                if total:
                    yield _row(course, actor_id, "subsection", seq, metric, done, total, done_truth)
        for metric, (done, total, done_truth) in section_sums.items():
            if total:
                yield _row(course, actor_id, "section", chapter, metric, done, total, done_truth)


def _row(course, actor_id, level, block, metric, done, total, done_truth) -> Dict:  # pylint: disable=too-many-positional-arguments
    return {
        "org": course.org,
        "course_key": course.course_key,
        "actor_id": actor_id,
        "content_level": level,
        "block_id": block.location,
        "metric": metric,
        "done": done,
        "total": total,
        "status": status(metric, done, total),
        "done_truth": done_truth,
        "status_truth": status(metric, done_truth, total),
    }


def video_seconds_rows(
    course: JourneyCourse, actor_id: str, truth: EnrollmentTruth, lengths: Dict[str, int]
) -> Iterator[Dict]:
    """Yield expected watched seconds per video for one enrollment."""
    for location, vt in sorted(truth.videos.items()):
        yield {
            "org": course.org,
            "course_key": course.course_key,
            "actor_id": actor_id,
            "video_block_id": location,
            "video_duration": lengths[location],
            **vt.summary(),
        }
