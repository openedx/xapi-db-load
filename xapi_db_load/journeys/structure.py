"""
Properly nested course structures for learner journeys.

Each course is chapters (sections) > sequentials (subsections) > verticals (units) > problems and
videos. The structure is built once and reused for every publish, so the generator and the
expected results agree on which subsection every block belongs to.
"""

import random
import uuid
from dataclasses import (
    dataclass,
    field,
)
from typing import (
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)


def seeded_uuid(rng: random.Random) -> str:
    """Return a UUID4 string drawn from ``rng`` so runs with the same seed are reproducible."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


@dataclass
class Block:
    """One course block, with its position in the course outline."""

    block_type: str  # chapter, sequential, vertical, problem, video
    location: str  # block-v1:... usage key
    url: str  # LMS xblock URL, as used for xAPI object ids
    display_name: str
    section: int
    subsection: int
    unit: int
    order: int = 0
    graded: bool = False
    length: int = 0  # Video length in seconds, 0 for other blocks
    children: List["Block"] = field(default_factory=list)


@dataclass
class CourseTemplate:
    """Ranges used to randomize the size of a course."""

    chapters: Tuple[int, int]
    sequentials_per_chapter: Tuple[int, int]
    verticals_per_sequential: Tuple[int, int]
    # Each vertical gets a random count of these, drawn from the given weights
    # (index = count), e.g. [0.4, 0.4, 0.2] -> 40% none, 40% one, 20% two.
    problems_per_vertical: List[float]
    videos_per_vertical: List[float]
    graded_sequential_fraction: float
    video_length: Tuple[int, int]

    @classmethod
    def from_config(cls, conf: Dict) -> "CourseTemplate":
        """Build a template from a YAML ``course_templates`` entry."""
        return cls(
            chapters=tuple(conf["chapters"]),
            sequentials_per_chapter=tuple(conf["sequentials_per_chapter"]),
            verticals_per_sequential=tuple(conf["verticals_per_sequential"]),
            problems_per_vertical=list(conf["problems_per_vertical"]),
            videos_per_vertical=list(conf["videos_per_vertical"]),
            graded_sequential_fraction=conf.get("graded_sequential_fraction", 0.3),
            video_length=tuple(conf.get("video_length", [60, 900])),
        )


class JourneyCourse:
    """A course run with a fixed, nested structure."""

    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        rng: random.Random,
        org: str,
        course_code: str,
        run: str,
        template_name: str,
        template: CourseTemplate,
        lms_url: str,
    ):
        self.org = org
        self.course_key = f"course-v1:{org}+{course_code}+{run}"
        self.url = f"{lms_url}/course/{self.course_key}"
        self.name = f"{course_code} ({template_name})"
        self.lms_url = lms_url
        self.learner_count = 0
        self._order = 0
        self.chapters: List[Block] = []
        # Blocks that only exist in earlier publishes, see add_deleted_vertical.
        self.deleted_blocks: List[Block] = []
        self._build(rng, template)

    def _new_block(  # pylint: disable=too-many-positional-arguments
        self,
        rng: random.Random,
        block_type: str,
        name: str,
        section: int,
        subsection: int,
        unit: int,
        graded: bool = False,
        length: int = 0,
    ) -> Block:
        """Create a block with the next position in course order."""
        self._order += 1
        key = self.course_key.replace("course-v1:", "")
        block_id = seeded_uuid(rng).replace("-", "")[:16]
        location = f"block-v1:{key}+type@{block_type}+block@{block_id}"
        return Block(
            block_type=block_type,
            location=location,
            url=f"{self.lms_url}/xblock/{location}",
            display_name=name,
            section=section,
            subsection=subsection,
            unit=unit,
            order=self._order,
            graded=graded,
            length=length,
        )

    def _build(self, rng: random.Random, t: CourseTemplate) -> None:
        """Build the outline, drawing each level's size from the template's ranges."""
        for s in range(1, rng.randint(*t.chapters) + 1):
            chapter = self._new_block(rng, "chapter", f"Section {s}", s, 0, 0)
            self.chapters.append(chapter)
            for ss in range(1, rng.randint(*t.sequentials_per_chapter) + 1):
                graded = rng.random() < t.graded_sequential_fraction
                seq = self._new_block(
                    rng, "sequential", f"Subsection {s}.{ss}", s, ss, 0, graded
                )
                chapter.children.append(seq)
                for u in range(1, rng.randint(*t.verticals_per_sequential) + 1):
                    vert = self._new_block(
                        rng, "vertical", f"Unit {s}.{ss}.{u}", s, ss, u, graded
                    )
                    seq.children.append(vert)
                    n_videos = rng.choices(
                        range(len(t.videos_per_vertical)), t.videos_per_vertical
                    )[0]
                    n_problems = rng.choices(
                        range(len(t.problems_per_vertical)), t.problems_per_vertical
                    )[0]
                    for v in range(n_videos):
                        vert.children.append(
                            self._new_block(
                                rng,
                                "video",
                                f"Video {s}.{ss}.{u}.{v + 1}",
                                s,
                                ss,
                                u,
                                graded,
                                length=rng.randint(*t.video_length),
                            )
                        )
                    for p in range(n_problems):
                        vert.children.append(
                            self._new_block(
                                rng,
                                "problem",
                                f"Problem {s}.{ss}.{u}.{p + 1}",
                                s,
                                ss,
                                u,
                                graded,
                            )
                        )

    def add_deleted_vertical(self, rng: random.Random) -> Optional[Block]:
        """
        Record a unit that existed in earlier publishes but was deleted before the final one.

        It's placed in the first subsection, so it would inflate that subsection's page count if a
        report kept counting it.
        """
        if not self.chapters or not self.chapters[0].children:
            return None
        seq = self.chapters[0].children[0]
        unit = len(seq.children) + 1
        block = self._new_block(
            rng, "vertical", f"Unit {seq.section}.{seq.subsection}.{unit} (deleted)",
            seq.section, seq.subsection, unit, seq.graded,
        )
        self.deleted_blocks.append(block)
        return block

    def sequentials(self) -> Iterator[Block]:
        """Yield the subsections in course order."""
        for chapter in self.chapters:
            yield from chapter.children

    def all_blocks(self) -> Iterator[Block]:
        """Yield every block of the final publish, parents before children."""
        for chapter in self.chapters:
            yield chapter
            for seq in chapter.children:
                yield seq
                for vert in seq.children:
                    yield vert
                    yield from vert.children

    def block_rows(self, include_deleted: bool) -> Iterator[Dict]:
        """
        Yield course_blocks rows as event-sink-clickhouse writes them.

        ``include_deleted`` adds the blocks that only exist in earlier publishes.
        """
        key = self.course_key.replace("course-v1:", "")
        yield {
            "location": f"block-v1:{key}+type@course+block@course",
            "display_name": self.name,
            "block_type": "course",
            "section": 0,
            "subsection": 0,
            "unit": 0,
            "graded": False,
            "order": 0,
        }
        blocks = list(self.all_blocks())
        if include_deleted:
            blocks += self.deleted_blocks
        for b in blocks:
            yield {
                "location": b.location,
                "display_name": b.display_name,
                "block_type": b.block_type,
                "section": b.section,
                "subsection": b.subsection,
                "unit": b.unit,
                "graded": b.graded,
                "order": b.order,
            }
