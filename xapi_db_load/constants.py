"""
Shared constants for the xapi-db-load package.

Centralizing these values makes the tool easier to reconfigure and avoids
"magic" literals scattered through event generators and course config code.
"""

# Default LMS base URL used in generated xAPI statements when not overridden
# via config. Override by setting ``lms_url`` in the YAML config file.
DEFAULT_LMS_URL = "http://localhost:18000"

# xAPI statement version emitted on every event.
XAPI_VERSION = "1.0.3"

# Human-readable display name used for the synthetic "Demonstration Course"
# in xAPI activity definitions.
DEMO_COURSE_NAME = "Demonstration Course"

# Hardcoded transformer / session identifiers from the original fixtures.
# These match values produced by event-routing-backends and are referenced
# in downstream regexes/tests, so they intentionally do not vary per event.
TRANSFORMER_VERSION = "event-routing-backends@7.0.1"
SESSION_ID_PLACEHOLDER = "e4858858443cd99828206e294587dac5"

# Default length (in seconds) used for synthetic video events.
DEFAULT_VIDEO_LENGTH_SECONDS = 195.0

# Lengths used when truncating UUIDs into short, human-readable identifiers.
UUID_SHORT_LENGTH = 8
COURSE_ID_SHORT_LENGTH = 6

# Catalog Pathways (openedx-core ``openedx_catalog``) written to the
# ``event_sink.catalog_pathway`` table when ``num_catalog_pathways`` is not set.
DEFAULT_NUM_CATALOG_PATHWAYS = 6

# First id given to a fake Catalog Pathway when ``first_catalog_pathway_id`` is
# not set. High enough not to collide with real pathways dumped by the LMS
# into the same table, the way ``first_user_id`` keeps fake users apart.
DEFAULT_FIRST_CATALOG_PATHWAY_ID = 100001

# Pathway Categories the fake Catalog Pathways rotate through, as
# (category_code, title) pairs.
PATHWAY_CATEGORIES = (
    ("masters", "Master's Degree"),
    ("annual-training", "Annual Training"),
    ("professional-certificate", "Professional Certificate"),
)

# Learners enrolled in each fake Catalog Pathway (``event_sink.pathway_enrollment``)
# when ``num_enrollments_per_pathway`` is not set. Capped at ``num_actors``.
DEFAULT_NUM_ENROLLMENTS_PER_PATHWAY = 5

# First id given to a fake Pathway Enrollment when ``first_pathway_enrollment_id``
# is not set, kept above the ids of real enrollments for the same reason as
# ``DEFAULT_FIRST_CATALOG_PATHWAY_ID``.
DEFAULT_FIRST_PATHWAY_ENROLLMENT_ID = 100001

# Share of fake Pathway Enrollments that end up unenrolled (``is_active`` False).
PATHWAY_UNENROLLED_RATIO = 0.1

# Defaults of the ``org_prefix``, ``taxonomy_export_id`` and ``taxonomy_type``
# config keys (see README for what each does).
DEFAULT_ORG_PREFIX = "Org"
DEFAULT_TAXONOMY_EXPORT_ID = "fake export id"

# The values the platform-plugin-aspects taxonomy sink writes in
# ``event_sink.taxonomy.taxonomy_type``: "competency" for an openedx-core
# CompetencyTaxonomy, "tags" otherwise. Only these two are accepted.
TAXONOMY_TYPE_TAGS = "tags"
TAXONOMY_TYPE_COMPETENCY = "competency"
TAXONOMY_TYPES = (TAXONOMY_TYPE_TAGS, TAXONOMY_TYPE_COMPETENCY)
DEFAULT_TAXONOMY_TYPE = TAXONOMY_TYPE_TAGS

# Defaults of ``first_taxonomy_id``, ``first_tag_id`` and ``first_object_tag_id``.
# They stay at 1, not above the real ids like the status ids below, because
# earlier loads always numbered these rows from 1: a load with the defaults
# keeps replacing the same fake rows instead of adding a second copy.
DEFAULT_FIRST_TAXONOMY_ID = 1
DEFAULT_FIRST_TAG_ID = 1
DEFAULT_FIRST_OBJECT_TAG_ID = 1

# Defaults of ``first_student_competency_status_id``,
# ``first_student_competency_course_status_id`` and
# ``first_competency_criteria_group_id``, kept above the ids of real rows dumped
# by the LMS, as with ``DEFAULT_FIRST_CATALOG_PATHWAY_ID``.
DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID = 100001
DEFAULT_FIRST_STUDENT_COMPETENCY_COURSE_STATUS_ID = 100001
DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID = 100001

# openedx-core mastery status ids and the names the sinks write next to them.
# Core never stores AttemptedNotDemonstrated at the competency level, only on
# criteria groups, and "not started" is no row at all. The ids rank the statuses:
# a higher id is further along, and the generator compares them with max() and >.
MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED = 10
MASTERY_STATUS_PARTIALLY_ATTEMPTED = 20
MASTERY_STATUS_DEMONSTRATED = 30
MASTERY_STATUS_NAMES = {
    MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED: "AttemptedNotDemonstrated",
    MASTERY_STATUS_PARTIALLY_ATTEMPTED: "PartiallyAttempted",
    MASTERY_STATUS_DEMONSTRATED: "Demonstrated",
}

# How a learner enrolled in a course stands on each competency the course
# assesses: the course-level criteria group status, drawn with these weights for
# (no status, AttemptedNotDemonstrated, PartiallyAttempted, Demonstrated). The
# competency-level status records AttemptedNotDemonstrated as PartiallyAttempted.
COMPETENCY_COURSE_STATUS_WEIGHTS = (0.2, 0.15, 0.25, 0.4)

# A learner's first status on a competency lands soon after they enroll: an
# exponential delay with this median, in hours. Their first xAPI activity in the
# course comes after the enrollment too, so time to mastery measured from either
# one is mostly the mastery time below.
COMPETENCY_FIRST_STATUS_MEDIAN_HOURS = 12.0

# Each competency gets its own difficulty: the median time its learners take to
# demonstrate it, drawn log-uniformly from this range (in days) on every run, so
# some take hours and others weeks. Each learner then takes a log-normal time
# around that median with this sigma.
COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE = (0.15, 30.0)
COMPETENCY_MASTERY_SIGMA = 0.6

# Range of "course runs" generated per logical course (inclusive of low,
# exclusive of high — passed straight to ``random.randrange``).
MIN_COURSE_RUNS = 1
MAX_COURSE_RUNS = 5
