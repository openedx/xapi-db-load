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

# Prefix of the fake organizations (``Org0``, ``Org1``...) when ``org_prefix``
# is not set. A different prefix keeps a load apart from earlier ones in the
# org filters.
DEFAULT_ORG_PREFIX = "Org"

# Export id written on every fake object tag when ``taxonomy_export_id`` is not
# set. An export id starting with ``COMPETENCY_EXPORT_ID_PREFIX`` makes the fake
# taxonomy a competency taxonomy: Aspects tells competency taxonomies apart by
# that prefix until the taxonomy sink carries a competency flag, and only then
# are learner competency statuses generated.
DEFAULT_TAXONOMY_EXPORT_ID = "fake export id"
COMPETENCY_EXPORT_ID_PREFIX = "competency-"

# First ids given to the fake taxonomies, tags and object tags when
# ``first_taxonomy_id``, ``first_tag_id`` and ``first_object_tag_id`` are not
# set. Raise them above the ids of real rows the LMS dumps into the same
# event_sink tables, or the two overwrite each other.
DEFAULT_FIRST_TAXONOMY_ID = 1
DEFAULT_FIRST_TAG_ID = 1
DEFAULT_FIRST_OBJECT_TAG_ID = 1

# First id given to a fake Student Competency Status
# (``event_sink.student_competency_status``) when
# ``first_student_competency_status_id`` is not set, kept above the ids of real
# statuses dumped by the LMS.
DEFAULT_FIRST_STUDENT_COMPETENCY_STATUS_ID = 100001

# openedx-core mastery statuses. Core never stores AttemptedNotDemonstrated (10)
# at the competency level, only on criteria groups, and "not started" is no row.
MASTERY_STATUS_ATTEMPTED_NOT_DEMONSTRATED = (10, "AttemptedNotDemonstrated")
MASTERY_STATUS_PARTIALLY_ATTEMPTED = (20, "PartiallyAttempted")
MASTERY_STATUS_DEMONSTRATED = (30, "Demonstrated")

# How a learner enrolled in a course stands on each competency the course
# assesses, as weights for (no status yet, attempted not demonstrated, partially
# attempted, demonstrated). That is the course-level criteria group status; the
# competency-level status records AttemptedNotDemonstrated as PartiallyAttempted.
COMPETENCY_STATUS_WEIGHTS = (0.2, 0.15, 0.25, 0.4)

# First id given to a fake Student Competency Criteria Group Status
# (``event_sink.student_competency_criteria_group_status``) and to the fake
# criteria groups those rows point at, when ``first_competency_group_status_id``
# and ``first_competency_criteria_group_id`` are not set.
DEFAULT_FIRST_COMPETENCY_GROUP_STATUS_ID = 100001
DEFAULT_FIRST_COMPETENCY_CRITERIA_GROUP_ID = 100001

# A learner's first status on a competency lands soon after they enroll: an
# exponential delay with this median, in hours. Their first xAPI activity in the
# course comes after the enrollment too, so time to mastery measured from either
# one is mostly the mastery time below.
COMPETENCY_FIRST_STATUS_MEDIAN_HOURS = 12.0

# Each competency gets its own difficulty: the median time its learners take to
# demonstrate it, drawn log-uniformly from this range (in days), so some take
# hours and others weeks. Each learner then takes a log-normal time around that
# median with this sigma.
COMPETENCY_MASTERY_MEDIAN_DAYS_RANGE = (0.15, 30.0)
COMPETENCY_MASTERY_SIGMA = 0.6

# Range of "course runs" generated per logical course (inclusive of low,
# exclusive of high — passed straight to ``random.randrange``).
MIN_COURSE_RUNS = 1
MAX_COURSE_RUNS = 5
