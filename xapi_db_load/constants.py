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

# Fraction of generated videos configured to play only a clip of the source
# video, using a start and end time. The video player reports positions against
# the full source video but the length of only the clip.
VIDEO_CLIP_FRACTION = 0.25

# Bounds (in seconds) for the start time of generated video clips, and the
# shortest clip generated.
VIDEO_CLIP_MIN_START_SECONDS = 10
VIDEO_CLIP_MAX_START_SECONDS = 60
VIDEO_CLIP_MIN_LENGTH_SECONDS = 60

# Lengths used when truncating UUIDs into short, human-readable identifiers.
UUID_SHORT_LENGTH = 8
COURSE_ID_SHORT_LENGTH = 6

# Range of "course runs" generated per logical course (inclusive of low,
# exclusive of high — passed straight to ``random.randrange``).
MIN_COURSE_RUNS = 1
MAX_COURSE_RUNS = 5
