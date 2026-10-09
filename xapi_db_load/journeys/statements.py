"""
Builders for the xAPI statements emitted by learner journeys.

The shapes follow what event-routing-backends emits for the matching Open edX events, and the
statements built by the random event classes in ``xapi_db_load.xapi``.
"""

import datetime
import json
from typing import (
    Any,
    Dict,
    Optional,
)

from xapi_db_load.constants import (
    SESSION_ID_PLACEHOLDER,
    TRANSFORMER_VERSION,
    XAPI_VERSION,
)

VERB_REGISTERED = "http://adlnet.gov/expapi/verbs/registered"
VERB_NAVIGATED = "https://w3id.org/xapi/dod-isd/verbs/navigated"
VERB_ATTEMPTED = "http://adlnet.gov/expapi/verbs/attempted"
VERB_EVALUATED = "https://w3id.org/xapi/acrossx/verbs/evaluated"
VERB_INITIALIZED = "http://adlnet.gov/expapi/verbs/initialized"
VERB_PLAYED = "https://w3id.org/xapi/video/verbs/played"
VERB_PAUSED = "https://w3id.org/xapi/video/verbs/paused"
VERB_SEEKED = "https://w3id.org/xapi/video/verbs/seeked"
VERB_COMPLETED = "http://adlnet.gov/expapi/verbs/completed"
VERB_TERMINATED = "http://adlnet.gov/expapi/verbs/terminated"

VIDEO_VERB_DISPLAY = {
    VERB_INITIALIZED: "initialized",
    VERB_PLAYED: "played",
    VERB_PAUSED: "paused",
    VERB_SEEKED: "seeked",
    VERB_COMPLETED: "completed",
    VERB_TERMINATED: "terminated",
}

EXT_VIDEO_TIME = "https://w3id.org/xapi/video/extensions/time"
EXT_VIDEO_TIME_FROM = "https://w3id.org/xapi/video/extensions/time-from"
EXT_VIDEO_TIME_TO = "https://w3id.org/xapi/video/extensions/time-to"
EXT_VIDEO_LENGTH = "https://w3id.org/xapi/video/extensions/length"


def actor_json(lms_url: str, account_name: str, mbox: Optional[str]) -> Dict[str, Any]:
    """
    Return the xAPI actor.

    event-routing-backends identifies learners by an anonymous account name by default, or by
    ``mailto:`` email when configured with ``XAPI_AGENT_IFI_TYPE = "mbox"``.
    """
    if mbox:
        return {"objectType": "Agent", "mbox": mbox}
    return {"objectType": "Agent", "account": {"homePage": lms_url, "name": account_name}}


def _course_parent(course_url: str) -> Dict[str, Any]:
    return {
        "contextActivities": {
            "parent": [
                {
                    "id": course_url,
                    "objectType": "Activity",
                    "definition": {
                        "name": {"en-US": "Demonstration Course"},
                        "type": "http://adlnet.gov/expapi/activities/course",
                    },
                }
            ]
        }
    }


def _base(event_id: str, actor: Dict, verb: str, display: str, when: datetime.datetime) -> Dict:
    return {
        "id": event_id,
        "actor": actor,
        "timestamp": when.isoformat(),
        "verb": {"display": {"en": display}, "id": verb},
        "version": XAPI_VERSION,
    }


def registered(event_id: str, actor: Dict, course_url: str, mode: str, when) -> str:
    """Return an enrollment statement."""
    e = _base(event_id, actor, VERB_REGISTERED, "registered", when)
    e["context"] = {
        "extensions": {
            "https://w3id.org/xapi/openedx/extension/transformer-version": TRANSFORMER_VERSION,
            "https://w3id.org/xapi/openedx/extensions/session-id": SESSION_ID_PLACEHOLDER,
        }
    }
    e["object"] = {
        "definition": {
            "extensions": {"https://w3id.org/xapi/acrossx/extensions/type": mode},
            "name": {"en": "Demonstration Course"},
            "type": "http://adlnet.gov/expapi/activities/course",
        },
        "id": course_url,
        "objectType": "Activity",
    }
    return json.dumps(e)


def navigated(  # pylint: disable=too-many-positional-arguments
    event_id: str,
    actor: Dict,
    course_url: str,
    unit_url: str,
    tab_count: int,
    current_tab: int,
    ending_point: str,
    when,
) -> str:
    """
    Return a sequence navigation statement (next / previous / tab selected).

    As in the Learning MFE, the object is the unit the learner is navigating away from.
    """
    e = _base(event_id, actor, VERB_NAVIGATED, "navigated", when)
    e["context"] = _course_parent(course_url)
    e["context"]["extensions"] = {
        "https://w3id.org/xapi/openedx/extension/transformer-version": TRANSFORMER_VERSION,
        "https://w3id.org/xapi/openedx/extensions/session-id": SESSION_ID_PLACEHOLDER,
        "http://id.tincanapi.com/extension/starting-position": current_tab,
        "http://id.tincanapi.com/extension/ending-point": ending_point,
    }
    e["object"] = {
        "definition": {
            "extensions": {"https://w3id.org/xapi/acrossx/extensions/total-items": tab_count},
            "type": "http://id.tincanapi.com/activitytype/resource",
        },
        "id": unit_url,
        "objectType": "Activity",
    }
    return json.dumps(e)


def problem_attempted(event_id: str, actor: Dict, course_url: str, problem_url: str, when) -> str:
    """Return the browser-side problem check statement."""
    e = _base(event_id, actor, VERB_ATTEMPTED, "attempted", when)
    e["context"] = _course_parent(course_url)
    e["object"] = {
        "definition": {"type": "http://adlnet.gov/expapi/activities/cmi.interaction"},
        "id": problem_url,
        "objectType": "Activity",
    }
    return json.dumps(e)


def problem_evaluated(  # pylint: disable=too-many-positional-arguments
    event_id: str,
    actor: Dict,
    course_url: str,
    problem_url: str,
    attempt: int,
    success: bool,
    raw_score: int,
    max_score: int,
    when,
) -> str:
    """Return the server-side problem check statement."""
    e = _base(event_id, actor, VERB_EVALUATED, "evaluated", when)
    e["context"] = _course_parent(course_url)
    e["context"]["extensions"] = {
        "https://github.com/openedx/event-routing-backends/blob/master/docs/xapi-extensions/eventVersion.rst": "1.0"
    }
    e["object"] = {
        "definition": {
            "extensions": {"http://id.tincanapi.com/extension/attempt-id": attempt},
            "description": {"en-US": "Add the question text, or prompt, here."},
            "interactionType": "other",
            "type": "http://adlnet.gov/expapi/activities/cmi.interaction",
        },
        "id": problem_url,
        "objectType": "Activity",
    }
    e["result"] = {
        "response": "A correct answer" if success else "An incorrect answer",
        "score": {
            "scaled": raw_score / max_score,
            "raw": raw_score,
            "min": 0.0,
            "max": max_score,
        },
        "success": success,
    }
    return json.dumps(e)


def video(  # pylint: disable=too-many-positional-arguments
    event_id: str,
    actor: Dict,
    course_url: str,
    video_url: str,
    length: float,
    verb: str,
    when,
    time: Optional[float] = None,
    time_from: Optional[float] = None,
    time_to: Optional[float] = None,
) -> str:
    """Return a video statement; which result extensions are set depends on the verb."""
    e = _base(event_id, actor, verb, VIDEO_VERB_DISPLAY[verb], when)
    e["context"] = _course_parent(course_url)
    e["context"]["extensions"] = {
        "https://github.com/openedx/event-routing-backends/blob/master/docs/xapi-extensions/eventVersion.rst": "1.0",
        EXT_VIDEO_LENGTH: length,
    }
    e["object"] = {
        "definition": {"type": "https://w3id.org/xapi/video/activity-type/video"},
        "id": video_url,
        "objectType": "Activity",
    }
    extensions: Dict[str, float] = {}
    if time is not None:
        extensions[EXT_VIDEO_TIME] = time
    if time_from is not None:
        extensions[EXT_VIDEO_TIME_FROM] = time_from
    if time_to is not None:
        extensions[EXT_VIDEO_TIME_TO] = time_to
    e["result"] = {"extensions": extensions}
    return json.dumps(e)
