"""
Fake xAPI statements for various video events.
"""
import json
from uuid import uuid4

from .xapi_common import XAPIBase


class BaseVideo(XAPIBase):
    """
    Base xAPI class for video events.
    """

    enabled = False

    def get_data(self):
        """
        Generate and return the event dict, including xAPI statement as "event".
        """
        event_id = str(uuid4())
        actor_id = self.parent_load_generator.get_actor()
        course = self.parent_load_generator.get_course()
        video_id = course.get_video_id()
        emission_time = course.get_random_emission_time()

        e = self.get_randomized_event(
            event_id, actor_id, course, video_id, emission_time
        )

        return {
            "event_id": event_id,
            "verb": self.verb,
            "actor_id": actor_id,
            "org": course.org,
            "course_run_id": course.course_url,
            "video_id": video_id,
            "emission_time": emission_time,
            "event": e,
        }

    def get_randomized_event(self, event_id, account, course, video_id, create_time):
        """
        Given the inputs, return an xAPI statement.
        """
        event = {
            "id": event_id,
            "actor": {
                "objectType": "Agent",
                "account": {"homePage": "http://localhost:18000", "name": account},
            },
            "context": {
                "contextActivities": {
                    "parent": [
                        {
                            "id": course.course_url,
                            "objectType": "Activity",
                            "definition": {
                                "name": {"en-US": "Demonstration Course"},
                                "type": "http://adlnet.gov/expapi/activities/course",
                            },
                        }
                    ]
                },
                "extensions": {
                    "https://github.com/openedx/event-routing-backends/blob/master/docs/xapi-extensions/eventVersion.rst": "1.0",  # pylint: disable=line-too-long
                    "https://w3id.org/xapi/video/extensions/length": 195.0,
                },
            },
            "object": {
                "definition": {
                    "type": "https://w3id.org/xapi/video/activity-type/video"
                },
                "id": video_id,
                "objectType": "Activity",
            },
            "result": {
                "extensions": {"https://w3id.org/xapi/video/extensions/time": 0.033}
            },
            "timestamp": create_time.isoformat(),
            "verb": {"display": {"en": self.verb_display}, "id": self.verb},
            "version": "1.0.3",
        }

        if self.verb_display == "interacted":
            event["result"]["extensions"]["https://w3id.org/xapi/video/extensions/cc-enabled"] = self.enabled

        return json.dumps(event)


class LoadedVideo(BaseVideo):
    verb = "http://adlnet.gov/expapi/verbs/initialized"
    verb_display = "initialized"


class PlayedVideo(BaseVideo):
    verb = "https://w3id.org/xapi/video/verbs/played"
    verb_display = "played"


# TODO: These four technically need different structures, though we're not using them now. Update!
class StoppedVideo(BaseVideo):
    verb = "http://adlnet.gov/expapi/verbs/terminated"
    verb_display = "terminated"


class PausedVideo(BaseVideo):
    verb = "https://w3id.org/xapi/video/verbs/paused"
    verb_display = "paused"


class PositionChangedVideo(BaseVideo):
    verb = "https://w3id.org/xapi/video/verbs/seeked"
    verb_display = "seeked"


class CompletedVideo(BaseVideo):
    verb = "http://adlnet.gov/expapi/verbs/completed"
    verb_display = "completed"


class TranscriptEnabled(BaseVideo):
    verb = "http://adlnet.gov/expapi/verbs/interacted"
    verb_display = "interacted"
    enabled = True


class TranscriptDisabled(BaseVideo):
    verb = "http://adlnet.gov/expapi/verbs/interacted"
    verb_display = "interacted"
