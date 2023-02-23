"""
xAPI navigation events.
"""
import json
from uuid import uuid4

from .xapi_common import XAPIBase


class BaseNavigation(XAPIBase):
    """
    Base class for navigation events.
    """

    # All subclasses use these verbs currently
    verb = "https://w3id.org/xapi/dod-isd/verbs/navigated"
    verb_display = "navigated"

    # These are set in subclasses. If they are left unset then a random location will be chosen.
    from_loc = None
    to_loc = None

    # To differentiate between links and other nav events, should be "link" or "nav"
    type = None

    def get_data(self):
        """
        Gather necessary data and generates a random xAPI event with it.
        """
        event_id = str(uuid4())
        actor_id = self.parent_load_generator.get_actor()
        course = self.parent_load_generator.get_course()
        emission_time = course.get_random_emission_time()
        from_loc = self.from_loc or course.get_random_nav_location()
        to_loc = self.to_loc or course.get_random_nav_location()

        e = self.get_randomized_event(
            event_id, actor_id, course, from_loc, to_loc, emission_time
        )

        return {
            "event_id": event_id,
            "verb": self.verb,
            "actor_id": actor_id,
            "org": course.org,
            "course_run_id": course.course_url,
            "emission_time": emission_time,
            "nav_starting_point": str(from_loc),
            "nav_ending_point": str(to_loc),
            "event": e,
        }

    def get_randomized_event(
        self, event_id, account, course, from_loc, to_loc, create_time
    ):
        """
        Create an event dict that should map to the appropriate xAPI JSON.
        """
        event = {
            "id": event_id,
            "actor": {
                "account": {"homePage": "http://localhost:18000", "name": account},
                "objectType": "Agent",
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
                    "https://github.com/openedx/event-routing-backends/blob/master/docs/xapi-extensions/"
                    "eventVersion.rst": "1.0",
                },
            },
            "timestamp": create_time.isoformat(),
            "verb": {"display": {"en": self.verb_display}, "id": self.verb},
            "version": "1.0.3",
        }

        # Todo: If we care about the exact links we'll need to update this id to be something in the course
        if self.type == "link":
            event.update(
                {
                    "object": {
                        "definition": {
                            "type": "http://adlnet.gov/expapi/activities/link"
                        },
                        "id": "http://localhost:18000/courses/course-v1:edX+DemoX+Demo_Course/jump_to/"
                              "block-v1:edX+DemoX+Demo_Course+type@sequential+block@6ab9c442501d472c8ed200e367b4edfa",
                        "objectType": "Activity",
                    }
                }
            )
        else:
            event.update(
                {
                    "object": {
                        "definition": {
                            "extensions": {
                                "https://w3id.org/xapi/acrossx/extensions/total-items": course.items_in_course
                            },
                            "type": "http://id.tincanapi.com/activitytype/resource",
                        },
                        "id": course.get_random_sequential_id(),
                        "objectType": "Activity",
                    }
                }
            )

            event["context"]["extensions"][
                "http://id.tincanapi.com/extension/ending-point"
            ] = to_loc
            event["context"]["extensions"][
                "http://id.tincanapi.com/extension/starting-position"
            ] = from_loc

        return json.dumps(event)


class NextNavigation(BaseNavigation):
    """
    Next navigation events.
    """

    type = "nav"
    to_loc = "next unit"


class PreviousNavigation(BaseNavigation):
    """
    Previous navigation events.
    """

    type = "nav"
    to_loc = "previous unit"


class TabSelectedNavigation(BaseNavigation):
    """
    Tab selected navigation events.
    """

    type = "nav"


class LinkClicked(BaseNavigation):
    """
    Link clicked navigation events.
    """

    type = "link"
