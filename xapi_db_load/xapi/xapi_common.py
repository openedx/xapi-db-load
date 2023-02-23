"""
Shared functionality across xAPI events.
"""


class XAPIBase:
    """
    Base class for XAPI events.
    """

    verb = None
    verb_display = None

    def __init__(self, load_generator):
        """
        Initialize base class for fake xAPI events.
        """
        if not self.verb:
            raise NotImplementedError(
                f"XAPIBase is abstract, add your verb in subclass {type(self)}."
            )
        self.parent_load_generator = load_generator
