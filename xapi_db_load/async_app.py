"""
Top level application state for UI and non-UI modes.
"""

import logging
import os
import sys

from xapi_db_load.runner import Runner


class App:
    """
    App is a singleton class that manages the global application state.
    """

    _shared_instance: "App|None" = None
    ui = None
    stages = []
    main_loop = None

    def __init__(self, config, ui: "TextUI | None" = None):
        # Ensure our singleton instance is set up
        if not App._shared_instance:
            App._shared_instance = self

        self.logfile_path = config.get("log_dir", "logs")
        self.logfile_path = os.path.join(self.logfile_path, "db_load.log")
        # If we are in UI mode, this lets us communicate with the UI
        self.ui = ui
        self._setup_logger()
        self.config = config

        # The Runner coordinates the async tasks from the configured backend
        self.runner = Runner(self.config, self.logger)

    def _setup_logger(self):
        """
        Logging is a little complicated, we always want to log to a file but also to stdout if we
        are not in UI mode.
        """
        self.logger = logging.getLogger(__name__)

        if not self.ui:
            stream_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s - %(message)s")
            stream_handler.setFormatter(formatter)
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(stream_handler)

        try:
            file_handler = logging.FileHandler(self.logfile_path)
            formatter = logging.Formatter(
                "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
            )
            file_handler.setFormatter(formatter)
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(file_handler)
        except Exception:
            self.logger.warning("Unable to open log file.")

        self.log("Logging set up")

    def set_main_loop(self, loop):
        """
        Set the main loop.

        The main loop is an asyncio loop, or uvloop if we are not in UI mode. It drives the async tasks and manages context switching.
        """
        self.main_loop = loop

    def draw_screen(self):
        if self.main_loop:
            self.main_loop.draw_screen()

    @staticmethod
    def get_shared_instance() -> "App":
        assert App._shared_instance, "App not initialized"
        return App._shared_instance

    def log(self, message):
        """
        Convenience method to log a message and print if necessary.
        """
        self.logger.info(message)

        if not self.ui:
            print(message)
