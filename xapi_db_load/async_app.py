"""
Top level application state for UI and non-UI modes.
"""

import logging
import logging.handlers
import os
import sys
from typing import TYPE_CHECKING

from xapi_db_load.runner import Runner

if TYPE_CHECKING:
    from xapi_db_load.ui.text_ui import TextUI


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
        self.config = config
        self._setup_logger()

        # The Runner coordinates the async tasks from the configured backend
        self.runner = Runner(self.config, self.logger)

    # Default size (bytes) at which the rotating log file rolls over.
    # Override via the ``log_max_bytes`` config key. Set to ``0`` to disable
    # rotation (1 big file, unbounded growth).
    DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB

    # Default number of rotated logs to retain. Override via
    # ``log_backup_count``. Ignored if rotation is disabled.
    DEFAULT_LOG_BACKUP_COUNT = 5

    def _setup_logger(self):
        """
        Configure file and (optionally) stdout logging.

        The logger always captures DEBUG to the file (with rotation) and INFO
        to stdout when not running under the urwid UI. Per-handler levels are
        set explicitly so adding a handler later cannot accidentally raise the
        root level for existing handlers.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        if not self.ui:
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
            self.logger.addHandler(stream_handler)

        try:
            file_handler = self._build_file_handler()
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
            )
            self.logger.addHandler(file_handler)
        except OSError as exc:
            # File logging is a nice-to-have; failing to open the log file
            # (permissions, disk full, missing directory, etc.) should not abort
            # the run, especially in non-UI mode where stdout logging still works.
            self.logger.warning(
                "Unable to open log file %r: %s", self.logfile_path, exc
            )

        self.log("Logging set up")

    def _build_file_handler(self) -> logging.Handler:
        """
        Return the file handler to use, honoring rotation config.

        ``log_max_bytes = 0`` disables rotation and returns a plain
        ``FileHandler`` for users who prefer the pre-rotation behavior.
        """
        max_bytes = self.config.get("log_max_bytes", self.DEFAULT_LOG_MAX_BYTES)
        backup_count = self.config.get(
            "log_backup_count", self.DEFAULT_LOG_BACKUP_COUNT
        )

        if not max_bytes:
            return logging.FileHandler(self.logfile_path)

        return logging.handlers.RotatingFileHandler(
            self.logfile_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )

    def set_main_loop(self, loop):
        """
        Set the main loop.

        The main loop is an asyncio loop, or uvloop if we are not in UI mode. It drives the async tasks and manages context switching.
        """
        self.main_loop = loop

    def draw_screen(self):
        """Trigger a UI redraw if a main loop is currently attached."""
        if self.main_loop:
            self.main_loop.draw_screen()

    @staticmethod
    def get_shared_instance() -> "App":
        """Return the process-wide singleton ``App`` instance."""
        assert App._shared_instance, "App not initialized"
        return App._shared_instance

    def log(self, message):
        """
        Log an info-level message.

        In non-UI mode the stdout ``StreamHandler`` attached to ``self.logger``
        already prints to the terminal, so we do not also call ``print()`` --
        doing so would emit every message twice.
        """
        self.logger.info(message)
