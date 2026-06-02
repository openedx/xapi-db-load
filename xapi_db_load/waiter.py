"""Base ``Waiter`` task class with shared progress bookkeeping for all backend tasks."""

import asyncio
from logging import Logger
from threading import Lock
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from xapi_db_load.generate_load_async import EventGenerator


class Waiter:
    """
    Base class for all tasks, handles bookeeping and boilerplate task management.
    """

    task_name: str = ""

    def __init__(
        self,
        config: Dict,
        logger: Logger,
        event_generator: "EventGenerator",
    ) -> None:
        self.config = config
        self.event_generator = event_generator
        self.logger = logger
        self.load_from_s3_after = config.get("load_from_s3_after", False)
        self.complete_pct = 0.0
        self.complete_pct_lock = Lock()
        self.total_task_count = 0
        self.total_task_count_lock = Lock()
        self.completed_task_count = 0
        self.completed_task_count_lock = Lock()
        self.finished = False

        if not self.task_name:
            self.task_name = f"[Unnamed task] {type(self)}"

    def get_complete(self) -> float:
        """Return the current completion percentage (0.0 - 1.0)."""
        return self.complete_pct

    def reset(self) -> None:
        """Reset progress counters so the task can be re-run."""
        self.complete_pct = 0.0
        self.total_task_count = 0
        self.completed_task_count = 0
        self.finished = False

    async def _run_task(self) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    async def _run_db_load_task(self) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    def update_complete_pct(self) -> None:
        """
        Threadsafe update of the completion percentage.
        """
        with self.complete_pct_lock:
            try:
                self.complete_pct = self.completed_task_count / self.total_task_count
            # Guard against div-by-zero when ``total_task_count`` has not yet
            # been initialised by a producer task. Any other exception here indicates a
            # real bug and should propagate.
            except ZeroDivisionError:
                self.logger.debug(
                    "update_complete_pct called before total_task_count was set; "
                    "leaving complete_pct unchanged."
                )

    def update_total_task_count(self, increment_by: int) -> None:
        """
        Threadsafe update of the toal task count, necessary when a task has several subtasks.
        """
        with self.total_task_count_lock:
            self.total_task_count += increment_by

        self.update_complete_pct()

    def update_completed_task_count(self, increment_by: int) -> None:
        """
        Threadsafe update of the task counter, necessary when a task has several subtasks.
        """
        with self.completed_task_count_lock:
            self.completed_task_count += increment_by

        self.update_complete_pct()

    def finish(self) -> None:
        """
        Mark this task as done, and force completion to 100% in case of rounding errors.
        """
        self.finished = True
        self.complete_pct = 1.0

    async def run_task(self) -> Any:
        """
        This top level task gets called when generating data and wraps the actual task to be run
        in a loop that checks if the event generator has completed setup.

        Tasks define their implementation in _run_task, except for EventGenerator which overrides
        this method instead.
        """
        while True:
            if self.event_generator.setup_complete:
                rtn = await self._run_task()
                self.logger.info(f"{self.task_name} complete.")
                self.finish()
                return rtn

            self.logger.info("Setup not complete? Waiting...")
            await asyncio.sleep(1)

    async def run_db_load_task(self) -> Any:
        """
        This top level task gets called when loading existing data into the database and wraps the
        actual task to be run. This may not be necessary since the event generator should not need
        to setup in this case, but is kept for consistency.
        """
        while True:
            # TODO: This is probably not needed, but needs to be tested without the EventGenerator.
            if self.event_generator.setup_complete:
                # This line is the only difference from run_task.
                rtn = await self._run_db_load_task()
                self.logger.info(f"{self.task_name} database load complete.")
                self.finish()
                return rtn

            self.logger.info("Setup not complete? Waiting...")
            await asyncio.sleep(1)
