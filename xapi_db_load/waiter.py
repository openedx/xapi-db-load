import asyncio
from logging import Logger
from threading import Lock
from typing import Dict


class Waiter:
    """
    Base class for all tasks, handles bookeeping and boilerplate task management.
    """

    def __init__(self, config: Dict, logger: Logger, event_generator: "EventGenerator"):
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

    def get_complete(self):
        return self.complete_pct

    def reset(self):
        self.complete_pct = 0.0
        self.total_task_count = 0
        self.completed_task_count = 0
        self.finished = False

    async def _run_task(self):
        raise NotImplementedError("Subclasses must implement this method")

    async def _run_db_load_task(self):
        raise NotImplementedError("Subclasses must implement this method")

    def update_complete_pct(self):
        """
        Threadsafe update of the completion percentage.
        """
        with self.complete_pct_lock:
            try:
                self.complete_pct = self.completed_task_count / self.total_task_count
            # Eat all errors here since this can be happening in a long running thread that would
            # otherwise die and leave the application hanging in the case of a div by 0 etc.
            except Exception as e:
                self.logger.error(f"Error in update_complete_pct {e}")

    def update_total_task_count(self, increment_by: int):
        """
        Threadsafe update of the toal task count, necessary when a task has several subtasks.
        """
        with self.total_task_count_lock:
            self.total_task_count += increment_by

        self.update_complete_pct()

    def update_completed_task_count(self, increment_by: int):
        """
        Threadsafe update of the task counter, necessary when a task has several subtasks.
        """
        with self.completed_task_count_lock:
            self.completed_task_count += increment_by

        self.update_complete_pct()

    def finish(self):
        """
        Mark this task as done, and force completion to 100% in case of rounding errors.
        """
        self.finished = True
        self.complete_pct = 1.0

    async def run_task(self):
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

    async def run_db_load_task(self):
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
