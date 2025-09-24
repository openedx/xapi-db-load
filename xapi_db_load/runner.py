import asyncio
import datetime
from logging import Logger
from typing import Dict

from xapi_db_load.backends.chdb import AsyncCHDBTasks
from xapi_db_load.backends.clickhouse import AsyncClickHouseTasks
from xapi_db_load.backends.csv import AsyncCSVTasks
from xapi_db_load.backends.ralph import AsyncRalphTasks
from xapi_db_load.generate_load_async import EventGenerator


class Runner:
    start_time = None
    end_time = None

    def __init__(self, config: Dict, logger: Logger):
        self.config = config
        self.logger = logger
        self.event_generator = EventGenerator(config, logger, None)
        self.running = False
        self.finished = False
        self.set_backend(config["backend"])

    def set_backend(self, backend):
        """
        Set up the backend and update the tasks to be run.

        Eventually we should be able to call this from the UI to update on the fly.
        """
        if backend == "clickhouse":
            self.backend = AsyncClickHouseTasks(
                self.config, self.logger, self.event_generator
            )
        elif backend == "csv":
            self.backend = AsyncCSVTasks(self.config, self.logger, self.event_generator)
        elif backend == "chdb":
            self.backend = AsyncCHDBTasks(
                self.config, self.logger, self.event_generator
            )
        elif backend == "ralph":
            self.backend = AsyncRalphTasks(
                self.config, self.logger, self.event_generator
            )
        else:
            raise ValueError("Invalid backend")

        self.test_data_tasks = self.backend.get_test_data_tasks()

    async def run(self, load_db_only: bool = False):
        """
        Run our configured tasks in the background, and an additional task to update progress.
        """
        if self.running:
            self.logger.info("Runner: Already running")
            return
        self.logger.info("Runner: Kicking off the run")
        self.start_time = datetime.datetime.now()
        self.running = True

        # This thread runs in the background and is responsible for updating the status
        # of the tasks and the overall status.
        log_status_task = asyncio.create_task(self.log_status())

        # We get different sets of tasks depending on whether we're creating new data
        # or just loading existing data. Either way, we block this thread here until all tasks are
        # complete.
        if load_db_only:
            self.logger.debug("Runner: Running tasks for load_db_only")
            results = await asyncio.gather(
                *[task.run_db_load_task() for task in self.test_data_tasks]
            )
        else:
            self.logger.debug("Runner: Running tasks")
            results = await asyncio.gather(
                *[task.run_task() for task in self.test_data_tasks]
            )

        self.end_time = datetime.datetime.now()

        for r in results:
            if isinstance(r, Exception):
                raise r
        self.running = False
        self.finished = True
        self.logger.info(
            f"ALL TASKS DONE! Run duration was {self.end_time - self.start_time}"
        )
        # If it is still running, cancel the log status task. This can happen if a task mis-reports
        # its status.
        log_status_task.cancel()

    def get_overall_time(self) -> str:
        """
        Return the overall time taken for the run.
        """
        if not self.start_time:
            return "Not started"
        if not self.end_time:
            return str(datetime.datetime.now() - self.start_time)
        return str(self.end_time - self.start_time)

    def get_overall_progress(self) -> float:
        """
        Return the overall progress of all tasks as a float.
        """
        tasks = self.test_data_tasks
        total = 0
        for task in tasks:
            total += task.get_complete()

        return total / len(tasks)

    async def log_status(self):
        """
        A long running thread that monitors the status of the tasks and logs their progress.

        This task also determines when all tasks are complete and sets the overall status.
        """
        assert self.start_time
        tasks = self.test_data_tasks

        while True:
            await asyncio.sleep(10)
            ttl = 0
            for task in tasks:
                completion = task.get_complete()
                ttl += completion

                # Tasks that wait for subtasks from other tasks, like loading to ClickHouse after
                # writing data to S3 can be at 100% complete, but not finished since they are still
                # wating for additional subtasks to be created.
                if task.finished:
                    self.logger.info(f"{task.task_name}: Complete!")
                elif completion == 1.0:
                    self.logger.info(
                        f"{task.task_name}: Caught up, waiting for more subtasks."
                    )
                else:
                    self.logger.info(f"{task.task_name}: {round(completion, 2) * 100}%")

                if completion > 1.0:
                    self.logger.error(
                        f"{task.task_name}: Over 100% complete! {completion} of {task.total_task_count} tasks supposedly complete."
                    )
            overall_progress = self.get_overall_progress()
            if overall_progress == 1.0:
                self.running = False
                self.finished = True
                self.logger.info("All tasks complete! Breaking out of loop.")
                return
            else:
                self.logger.info(
                    f"Overall progress: {round(overall_progress * 100, 2)}%"
                )
                self.logger.info(
                    f"   Duration: {datetime.datetime.now() - self.start_time}"
                )
                self.logger.info("--------------------------")

    def reset_status(self):
        """
        If we have canceled or completed a run in the UI, we need to reset all state before
        starting a new one.
        """
        for task in self.test_data_tasks:
            self.logger.info(f"Resetting {task.task_name}")
            task.reset()

        self.running = False
        self.finished = False
        self.start_time = None
        self.end_time = None
