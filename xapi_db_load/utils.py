"""
Utility code for xapi-db-load.
"""
import json
import logging
import os
from datetime import datetime

import click

from xapi_db_load.backends import clickhouse_lake as clickhouse
from xapi_db_load.backends import csv
from xapi_db_load.backends import ralph_lrs as ralph

timing = logging.getLogger("timing")


def get_backend_from_config(config):
    """
    Return an instantiated backend from the given config dict.
    """
    return get_backend(
        config["backend"],
        config.get("db_host"),
        config.get("db_port"),
        config.get("db_username"),
        config.get("db_password"),
        config.get("db_name"),
        config.get("lrs_url"),
        config.get("lrs_username"),
        config.get("lrs_password"),
        config.get("csv_output_directory"),
    )


def get_backend(
    backend, db_host, db_port, db_username, db_password, db_name,
    lrs_url=None, lrs_username=None, lrs_password=None, csv_output_directory=None
):
    """
    Return an instantiated backend from the given arguments.
    """
    if backend == "clickhouse":
        lake = clickhouse.XAPILakeClickhouse(
            db_host=db_host,
            db_port=db_port,
            db_username=db_username,
            db_password=db_password,
            db_name=db_name,
        )
    elif backend == "ralph_clickhouse":
        lake = ralph.XAPILRSRalphClickhouse(
            db_host=db_host,
            db_port=db_port,
            db_username=db_username,
            db_password=db_password,
            db_name=db_name,
            lrs_url=lrs_url,
            lrs_username=lrs_username,
            lrs_password=lrs_password,
        )
    elif backend == "csv_file":
        if not csv_output_directory:
            raise click.UsageError(
                "--csv_output_directory must be provided for this backend."
            )
        lake = csv.XAPILakeCSV(output_directory=csv_output_directory)
    else:
        raise NotImplementedError(f"Unknown backend {backend}.")

    return lake


def setup_timing(log_dir):
    """
    Set up the timing logger.

    This should probably take an optional logging config file eventually.
    """
    formatter = logging.Formatter('%(message)s')

    if log_dir:
        timing_log_name = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_timing.log")
        print(f"Logging timing data to {timing_log_name}")
        handler = logging.FileHandler(timing_log_name)
    else:
        print("No log dir provided, logging timing data to stdout.")
        handler = logging.StreamHandler()

    handler.setFormatter(formatter)
    timing.addHandler(handler)
    timing.setLevel(logging.INFO)


class LogTimer:
    """
    Class to time and log our various operations.
    """

    start_time = None

    def __init__(self, timer_type, timer_key):
        self.timer_type = timer_type
        self.timer_key = timer_key

    def __enter__(self):
        self.start_time = datetime.now()

    def __exit__(self, exc_type, exc_val, exc_tb):
        log_duration(
            self.timer_type,
            self.timer_key,
            (datetime.now() - self.start_time).total_seconds()
        )


def log_duration(timer_type, timer_key, duration):
    """
    Log timing data to the configured logger.

    timer_type: Top level type of the timer ("query", "batch_load", "setup"...)
    timer_key: Specific timer ("Count of Users", "Batch 100", "init"...)
    duration: Timing in fractional seconds (1.20, 12.345, 0.03)
    """
    stmt = {'time': datetime.now().isoformat(), 'timer': timer_type, 'key': timer_key, 'duration': duration}
    timing.info(json.dumps(stmt))
