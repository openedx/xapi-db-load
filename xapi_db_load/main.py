"""
Top level script to generate random xAPI events against various backends.
"""

import asyncio
import datetime
import logging
import os
import sys

import click
import uvloop
import yaml

from xapi_db_load.constants import DEFAULT_LMS_URL
from xapi_db_load.journeys.generate import JourneyGenerator
from xapi_db_load.ui.text_ui import TextUI

_ENV_VAR_OVERRIDES = {
    "XAPI_DB_LOAD_CLICKHOUSE_PASSWORD": "db_password",
    "XAPI_DB_LOAD_AWS_SECRET_ACCESS_KEY": "s3_secret",
    "XAPI_DB_LOAD_RALPH_PASSWORD": "lrs_password",
}


def get_config(config_file: str) -> dict:
    """
    Load YAML config and apply environment variable overrides for secrets.

    We override this in tests so that we can use temp dirs for logs etc.
    Environment variables take precedence over values in the config file::

      XAPI_DB_LOAD_CLICKHOUSE_PASSWORD -> db_password
      XAPI_DB_LOAD_AWS_SECRET_ACCESS_KEY -> s3_secret
      XAPI_DB_LOAD_RALPH_PASSWORD -> lrs_password
    """
    with open(config_file, "r") as y:
        conf = yaml.safe_load(y)

    conf["config_file"] = config_file

    for env_var, config_key in _ENV_VAR_OVERRIDES.items():
        value = os.environ.get(env_var)
        if value is not None:
            conf[config_key] = value

    # Apply defaults for optional config keys.
    conf.setdefault("lms_url", DEFAULT_LMS_URL)

    return conf


@click.group()
def cli():
    """
    Top level group of command objects.
    """


@click.command()
@click.option(
    "--config_file",
    help="Configuration file.",
    required=True,
    default="default_config.yaml",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, writable=False),
)
def ui(config_file: str):
    """
    Execute a database load by performing inserts.
    """
    config = get_config(config_file)
    try:
        TextUI(config)
    finally:
        # Clean up the terminal settings when we exit, otherwise there will be artifacts
        # such as mouse handling being broken.
        os.system("stty ixon")


@click.command()
@click.option(
    "--config_file",
    help="Configuration file.",
    required=True,
    default="default_config.yaml",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, writable=False),
)
@click.option(
    "--load_db_only",
    help=(
        "If this option is passed we will try to load from the configured block storage, "
        "no new data will be generated."
    ),
    is_flag=True,
)
def load_db(config_file: str, load_db_only: bool):
    """
    Execute a database load by performing inserts.
    """
    # Import here to avoid circular imports in the UI path.
    from xapi_db_load.async_app import App  # pylint: disable=import-outside-toplevel

    # Use UVLoop to speed up asyncio operations
    # https://uvloop.readthedocs.io/
    # We can't currently use this in the UI mode as it throws BlockingIOError on startup
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    start = datetime.datetime.now()
    config = get_config(config_file)
    app = App(config)
    asyncio.run(app.runner.run(load_db_only))
    app.log(f"Total duration: {datetime.datetime.now() - start}")
    sys.exit(0)


@click.command()
@click.option(
    "--config_file",
    help="Configuration file with a 'journeys' section.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, file_okay=True, writable=False),
)
@click.option(
    "--output_dir",
    help="Where to write the dataset. Defaults to journeys.output_dir from the config.",
    default=None,
)
@click.option("--seed", help="Override journeys.seed.", type=int, default=None)
@click.option(
    "--now",
    help="Override journeys.now (UTC, e.g. '2026-10-08 12:00:00'); pin it for identical output.",
    default=None,
)
def journeys(config_file: str, output_dir: str | None, seed: int | None, now: str | None):
    """
    Generate a learner-journey dataset with known expected results, as CSV files.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    config = get_config(config_file)
    conf = config.get("journeys")
    if not conf:
        raise click.UsageError(f"{config_file} has no 'journeys' section.")
    if seed is not None:
        conf["seed"] = seed
    if now is not None:
        conf["now"] = now
    out = output_dir or conf.get("output_dir")
    if not out:
        raise click.UsageError("Set --output_dir or journeys.output_dir in the config.")
    try:
        generator = JourneyGenerator(conf, config["lms_url"], logging.getLogger("journeys"))
    except ValueError as e:
        raise click.UsageError(str(e)) from e
    start = datetime.datetime.now()
    manifest = generator.run(out)
    click.echo(
        f"Wrote {manifest['num_xapi_events']} xAPI events for {manifest['num_enrollments']} "
        f"enrollments in {manifest['num_course_runs']} course runs to {out} "
        f"in {datetime.datetime.now() - start}"
    )


cli.add_command(load_db)
cli.add_command(journeys)
cli.add_command(ui)

if __name__ == "__main__":
    cli()
