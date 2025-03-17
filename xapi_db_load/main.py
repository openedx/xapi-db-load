"""
Top level script to generate random xAPI events against various backends.
"""

import asyncio
import datetime
import os

import click
import uvloop
import yaml

from xapi_db_load.ui.text_ui import TextUI


def get_config(config_file: str) -> dict:
    """
    Wrap around config loading.

    We override this in tests so that we can use temp dirs for logs etc.
    """
    with open(config_file, "r") as y:
        conf = yaml.safe_load(y)

    conf["config_file"] = config_file
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
    help="If this option is passed we will try to load from the configured block storage, no new data will be generated.",
    is_flag=True,
)
def load_db(config_file: str, load_db_only: bool):
    """
    Execute a database load by performing inserts.
    """
    # Import here to avoid circular imports in the UI path.
    from xapi_db_load.async_app import App

    # Use UVLoop to speed up asyncio operations
    # https://uvloop.readthedocs.io/
    # We can't currently use this in the UI mode as it throws BlockingIOError on startup
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    start = datetime.datetime.now()
    config = get_config(config_file)
    app = App(config)
    asyncio.run(app.runner.run(load_db_only))
    print(f"Total duration: {datetime.datetime.now() - start}")
    exit(0)


cli.add_command(load_db)
cli.add_command(ui)

if __name__ == "__main__":
    cli()
