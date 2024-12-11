"""
Top level script to generate random xAPI events against various backends.
"""
import click
import yaml

from xapi_db_load.generate_load import generate_events
from xapi_db_load.utils import get_backend_from_config


def get_config(config_file):
    """
    Wrap around config loading.

    We override this in tests so that we can use temp dirs for logs etc.
    """
    with open(config_file, 'r') as y:
        return yaml.safe_load(y)


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
    type=click.Path(
        exists=True,
        dir_okay=False,
        file_okay=True,
        writable=False
    )
)
def load_db(config_file):
    """
    Execute a database load by performing inserts.
    """
    config = get_config(config_file)
    backend = get_backend_from_config(config)
    generate_events(config, backend)

    try_s3_load = config.get("csv_load_from_s3_after")

    if try_s3_load:
        print("Attempting to load to ClickHouse from S3...")
        # No matter what the configured backend is for event generation we need to
        # use the clickhouse config for the load.
        config["backend"] = "clickhouse"
        ch_backend = get_backend_from_config(config)
        ch_backend.load_from_s3(config["s3_source_location"])

    print("Done.")


@click.command()
@click.option(
    "--config_file",
    help="Configuration file.",
    required=True,
    default="default_config.yaml",
    type=click.Path(
        exists=True,
        dir_okay=False,
        file_okay=True,
        writable=False
    )
)
def load_db_from_s3(config_file):
    """
    Execute the database by importing existing files from S3.
    """
    config = get_config(config_file)

    # When loading from S3 we always need the clickhouse backend.
    config["backend"] = "clickhouse"
    backend = get_backend_from_config(config)
    backend.load_from_s3(config["s3_source_location"])


cli.add_command(load_db)
cli.add_command(load_db_from_s3)

if __name__ == "__main__":
    cli()
