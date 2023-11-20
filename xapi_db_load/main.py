"""
Top level script to generate random xAPI events against various backends.
"""
import click
import yaml

from xapi_db_load.generate_load import generate_events
from xapi_db_load.utils import get_backend_from_config


@click.group()
def cli():
    pass


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
    Execute the database load.
    """
    with open(config_file, 'r') as y:
        config = yaml.safe_load(y)

    backend = get_backend_from_config(config)
    print(config)
    generate_events(config, backend)

    if config["backend"] == "csv_file" and config["csv_load_from_s3_after"]:
        print("Attempting to load to ClickHouse from S3...")
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
    Execute the database load.
    """
    with open(config_file, 'r') as y:
        config = yaml.safe_load(y)

    if not "clickhouse" in config["backend"]:
        raise click.BadParameter("You must have a ClickHouse based backend to load from S3.")

    backend = get_backend_from_config(config)
    print(config)
    backend.load_from_s3(config["s3_source_location"])


cli.add_command(load_db)
cli.add_command(load_db_from_s3)

if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter
