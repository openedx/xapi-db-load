"""
Top level script to generate random xAPI events against various backends.
"""

import datetime

import click
import yaml

from xapi_db_load.generate_load import generate_events
from xapi_db_load.utils import get_backend_from_config


def _run(config):
    """
    Bulk load events from the given config.
    """
    backend = get_backend_from_config(config)
    generate_events(config, backend)


@click.command()
@click.option(
    "--backend",
    required=True,
    type=click.Choice(
        ["clickhouse", "ralph_clickhouse", "csv_file"],
        case_sensitive=True,
    ),
    help="Which backend to run against",
)
@click.option(
    "--num_batches",
    default=1,
    help="Number of batches to run, num_batches * batch_size is the total rows",
)
@click.option(
    "--batch_size",
    default=10000,
    help="Number of rows to insert per batch, num_batches * batch_size is the total rows",
)
@click.option(
    "--drop_tables_first",
    default=False,
    help="If True, the target tables will be dropped if they already exist",
)
@click.option(
    "--distributions_only",
    default=False,
    help="Just run distribution queries and exit",
)
@click.option(
    "--start_date",
    default=(datetime.date.today() - datetime.timedelta(days=365)).strftime(
        "%Y-%m-%d"
    ),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Create events starting at this date, default to 1 yr ago. ex: 2020-11-30"
)
@click.option(
    "--end_date",
    default=(datetime.date.today() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    ),
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Create events ending at this date, default to tomorrow. ex: 2020-11-31"
)
@click.option("--db_host", default="localhost", help="Database host name")
@click.option("--db_port", help="Database port")
@click.option("--db_name", default="xapi", help="Database name")
@click.option("--db_username", help="Database username")
@click.option(
    "--db_password", help="Password for the database so it's not stored on disk"
)
@click.option("--lrs_url", default="http://localhost:8100/", help="URL to the LRS, if used")
@click.option("--lrs_username", default="ralph", help="LRS username")
@click.option("--lrs_password", help="Password for the LRS")
@click.option(
    "--csv_output_directory",
    help="Directory where the output files should be written when using the csv backend.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, writable=True)
)
@click.option(
    "--log_dir",
    help="The directory to log timing information to.",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, writable=True)
)
def load_db(**kwargs):
    """
    Execute the database load with the given options.
    """
    _run(kwargs)


@click.command()
@click.option(
    "--config_file",
    help="Configuration file.",
    required=True,
    type=click.Path(
        exists=True,
        dir_okay=False,
        file_okay=True,
        writable=False
    )
)
def load_db_config(config_file):
    """
    Execute the database load.
    """
    with open(config_file, 'r') as y:
        config = yaml.safe_load(y)

    _run(config)


if __name__ == "__main__":
    load_db_config()  # pylint: disable=no-value-for-parameter
