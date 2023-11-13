"""
Top level script to generate random xAPI events against various backends.
"""
import click
import yaml

from xapi_db_load.generate_load import generate_events
from xapi_db_load.utils import get_backend_from_config


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


if __name__ == "__main__":
    load_db()  # pylint: disable=no-value-for-parameter
