"""
Top level script to generate random xAPI events against various backends.
"""

import datetime

import click

from .backends import clickhouse_lake as clickhouse
from .backends import csv
from .backends import ralph_lrs as ralph
from .generate_load import EventGenerator


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
def load_db(
    backend,
    num_batches,
    batch_size,
    drop_tables_first,
    distributions_only,
    db_host,
    db_port,
    db_name,
    db_username,
    db_password,
    lrs_url,
    lrs_username,
    lrs_password,
    csv_output_directory,
):
    """
    Execute the database load.
    """
    start = datetime.datetime.utcnow()

    # Since we're accepting pw on input we need a way to "None" it.
    if db_password == " ":
        db_password = None

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
        lake = csv.XAPILakeCSV(output_directory=csv_output_directory)
    else:
        raise NotImplementedError(f"Unkown backend {backend}.")

    if distributions_only:
        lake.do_distributions()
        print("Done!")
        return

    if drop_tables_first:
        lake.drop_tables()

    lake.create_tables()
    lake.print_db_time()

    event_generator = EventGenerator(batch_size=batch_size)

    for x in range(num_batches):
        if x % 100 == 0:
            print(f"{x} of {num_batches}")
            lake.print_db_time()

        events = event_generator.get_batch_events()
        lake.batch_insert(events)

        if x % 1000 == 0:
            lake.do_queries(event_generator)
            lake.print_db_time()
            lake.print_row_counts()

    # event_generator.dump_courses()
    print("Inserting course metadata...")
    lake.insert_event_sink_course_data(event_generator.known_courses)
    print("Inserting block metadata...")
    lake.insert_event_sink_block_data(event_generator.known_courses)

    print(f"Done! Added {num_batches * batch_size:,} rows!")

    end = datetime.datetime.utcnow()
    print("Batch insert time: " + str(end - start))

    lake.print_db_time()
    lake.print_row_counts()
    lake.do_distributions()

    end = datetime.datetime.utcnow()
    print("Total run time: " + str(end - start))


if __name__ == "__main__":
    load_db()  # pylint: disable=no-value-for-parameter
