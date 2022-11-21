import click

import clickhouse_insert_xapi as db
from generate_load import EventGenerator

# PASSWORD = "DdAzDVD5xrjwPLBgvVO4xPny"


@click.command()
@click.option("--num_batches", default=1, help="Number of batches to run, num_batches * batch_size is the total rows")
@click.option("--batch_size", default=10000, help="Number of rows to insert per batch, num_batches * batch_size is the total rows")
@click.option("--drop_tables_first", default=False, help="If True, the target tables will be dropped if they already exist")
@click.option("--host", default="localhost", help="Database host name")
@click.option("--port", default="18123", help="Database port")
@click.option("--username", default="ch_lrs", help="Database username")
@click.option("--password", prompt="Database password", hide_input=True, help="Password for the database so it's not stored on disk")
@click.option("--database", default="xapi", help="Database name")
def load_db(num_batches, batch_size, drop_tables_first, host, port, username, password, database):
    lake = db.XAPILake(host, port, username, password, database=database)

    if drop_tables_first:
        lake.drop_tables()

    lake.create_tables()
    lake.print_db_time()

    for x in range(num_batches):
        if x % 10 == 0:
            print(f"{x} of {num_batches}")
            lake.print_db_time()

        event_generator = EventGenerator(batch_size=batch_size)

        events = event_generator.get_batch_events()
        lake.batch_insert(events)

        if x % 100 == 0:
            lake.print_db_time()
            lake.print_row_counts()

    print(f"Done! Added {num_batches * batch_size} rows!")
    lake.print_db_time()
    lake.print_row_counts()


if __name__ == '__main__':
    load_db()
