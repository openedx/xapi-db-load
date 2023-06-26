"""
CSV Lake implementation.

This can be used to generate a gzipped csv of events that can be loaded into any system.
"""
import csv
import gzip
from datetime import datetime


class XAPILakeCSV:
    """
    CSV fake data lake implementation.
    """

    def __init__(self, output_file=""):
        # This isn't really a database, so just faking out all of this.
        if not output_file or (
            not output_file.endswith(".csv") and not output_file.endswith(".csv.gz")
        ):
            raise Exception(  # pylint: disable=broad-exception-raised
                "No output file given or doesn't end with '.csv' / '.csv.gz'"
            )
        if not output_file.endswith(".gz"):
            output_file = output_file + ".gz"
        self.out_filehandle = gzip.open(output_file, "wt")
        self.csv_writer = csv.writer(self.out_filehandle)
        self.row_count = 0

    def print_db_time(self):
        """
        Print the database time, in our case it's just the local computer time.
        """
        print(datetime.now(), flush=True)

    def print_row_counts(self):
        """
        Print the number of rows written to CSV so far.
        """
        print("Currently written row count:")
        print(self.row_count)

    def create_db(self):
        """
        Create database, not needed here.
        """

    def drop_tables(self):
        """
        Drop tables, not needed here.
        """

    def create_tables(self):
        """
        Create tables, not needed here.
        """

    def batch_insert(self, events):
        """
        Write a batch of rows to the CSV.
        """
        for v in events:
            out = (v["event_id"], v["emission_time"], str(v["event"]))
            self.csv_writer.writerow(out)
        self.row_count += len(events)

    def do_queries(self, event_generator):
        """
        Execute queries, not needed here.
        """

    def do_distributions(self):
        """
        Execute distribution queries, not needed here.

        But this is the last step, so take the opportunity to close the file.
        """
        self.out_filehandle.close()
