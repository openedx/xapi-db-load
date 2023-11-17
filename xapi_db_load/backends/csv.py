"""
CSV Lake implementation.

This can be used to generate a gzipped csv of events that can be loaded into any system.
"""
import csv
import gzip
import os
import uuid
from datetime import datetime

from smart_open import open as smart


class XAPILakeCSV:
    """
    CSV fake data lake implementation.
    """

    def __init__(self, output_destination):
        # This isn't really a database, so just faking out all of this.
        self.xapi_csv_writer = self._get_csv_handle("xapi", output_destination)
        self.course_csv_writer = self._get_csv_handle("courses", output_destination)
        self.blocks_csv_writer = self._get_csv_handle("blocks", output_destination)

        self.row_count = 0

    def _get_csv_handle(self, file_type, output_destination):
        out_filepath = os.path.join(output_destination, f"{file_type}.csv.gz")
        x = smart(out_filepath, "w", compression=".gz")
        return csv.writer(x)

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
            self.xapi_csv_writer.writerow(out)
        self.row_count += len(events)

    def insert_event_sink_course_data(self, courses):
        """
        Write the course overview data.
        """
        for course in courses:
            c = course.serialize_course_data_for_event_sink()
            dump_id = str(uuid.uuid4())
            dump_time = datetime.utcnow()
            self.course_csv_writer.writerow((
                c['org'],
                c['course_key'],
                c['display_name'],
                c['course_start'],
                c['course_end'],
                c['enrollment_start'],
                c['enrollment_end'],
                c['self_paced'],
                c['course_data_json'],
                c['created'],
                c['modified'],
                dump_id,
                dump_time
            ))

    def insert_event_sink_block_data(self, courses):
        """
        Write out the block data file.
        """
        for course in courses:
            blocks = course.serialize_block_data_for_event_sink()
            dump_id = str(uuid.uuid4())
            dump_time = datetime.utcnow()
            for b in blocks:
                self.blocks_csv_writer.writerow((
                    b['org'],
                    b['course_key'],
                    b['location'],
                    b['display_name'],
                    b['xblock_data_json'],
                    b['order'],
                    b['edited_on'],
                    dump_id,
                    dump_time
                ))

    def do_queries(self, event_generator):
        """
        Execute queries, not needed here.
        """

    def do_distributions(self):
        """
        Execute distribution queries, not needed here.

        But this is the last step, so take the opportunity to close the file.
        """
