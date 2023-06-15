from datetime import datetime
import csv
import gzip


class XAPILakeCSV:
    def __init__(self, output_file=""):
        """
        This isn't really a database, so just faking out all of this.
        """
        if not output_file or (not output_file.endswith(".csv") and not output_file.endswith(".csv.gz")):
            raise Exception("No output file given or doesn't end with '.csv' / '.csv.gz'")
        if not output_file.endswith(".gz"):
            output_file = output_file + ".gz"
        self.out_filehandle = gzip.open(output_file, "wt")
        self.csv_writer = csv.writer(self.out_filehandle)
        self.row_count = 0

    def print_db_time(self):
        print(datetime.now(), flush=True)

    def print_row_counts(self):
        print("Currently written row count:")
        print(self.row_count)

    def create_db(self):
        pass

    def drop_tables(self):
        pass

    def create_tables(self):
        pass

    def batch_insert(self, events):
        for v in events:
            out = (v['event_id'], v['emission_time'], str(v['event']))
            self.csv_writer.writerow(out)
        self.row_count += len(events)

    def do_queries(self, event_generator):
        pass

    def do_distributions(self):
        self.out_filehandle.close()
