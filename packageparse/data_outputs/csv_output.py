import csv
import os


class CSVOutput:
    def __init__(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        self.filename = filename
        self.raw_file = open(filename, "w")
        self.csv_file = csv.writer(self.raw_file, quoting=csv.QUOTE_ALL)

        self.header_wrote = False

    def set_header(self, keys: list):
        self.csv_file.writerow(keys)

    def add_row(self, row):
        if type(row) == dict:
            if not self.header_wrote:
                self.set_header(row.keys())
                self.header_wrote = True
            values = row.values()
        else: # hope it's a compatible iterable
            values = row

        self.csv_file.writerow(values)

    def set_commit_mode(self):
        pass

    def commit(self):
        pass

    def close(self):
        self.raw_file.close()
