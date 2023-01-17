import csv
import os


class CSVOutput:
    def __init__(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        self.filename = filename
        self.raw_file = open(filename, "w")
        self.csv_file = csv.writer(self.raw_file, quoting=csv.QUOTE_ALL)

        self.header_wrote = False

    def add_row(self, row: dict):
        if not self.header_wrote:
            self.csv_file.writerow(row.keys())
            self.header_wrote = True

        self.csv_file.writerow(row.values())

    def set_commit_mode(self):
        pass

    def commit(self):
        pass

    def close(self):
        self.raw_file.close()
