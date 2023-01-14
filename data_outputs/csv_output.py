import csv
import os


class CSVOutput:
    def __init__(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        self.filename = filename
        self.raw_file = open(filename, "w")
        self.csv_file = csv.writer(self.raw_file, quoting=csv.QUOTE_ALL)

    def add_row(self, row):
        if type(row) == dict:
            actual_row = row.values()
        else: # pray it wil be right
            actual_row = row
        self.csv_file.writerow(actual_row)

    def set_commit_mode(self):
        pass

    def commit(self):
        pass

    def close(self):
        self.raw_file.close()
