import sqlite3


class Dataquerier:

    def __init__(self, db_filepath):
        self.db_filepath = db_filepath
        self.conn = sqlite3.connect(db_filepath)

    def getPackageInfo(self, package_name):
        self.conn.execute(
            """
        SELECT * FROM package
        WHERE package.name = ?
        """,
            package_name,
        )
