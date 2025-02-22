
from packageviewer.db.sql_table import SQLTable
from packageviewer.db.inserters.inserter import Inserter


class PacmanInserter(Inserter):

    def __init__(self, conn) -> None:
        super().__init__(conn)

        self.table_tmp_package = SQLTable(
            conn=self.conn,
            table_name="tmp_package",
            create_query="""
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_package (name, version, repo)
        """,
        )

        self.table_tmp_file = SQLTable(
            conn=self.conn,
            table_name="tmp_file",
            create_query="""
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_file (package, dirname, filename)
        """,
        )

        self.table_tmp_repo = SQLTable(
            conn=self.conn,
            table_name="tmp_repo",
            create_query="""
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_repo(repo_id INTEGER, name TEXT)
        """,
        )

        self.table_tmp_dep = SQLTable(
            conn=self.conn,
            table_name="tmp_dep",
            create_query="""
            CREATE TABLE IF NOT EXISTS tmp_dep(parent_name, dep_name)
        """,
        )
