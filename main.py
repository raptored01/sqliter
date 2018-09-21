import sqlite3


class Connection:

    def __init__(self, __db_name):
        self.db_name = __db_name
        self.connection = sqlite3.connect(__db_name)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    @property
    def tables(self):
        tables = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if tables:
            for table_name in tables:
                self.tables.append(table_name["name"])

    def table(self, name):
        return Table(self, name)


class Table:

    def __init__(self, __connection, __name):
        self.connection = __connection.connection
        self.cursor = __connection.cursor
        self.name = __name

    def all(self, as_list=False, *columns):
        cols = ", ".join(columns) or "*"
        if as_list:
            return [dict(row) for row in self.cursor.execute(
                f"""
                SELECT {cols} FROM {self.name}
                """
            )]
        else:
            return self.cursor.execute(
                """
                SELECT {cols} FROM :table_name
                """, dict(table_name=self.name)
            )


connection = Connection("na2akn.db")
table = Table(connection, "document_types")

# print(connection.db_name, connection.tables)
print(table.all(True, "doc_type"))
