import sqlite3


from exceptions import (
    InvalidFieldName,
    NoSuchTable,
    NoSuchEntry,
    NoSuchField,
)

from field_types import FieldTypes


FIELD_TYPES = {
    "TEXT": FieldTypes.text,
    "INTEGER": FieldTypes.integer,
    "REAL": FieldTypes.real,
    "DATE": FieldTypes.date,
    "DATETIME": FieldTypes.datetime,
    "BLOB": FieldTypes.blob,
    "BOOLEAN": FieldTypes.boolean,
}


def scrub(text):
    return "".join(c for c in text if c.isalnum() or c is "_")


def clean_kwargs(**kwargs):
    cleaned_kwargs = {}
    for key, value in kwargs.items():
        cleaned_key = scrub(key)
        cleaned_kwargs[cleaned_key] = value
    return cleaned_kwargs


def is_valid_field_name(text):
    return not text.startswith("__")


class Database:

    def __init__(self, __db_name):
        self.db_name = __db_name
        self.connection = sqlite3.connect(__db_name)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

    def raw(self, sql):
        self.cursor.execute(sql)
        return self.cursor.lastrowid

    @property
    def tables(self):
        table_rows = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = []
        if table_rows:
            for table_row in table_rows:
                tables.append(table_row["name"])
        return tables

    def table(self, name):
        if name in self.tables:
            return Table(self, name)
        else:
            raise NoSuchTable(name)


class Table:

    def __init__(self, __connection, __name):
        self._connection = __connection.connection
        self.__cursor = self._connection.cursor()
        self.name = __name
        self.fields = self.__get_fields()

    def all(self, *columns):
        cols = ", ".join(columns) or "*"
        for row in self.__cursor.execute(f"""SELECT {cols} FROM {self.name}"""):
            yield Entry(self, row["id"], **row)

    def create(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)

        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())

        sql = f"""
        INSERT INTO {self.name} (
            {cols}
        ) VALUES ({values})
        """
        with self._connection:
            self.__cursor.execute(sql, kwargs)
            entry = Entry(self, self.__cursor.lastrowid)
            entry._instanciate()
            return entry

    def create_or_replace(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)

        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())

        sql = f"""
        INSERT REPLACE INTO {self.name} (
            {cols}
        ) VALUES ({values})
        """
        with self._connection:
            self.__cursor.execute(sql, kwargs)
            entry = Entry(self, self.__cursor.lastrowid)
            entry._instanciate()
            return entry

    def create_or_ignore(self, **kwargs):
        kwargs = clean_kwargs(**kwargs)

        cols = ", ".join(kwargs.keys())
        values = ", ".join(f":{kw}" for kw in kwargs.keys())

        sql = f"""
        INSERT REPLACE INTO {self.name} (
            {cols}
        ) VALUES ({values})
        """
        with self._connection:
            self.__cursor.execute(sql, kwargs)  # TODO check if created or ignored
            entry = Entry(self, self.__cursor.lastrowid)
            entry._instanciate()
            return entry

    def clear(self):
        self.__cursor.execute(
            f"""
            DELETE FROM {self.name}
            """
        )

    @property
    def columns(self):
        cursor = self._connection.execute(f"SELECT * FROM {self.name}")
        return list(map(lambda x: x[0], cursor.description))

    def filter(self, **kwargs):
        # TODO
        pass

    def get(self, **kwargs):
        assert len(kwargs) > 0, "You must provide **kwargs"
        kwargs = clean_kwargs(**kwargs)
        if "pk" in kwargs:
            kwargs["id"] = kwargs["pk"]
            kwargs.pop("pk")
        conditions = " AND ".join(f"{key}=:{key}" for key in kwargs)
        sql = f"""
            SELECT * FROM {self.name}
            WHERE {conditions}
            """
        row = self.__cursor.execute(sql, kwargs).fetchone()
        if row is None:
            raise NoSuchEntry
        entry = Entry(self, row["id"])
        entry._instanciate()
        return entry

    @property
    def pragma(self):
        return [dict(row)
                for row in self.__cursor.execute(f"PRAGMA table_info({self.name})")]

    def __get_fields(self):
        fields = {pragma["name"]: pragma for pragma in self.pragma}
        for name, pragma in fields.items():
            pragma["type"] = FIELD_TYPES[pragma["type"]]
        return fields


class Entry:

    def __init__(self, __table, __pk, **kwargs):
        self.__table = __table
        self.__connection = self.__table._connection
        self.__cursor = self.__connection.cursor()
        self.__pk = __pk

        for key in kwargs.keys():
            if not is_valid_field_name(key):
                raise InvalidFieldName(key)
            # if not key in self.__table.columns:
                # raise NoSuchField(key)
            setattr(self, key, kwargs[key])

    def _instanciate(self):
        row = self.__cursor.execute(
            f"""
            SELECT * FROM {self.__table.name}
            WHERE id=?
            """, (self.__pk, )
        ).fetchone()
        if row is None:
            raise NoSuchEntry(self.__table.name, self.__pk)

        for key in row.keys():
            if not is_valid_field_name(key):
                raise InvalidFieldName(key)
            value = self.__table.fields[key]["type"](row[key])
            setattr(self, key, value)

    def __is_protected_name(self, name):
        protected_name = f"_{self.__class__.__name__}__"
        return name.startswith(protected_name)

    def save(self):
        valid_fields = self.__table.fields.keys()
        kwargs = {}
        for key in self.__dict__.keys():
            if not self.__is_protected_name(key) and key not in valid_fields:
                raise NoSuchField(key)
            kwargs[key] = self.__dict__[key]
        values = ", ".join([f"{key}=:{key}" for key in kwargs if key != "id"])
        sql = f"""
            UPDATE {self.__table.name}
            SET {values}
            WHERE id=:id
            """
        with self.__connection:
            self.__cursor.execute(sql, kwargs)

    def delete(self):
        self.__cursor.execute(
            f"""
            DELETE FROM {self.__table.name}
            WHERE id=?
            """, (self.id, )
        )

    def __repr__(self):
        return str({key: self.__dict__[key] for key in self.__table.columns})


connection = Database("na2akn.db")
documents = connection.table("documents")
doc = documents.get(pk=1)
print(doc)
